# ADR-002: Durable Producer

**Status**: Proposed  
**Date**: 2026-06-30  
**Context**: Multi-cluster Kafka replication with durability guarantees for CloudEvents

---

## Context

### Problem
Need to produce events to heterogeneous Kafka clusters with guarantees:
- **No message loss** — events durably persisted before acknowledging
- **Ordering preserved** — per-key ordering maintained
- **Exactly-once or idempotent** — safe retries, no duplicate events
- **Cross-cluster compatibility** — works with MSK, on-prem KRaft, Zenith

### Event sources
- **Postgres CDC**: Party/PartyRole/PartyRelationship state changes
- **HTTP API**: CloudEvents POST (Spring Boot bridge)
- **Internal services**: Game state mutations, system events

### Target clusters
- **MSK**: AWS managed, KRaft-based, known behavior
- **On-prem KRaft 3.7.0**: Direct control, local ISR
- **Zenith**: Unknown transaction support, unknown idempotency guarantees

### Constraints
- **CloudEvents format**: Standard envelope for interoperability
- **Audit-first**: Every event must reach audit consumer
- **Zenith unknowns**: Can't assume advanced Kafka features

---

## Decision

### Producer semantics: **Idempotent producer with acks=all**

**Rationale**:
- **Not fire-and-forget (acks=0)**: Unacceptable message loss
- **Not leader-only (acks=1)**: Loses messages if leader fails before replication
- **Idempotent + acks=all**: Durable, safe retries, works without transactions

### Configuration

```properties
# Durability
acks=all                          # Wait for all in-sync replicas
min.insync.replicas=2             # Require 2 replicas (1 leader + 1 follower minimum)

# Idempotency
enable.idempotence=true           # Producer assigns sequence numbers, broker dedupes
max.in.flight.requests.per.connection=5  # Kafka handles ordering with idempotency

# Retries
retries=2147483647                # Effectively infinite retries
delivery.timeout.ms=120000        # 2 min total time including retries
retry.backoff.ms=100              # Exponential backoff between retries

# Batching (for throughput)
linger.ms=10                      # Wait up to 10ms to batch
batch.size=16384                  # 16KB batch size
compression.type=snappy           # Compress batches
```

---

## Architecture

```
┌──────────────────────┐
│ Event Sources        │
│                      │
│ - Postgres CDC       │
│ - HTTP API           │
│ - Internal services  │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────────────┐
│ Durable Producer (Go service)        │
│                                      │
│ CloudEvent → Kafka Record:           │
│ - Key: event subject (Party ID)      │
│ - Value: CloudEvent JSON             │
│ - Headers: CE headers                │
│ - Partition: hash(key) % partitions  │
│                                      │
│ Producer config:                     │
│ - acks=all                           │
│ - enable.idempotence=true            │
│ - retries=max                        │
└──────────┬───────────────────────────┘
           │
           ├────────────────┐
           │                │
           ▼                ▼
    ┌───────────┐    ┌───────────┐
    │ MSK       │    │ On-prem   │
    │           │    │ KRaft     │
    │ acks=all  │    │ acks=all  │
    │ min.isr=2 │    │ min.isr=2 │
    └───────────┘    └───────────┘
           │
           ▼
    ┌───────────┐
    │ Zenith    │
    │ (best     │
    │  effort)  │
    └───────────┘
```

---

## CloudEvents Mapping

### CloudEvent → Kafka Record

```go
type CloudEvent struct {
    SpecVersion     string            `json:"specversion"`      // "1.0"
    Type            string            `json:"type"`             // "party.created"
    Source          string            `json:"source"`           // "postgres://party-db"
    Subject         string            `json:"subject"`          // "party/12345"
    ID              string            `json:"id"`               // UUID
    Time            string            `json:"time"`             // RFC3339
    DataContentType string            `json:"datacontenttype"`  // "application/json"
    Data            json.RawMessage   `json:"data"`             // Party JSON
}

// Kafka mapping
record := &kafka.ProducerRecord{
    Topic:     "game.party.events",
    Key:       []byte(event.Subject),        // Partition by Party ID
    Value:     marshal(event),                // Full CloudEvent as JSON
    Headers: []kafka.Header{
        {Key: "ce_specversion", Value: []byte(event.SpecVersion)},
        {Key: "ce_type", Value: []byte(event.Type)},
        {Key: "ce_source", Value: []byte(event.Source)},
        {Key: "ce_id", Value: []byte(event.ID)},
    },
}
```

### Ordering guarantee
- **Key = event.Subject** (e.g., `party/12345`)
- All events for same Party → same partition → strict order
- Different Parties → different partitions → parallel processing

---

## Implementation

### Producer interface

```go
type DurableProducer interface {
    // Send synchronously (blocks until ack)
    Send(ctx context.Context, event CloudEvent) (partition int32, offset int64, error)
    
    // SendAsync (callback on ack)
    SendAsync(ctx context.Context, event CloudEvent, callback func(partition int32, offset int64, err error))
    
    // Close gracefully (flush pending)
    Close() error
}
```

### Error handling

```go
func (p *Producer) Send(ctx context.Context, event CloudEvent) (int32, int64, error) {
    record := toKafkaRecord(event)
    
    partition, offset, err := p.producer.SendMessage(record)
    if err != nil {
        // Retriable errors (network, leader election)
        if isRetriable(err) {
            return 0, 0, fmt.Errorf("retriable: %w", err)  // Caller can retry
        }
        
        // Non-retriable (message too large, invalid topic)
        return 0, 0, fmt.Errorf("permanent failure: %w", err)
    }
    
    // Success: event durably persisted
    return partition, offset, nil
}
```

### Failure modes

| Failure | Idempotent Producer Behavior | Outcome |
|---------|------------------------------|---------|
| Network timeout | Retry with same sequence number | Deduped by broker, no duplicate |
| Leader election | Retry, new leader has replicated data | Deduped, no duplicate |
| ISR too small | Block until `min.insync.replicas` available | Waits or fails after timeout |
| Message too large | Immediate error, no retry | Caller handles (split message?) |
| Broker full | Retry until timeout | Backpressure to caller |

---

## Consequences

### Positive
✓ No message loss (acks=all + min.isr)  
✓ No duplicates on retry (idempotent producer)  
✓ Ordering per key (partition assignment)  
✓ Works with unknown clusters (Zenith)  
✓ CloudEvents standard (interoperability)  

### Negative
✗ Higher latency (wait for all replicas)  
✗ Throughput limited by slowest replica  
✗ Blocks on ISR availability (not fire-and-forget)  
✗ Zenith may not support idempotency (unknowns)  

### Mitigations
- **Batching**: Amortize latency over multiple events (linger.ms)
- **Compression**: Reduce network overhead (snappy)
- **Async send**: Don't block application (callback-based)
- **Circuit breaker**: Fail fast if cluster unhealthy
- **Fallback**: Write-ahead log (WAL) if Kafka unavailable

---

## Zenith Considerations

### Unknown capabilities
1. **Idempotency support?** — Producer may send duplicates on retry
2. **Transaction support?** — Likely no (not using for now)
3. **ISR semantics?** — Unknown if `min.insync.replicas` honored

### Mitigation strategy
- **Best-effort delivery** to Zenith (acks=all, but expect unknowns)
- **Consumer deduplication** (audit consumer uses event ID for idempotency)
- **Monitoring** (track Zenith ack failures vs MSK/on-prem)
- **Fallback**: If Zenith unreliable, route events differently

---

## Alternatives Considered

### 1. Transactional producer (exactly-once)
**Rejected**: Unknown if Zenith supports transactions  
**Future**: Revisit if Zenith capabilities confirmed

### 2. Fire-and-forget (acks=0)
**Rejected**: Unacceptable message loss for audit

### 3. Leader-only acks (acks=1)
**Rejected**: Loses messages on leader failure

### 4. Write-ahead log (WAL) before Kafka
**Considered**: Postgres WAL → Kafka ensures durability  
**Deferred**: Adds complexity, implement if Kafka unavailable scenarios common

### 5. Dual-write (Kafka + S3)
**Considered**: S3 as backup if Kafka fails  
**Deferred**: CloudEvents in Kafka should be sufficient with acks=all

---

## Open Questions

1. **Zenith idempotency**: Does it dedupe based on producer sequence numbers?
2. **Throughput targets**: Messages/sec expected per source?
3. **Backpressure strategy**: What if Kafka can't keep up?
4. **Monitoring**: What metrics to track (latency, retries, failures)?
5. **Topic design**: Single topic `game.party.events` or topic-per-event-type?

---

## Next Steps

1. Implement Go producer with Sarama/confluent-kafka-go
2. Test against 3-node cluster (verify acks=all, min.isr=2)
3. Measure latency/throughput (single producer, batching)
4. Test failure scenarios (kill leader, ISR shrink)
5. Design monitoring/alerting (Prometheus metrics)

---

## References
- Kafka idempotent producer: https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence
- CloudEvents Kafka Protocol Binding: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/kafka-protocol-binding.md
- Kafka durability guarantees: https://kafka.apache.org/documentation/#semantics
