# ADR-003: Broad Consumer Groups at Enterprise Scale

**Status**: Proposed  
**Date**: 2026-06-30  
**Context**: 180K-300K consumers, high idempotency requirement, enterprise traffic volume

---

## Context

### Problem
Support 180K-300K concurrent consumers with guarantees:
- **High idempotency** — consumers must safely handle duplicate events
- **Scalable consumption** — partition assignment works at this scale
- **Selective consumption** — consumers only receive relevant events
- **Low rebalance overhead** — minimize disruption from consumer churn
- **Enterprise traffic** — not megascale message volume, but massive consumer diversity

### Use cases
- **Game clients**: 180K-300K active players consuming Party/world state
- **Microservices**: Each service consumes events relevant to its domain
- **Multi-tenant**: Per-tenant consumers for isolated event streams
- **Regional sharding**: Consumers grouped by geographic region

### Constraints
- **Kafka partition limits**: ~10K partitions per cluster (practical max)
- **Consumer group rebalancing**: Stop-the-world at scale without tuning
- **Network overhead**: 300K connections to brokers is significant
- **Heterogeneous clusters**: MSK, on-prem, Zenith all must support this

---

## Decision

### Consumer topology: **Hierarchical consumer groups with event filtering**

**Not**: 300K consumer groups (partition assignment overhead insane)  
**Not**: Single consumer group (rebalancing would block 300K consumers)  
**Instead**: Layered consumption with filtering

---

## Architecture

### Layer 1: Topic Partitioning (Kafka native)

```
Topic: game.party.events
Partitions: 1000 (high parallelism, manageable)

Partition key: hash(Party.region + Party.id) % 1000
- All events for same Party → same partition (ordering)
- Parties in same region tend to same partition (locality)
```

### Layer 2: Consumer Group Sharding

```
Consumer groups organized by region/shard:

- game-client-us-east (30K consumers, 200 partitions)
- game-client-us-west (25K consumers, 150 partitions)
- game-client-eu-central (40K consumers, 250 partitions)
- game-client-ap-southeast (35K consumers, 200 partitions)
- ...
- audit-global (10 consumers, 1000 partitions) — reads everything
- analytics-global (50 consumers, 1000 partitions) — reads everything
```

**Key insight**: Not every consumer needs ALL events  
- Game clients filter by region/proximity  
- Services filter by event type  
- Audit/analytics consume everything

### Layer 3: Application-Level Filtering

```go
// Consumer receives events from assigned partitions
// Filters in-application before processing

type Consumer struct {
    groupID     string
    filter      EventFilter
    processor   EventProcessor
    seenEvents  *SeenEventCache  // Idempotency tracking
}

func (c *Consumer) Poll(ctx context.Context) {
    records := c.kafkaConsumer.Poll(ctx)
    
    for _, record := range records {
        event := parseCloudEvent(record)
        
        // Idempotency check
        if c.seenEvents.Contains(event.ID) {
            continue  // Skip duplicate
        }
        
        // Application filter
        if !c.filter.Matches(event) {
            continue  // Not relevant to this consumer
        }
        
        // Process
        if err := c.processor.Process(event); err != nil {
            // Don't commit offset on failure
            return err
        }
        
        // Mark seen
        c.seenEvents.Add(event.ID)
    }
    
    // Commit offsets after successful batch
    c.kafkaConsumer.CommitSync()
}
```

---

## Consumer Group Design

### Partition assignment strategy: **Static membership**

```properties
# Consumer config
group.id=game-client-us-east
group.instance.id=client-12345-us-east-a  # Unique per consumer instance
session.timeout.ms=45000                   # 45 sec (higher for stability)
heartbeat.interval.ms=3000                 # 3 sec
max.poll.interval.ms=300000                # 5 min (processing time tolerance)
partition.assignment.strategy=CooperativeStickyAssignor
```

**Static membership benefits**:
- Consumer rejoins with same `group.instance.id` → keeps same partitions
- No rebalance on temporary disconnect (within session timeout)
- Reduces rebalance frequency at scale

**Cooperative rebalancing**:
- Only affected partitions reassigned (not stop-the-world)
- Consumer keeps processing non-reassigned partitions during rebalance
- Critical at 30K+ consumers per group

### Consumer group sizing

| Consumer Group | Consumers | Partitions | Consumers per Partition |
|----------------|-----------|------------|------------------------|
| game-client-us-east | 30,000 | 200 | 150 |
| game-client-us-west | 25,000 | 150 | 167 |
| game-client-eu-central | 40,000 | 250 | 160 |
| audit-global | 10 | 1000 | 0.01 (over-partitioned) |
| analytics-global | 50 | 1000 | 0.05 (over-partitioned) |

**Over-subscription pattern**:
- More consumers than partitions = some consumers idle (standby)
- Provides instant failover (idle consumer picks up partition)
- Acceptable for lightweight game clients

---

## Idempotency Strategy

### Application-level deduplication (required)

**Why not rely on Kafka exactly-once?**
- Unknown if Zenith supports it
- Consumer may crash after processing but before commit
- Network retries may deliver same message twice

### Implementation: Seen-event cache

```go
type SeenEventCache struct {
    cache *lru.Cache  // LRU cache of event IDs
    ttl   time.Duration
}

func NewSeenEventCache(size int, ttl time.Duration) *SeenEventCache {
    return &SeenEventCache{
        cache: lru.New(size),      // e.g., 10K recent events
        ttl:   ttl,                // e.g., 1 hour
    }
}

func (s *SeenEventCache) Contains(eventID string) bool {
    if item, ok := s.cache.Get(eventID); ok {
        if time.Since(item.timestamp) < s.ttl {
            return true  // Seen recently
        }
        s.cache.Remove(eventID)  // Expired
    }
    return false
}

func (s *SeenEventCache) Add(eventID string) {
    s.cache.Add(eventID, &CacheItem{
        eventID:   eventID,
        timestamp: time.Now(),
    })
}
```

**Tuning**:
- Cache size: 10K-100K events (memory vs dedup window)
- TTL: 1-24 hours (longer = better dedup, more memory)
- Persistence: Optional (write to local DB for crash recovery)

### CloudEvent ID as dedup key

```json
{
  "specversion": "1.0",
  "type": "party.created",
  "source": "postgres://party-db",
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",  ← Unique, deterministic
  "subject": "party/12345",
  "time": "2026-06-30T10:00:00Z",
  "data": {...}
}
```

**Event ID generation**:
- **Source system**: Postgres generates UUID on write (deterministic)
- **Idempotent**: Same event = same ID (even if produced twice)
- **Global**: Unique across all clusters (UUID collision ~impossible)

---

## Rebalancing at Scale

### Problem: 30K consumers rebalancing simultaneously

**Without tuning**: Stop-the-world, 30K consumers pause, reassign all partitions, resume → **minutes of downtime**

**With tuning**:

```properties
# Incremental cooperative rebalancing
partition.assignment.strategy=CooperativeStickyAssignor

# Static membership (avoid rebalance on transient failure)
group.instance.id=${CLIENT_ID}  # Set per consumer instance
session.timeout.ms=45000         # 45 sec tolerance

# Delayed rebalancing (wait for consumer to reconnect)
group.initial.rebalance.delay.ms=3000  # Wait 3s before first rebalance
```

**Strategies**:
1. **Rolling restarts**: Deploy new consumers gradually (not all at once)
2. **Blue/green consumer groups**: Deploy to new group, cut over, retire old
3. **Partition reservation**: Pre-assign partitions to consumer IDs (manual, but stable)

---

## Network and Broker Considerations

### Connection pooling

**Problem**: 300K TCP connections to brokers = resource exhaustion

**Solution**: Consumer proxy/gateway

```
┌─────────────────────────────────────────┐
│ Game Clients (300K)                     │
│ - WebSocket to regional gateway         │
│ - Receives filtered events              │
└──────────────┬──────────────────────────┘
               │ WebSocket (regional)
               ▼
┌──────────────────────────────────────────┐
│ Consumer Gateway (per region)            │
│ - 1K Kafka consumers per gateway         │
│ - Filters events                         │
│ - Pushes to clients via WebSocket        │
└──────────────┬───────────────────────────┘
               │ Kafka protocol
               ▼
┌──────────────────────────────────────────┐
│ Kafka Cluster                            │
│ - 1000 partitions                        │
│ - 10K actual Kafka connections           │
│   (not 300K)                             │
└──────────────────────────────────────────┘
```

**Architecture shift**:
- **Not**: 300K direct Kafka consumers
- **Instead**: 10K Kafka consumers in gateways, 300K WebSocket clients

**Benefits**:
- Kafka cluster sees manageable connection count
- Gateways handle filtering/fanout
- Clients use lightweight WebSocket (not full Kafka client)
- Regional gateways reduce cross-region traffic

---

## Event Filtering Strategy

### Partition-level routing (coarse)

```
Partition assignment based on region:
- US-East clients → partitions 0-199
- US-West clients → partitions 200-349
- EU-Central clients → partitions 350-599
- ...
```

**Limitation**: Still delivers events from other regions (filtered in-app)

### Topic-level routing (finer)

```
Topics per region:
- game.party.events.us-east
- game.party.events.us-west
- game.party.events.eu-central

Consumers subscribe to regional topic only
```

**Tradeoff**: More topics, but consumers only see relevant events

### Hybrid: Partitioning + filtering

```
Single topic: game.party.events (1000 partitions)

Producer: partition key = hash(region + party_id)
  → Events for same region cluster in partition ranges

Consumer: 
  1. Subscribe to partition range (Kafka-level)
  2. Filter by proximity (application-level)

Example:
- US-East consumer assigned partitions 0-199
- Receives mostly US-East events (co-located)
- Filters out occasional EU/AP events (hash collision)
```

---

## Monitoring and Observability

### Key metrics

**Consumer lag (per group)**:
```
kafka_consumer_lag{group="game-client-us-east",partition="42"} 1523
```
- Alert if lag > 10K messages (falling behind)

**Rebalance frequency**:
```
kafka_consumer_rebalances_total{group="game-client-us-east"} 12
```
- Alert if > 1 per hour (instability)

**Duplicate event rate**:
```
app_duplicate_events_total{consumer_id="client-12345"} 47
```
- Normal: <1% duplicates (network retries)
- High: >5% duplicates (investigate producer/consumer issues)

**Processing latency**:
```
app_event_processing_duration_seconds{p99} 0.15
```
- Alert if p99 > max.poll.interval.ms (will trigger rebalance)

---

## Consequences

### Positive
✓ Scales to 300K consumers without partition explosion  
✓ High idempotency via application-level dedup  
✓ Regional filtering reduces irrelevant traffic  
✓ Cooperative rebalancing minimizes disruption  
✓ Static membership reduces rebalance frequency  
✓ Gateway pattern reduces broker connection load  

### Negative
✗ Complexity: Gateways, filtering, caching all add layers  
✗ Memory overhead: Seen-event cache per consumer  
✗ Network hops: Client → Gateway → Kafka (added latency)  
✗ Over-subscription: Idle consumers waste resources  
✗ Operational burden: Monitor 100+ consumer groups  

### Mitigations
- **Start simple**: Direct Kafka consumers, add gateways if needed
- **Cache tuning**: Right-size seen-event cache (10K vs 100K)
- **Metrics**: Instrument everything (lag, rebalances, duplicates)
- **Alerting**: Catch rebalance storms early

---

## Alternatives Considered

### 1. Single massive consumer group (300K consumers)
**Rejected**: Rebalancing 300K consumers = outage-level event

### 2. One partition per consumer (300K partitions)
**Rejected**: Kafka doesn't scale to 300K partitions (metadata overhead)

### 3. Pub/sub fanout (Redis, NATS)
**Considered**: Better for ephemeral clients, worse for durability  
**Deferred**: Kafka provides audit trail, offset management

### 4. Server-sent events (SSE) instead of Kafka consumers
**Considered**: Lightweight for game clients  
**Hybrid**: Gateway uses Kafka, pushes to clients via SSE/WebSocket

### 5. Kafka Streams for filtering
**Considered**: Stream processing to filter events into per-region topics  
**Deferred**: Adds complexity, evaluate if in-app filtering insufficient

---

## Open Questions

1. **Gateway scaling**: How many clients per gateway instance?
2. **Seen-event cache persistence**: Write to disk for crash recovery?
3. **Cross-region replication**: Should EU clients see US events (eventually)?
4. **Zenith compatibility**: Does it support CooperativeStickyAssignor?
5. **Consumer churn rate**: How often do clients connect/disconnect?

---

## Next Steps

1. Test consumer group at scale (simulate 10K, 30K, 100K consumers)
2. Measure rebalance duration (static membership vs dynamic)
3. Benchmark gateway architecture (Kafka → WebSocket fanout)
4. Implement seen-event cache (LRU, TTL, optional persistence)
5. Monitor duplicate event rate (idempotency effectiveness)

---

## References
- Kafka consumer groups: https://kafka.apache.org/documentation/#consumerconfigs
- Incremental cooperative rebalancing: https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
- Static membership: https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances
- CloudEvents spec: https://cloudevents.io
