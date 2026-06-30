# ADR-001: High Fidelity Consumer (Audit)

**Status**: Proposed  
**Date**: 2026-06-30  
**Context**: Multi-cluster Kafka replication (MSK, on-prem 3.7.0, Zenith) with audit-first requirements

---

## Context

### Problem
Need to consume events from heterogeneous Kafka sources (MSK, on-prem KRaft, Zenith) with guarantees:
- **No message loss** — every event captured for audit
- **Ordering preserved** — per-partition order maintained
- **Provable completeness** — can verify no gaps in audit trail
- **Cross-cluster fidelity** — audit works regardless of source cluster

### Replication boundaries
- **MSK → Audit**: AWS managed, known behavior
- **On-prem KRaft 3.7.0 → Audit**: Direct control, KRaft metadata local
- **KRaft 3.7 ↔ 4.x → Audit**: Version heterogeneity
- **Zenith → Audit**: Unknown internals, Kafka facade

### Audit requirements
- Store in pgvector (embeddings for pattern detection)
- CloudEvents format compliance
- Bedrock-compatible metadata (JSONB)
- No runtime AI (deterministic, auditable itself)

---

## Decision

### Consumer semantics: **At-least-once with idempotency**

**Rationale**:
- **Not exactly-once**: Zenith capabilities unknown, can't guarantee across all sources
- **Not at-most-once**: Unacceptable for audit (message loss)
- **At-least-once + idempotent write**: Safe, works with any source

### Implementation

**1. Consumer group per source cluster**
```
audit-msk-consumer (group: audit-msk)
audit-onprem-consumer (group: audit-onprem)
audit-zenith-consumer (group: audit-zenith)
```

**2. Offset management**
- **Kafka offset commits** (automatic via consumer group)
- **Postgres watermark** (last processed offset per partition)
- **Reconciliation**: On restart, max(kafka_offset, postgres_watermark)

**3. Idempotent writes**
```sql
INSERT INTO event_vectors (id, event_id, ...)
VALUES (...)
ON CONFLICT (event_id) DO NOTHING;
```

**4. Ordering guarantee**
- Single consumer per partition (consumer group handles this)
- Postgres insert order = Kafka partition order
- Partition number stored in metadata for audit trail

**5. Completeness verification**
```sql
-- Detect gaps in offset sequence per partition
SELECT partition, 
       offset_value,
       LAG(offset_value) OVER (PARTITION BY partition ORDER BY offset_value) as prev_offset
FROM event_vectors
WHERE offset_value - prev_offset > 1;
```

---

## Architecture

```
┌─────────────┐
│ MSK         │──┐
└─────────────┘  │
                 │
┌─────────────┐  │    ┌──────────────────────┐
│ On-prem     │──┼───→│ Audit Consumer       │
│ KRaft 3.7   │  │    │ (Go service)         │
└─────────────┘  │    │                      │
                 │    │ - At-least-once      │
┌─────────────┐  │    │ - Offset tracking    │
│ Zenith      │──┘    │ - Idempotent writes  │
└─────────────┘       └──────────┬───────────┘
                                 │
                                 ▼
                      ┌──────────────────────┐
                      │ PostgreSQL + pgvector│
                      │                      │
                      │ event_vectors table  │
                      │ - CloudEvents        │
                      │ - Embeddings (768d)  │
                      │ - Offset watermarks  │
                      └──────────────────────┘
```

---

## Consequences

### Positive
✓ Works with unknown sources (Zenith)  
✓ No duplicate audit records (idempotency)  
✓ Provable completeness (gap detection)  
✓ Survives consumer restarts (offset reconciliation)  
✓ Cross-cluster compatible (no special cluster features required)

### Negative
✗ Slightly higher latency (Postgres write per message)  
✗ Duplicate processing on failure (must be idempotent)  
✗ Storage overhead (offset tracking in two places)

### Mitigations
- **Batch writes**: Accumulate N messages, write batch, commit offsets
- **Async embeddings**: Write event first, embed async (separate process)
- **Partition parallelism**: Scale consumers horizontally per partition

---

## Alternatives Considered

### 1. Exactly-once semantics
**Rejected**: Requires Kafka transactions, unknown if Zenith supports

### 2. At-most-once (fire-and-forget)
**Rejected**: Unacceptable for audit (message loss possible)

### 3. External offset store only (no Kafka commits)
**Rejected**: Consumer group rebalancing breaks, no auto-assignment

### 4. Dual-write to S3 + Postgres
**Considered**: S3 as immutable archive, Postgres for query  
**Deferred**: Adds complexity, implement if Postgres becomes bottleneck

---

## Open Questions

1. **Zenith wire protocol**: Does it support Kafka consumer groups?
2. **Embedding performance**: Can we keep up with message rate?
3. **Postgres scaling**: What's the write throughput limit?
4. **Replication lag tolerance**: How far behind can audit fall?

---

## Next Steps

1. Implement Go consumer skeleton (Kafka → Postgres)
2. Test idempotent write performance (single partition)
3. Measure embedding throughput (EmbeddingGemma)
4. Design monitoring (lag, gaps, throughput)
5. Test against 3-node cluster

---

## References
- CloudEvents spec: https://cloudevents.io
- Kafka consumer semantics: https://kafka.apache.org/documentation/#semantics
- pgvector Bedrock integration: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.VectorDB.html
