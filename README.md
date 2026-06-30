# Kuule — Hex-Based Event Topology System

**Status**: Learning project / AI certification training  
**Philosophy**: Ephemeral runtime, durable config. Hypothetically promotable to exabyte scale.

---

## What This Is

Event-driven system orchestration using:
- **Hex geometry** (M-19 substrate) as semantic configuration layer
- **CloudEvents** as source of truth (event-sourced topology)
- **Kafka KRaft** as both platform and configuration target
- **pgvector + AI** for learned patterns and config generation

**Not**: A Kafka monitoring tool  
**Instead**: A performance measurement harness + AI learning framework

---

## Architecture

```
CloudEvents (hex.topology.events)
    ↓
Event Stream = Configuration Source of Truth
    ↓
Containers orchestrated via event consumption
    ↓
Metrics embedded in pgvector
    ↓
AI learns patterns, generates configs
```

**Core insight**: The hex substrate IS event-sourced. Each hex mutation = CloudEvent.

---

## Components

### Event Schema
- `events/hex-topology-schema.json` — CloudEvents for hex topology
- Event types: `hex.node.assigned`, `hex.relationship.created`, `hex.disruption.injected`

### CLI Tools
- `kuule` — Event producer (assign nodes to hex coordinates)
- `kuule-runtime` — Event consumer (orchestrates containers from events)

### Container Images
- `Dockerfile` — Full Kafka + Spring Boot (654MB)
- `Dockerfile.minimal` — Arch Linux + JRE + Kafka 3.7.0 (876MB, target <200MB)

### Configs
- `kraft-configs/` — Controller + broker properties (minimal resource tuning)
- `launch-minimal-cluster.sh` — Bootstrap 1 controller + 2 brokers

### ADRs (Architectural Decision Records)
- `adr-001-high-fidelity-audit-consumer.md` — At-least-once + idempotency
- `adr-002-durable-producer.md` — acks=all, idempotent producer
- `adr-003-broad-consumer-groups.md` — 300K consumers, gateway pattern

### AI/ML Layer
- `schema.sql` — pgvector schema for topology embeddings
- `embed_corpus.py` — Embedding pipeline (ADRs/configs → pgvector)

---

## Quick Start

**Prerequisites**:
- Podman (rootless containers)
- PostgreSQL 17 + pgvector
- Kafka running (or use containers)
- Python 3 + kafka-python

**Launch minimal cluster**:
```bash
./launch-minimal-cluster.sh
```

**Produce topology events**:
```bash
./kuule boot                # Controller at Hiljaisuus (0,0,0)
./kuule raft-monitor        # Observability node at (1,0,-1)
```

**Consume events → orchestrate containers**:
```bash
./kuule-runtime
```

---

## Design Principles

### Ephemeral Runtime, Durable Config
- Containers are disposable (kill/restart freely)
- `hex.topology.events` is truth (replay = rebuild)
- State in Kafka, not in containers

### Event-Sourced Orchestration
- Every topology change is a CloudEvent
- Audit trail: who changed what, when
- Replay: reconstruct any historical state
- Branching: fork event log, test different topologies

### AI as Event Principal
- AI agents consume events (learn patterns)
- AI agents produce events (generate configs)
- Feedback loop: outcomes → embeddings → better configs

### Hex Geometry as Semantic Layer
- M-19 substrate: 19 cells (Hiljaisuus + Ring 1 + Ring 2)
- Spatial relationships encode logical relationships
- Rails (s-rail, q-rail, r-rail) carry semantic context
- Rotation algebra for state mutations

---

## Measurement Goals

**What does it COST to replicate one CloudEvent from Hiljaisuus across 6 nodes?**

Measured in:
- JVM cycles (GC, serialization, context switches)
- Network latency (loopback TCP, or optimized transport)
- Kafka protocol overhead
- Waste (anything that doesn't directly contribute)

**Instrumentation**:
- Each hex reports: `hex.metrics.jvm` (timestamp, CPU cycles, heap allocation)
- Aggregation: correlate by event ID across all hexes
- Optimization experiments: TCP → Unix sockets → shared memory → eBPF

**Minimal footprint**:
- Controller: 159MB RAM (measured)
- Broker: 256MB+ RAM (LogCleaner requirement)
- Total: ~670MB for 3-node cluster

---

## Roadmap

**Current** (2026-06-30):
- ✅ Event schema (CloudEvents for hex topology)
- ✅ CLI tools (kuule, kuule-runtime)
- ✅ Container orchestration (event-driven podman)
- ✅ ADRs (audit, producer, consumer patterns)
- ✅ Minimal cluster (1 controller + 2 brokers)
- ⏸️ Brokers running (256MB heap floor discovered)

**Next**:
- [ ] Fix broker startup (bump heap to 256MB)
- [ ] Test CloudEvent replication across 2 brokers
- [ ] Measure end-to-end latency (produce → replicate → consume)
- [ ] Instrumentation (JVM metrics per hex)
- [ ] UI event pattern (click hex → launch visualizer)

**Horizon**:
- [ ] GraalVM native-image experiment (Opus agent)
- [ ] Enterprise topology abstraction (strip hex-ness)
- [ ] Federated pgvector (multi-datacenter learning)
- [ ] Network optimization (Unix sockets, shared memory)

---

## Learning Objectives

**For students**:
1. Kafka internals (KRaft, replication, ISR, partition leadership)
2. Event-driven architecture (CloudEvents, event sourcing)
3. AI fundamentals (embeddings, semantic search, RAG)
4. Distributed systems (replication boundaries, failure modes)
5. Hex geometry (spatial reasoning, visual programming)

**Meta-lesson**: Design for unknowns by using minimal assumptions.

---

## References

- M-19 substrate: `~detti/rebraining.org/essays/kaamos/math_vocabulary.md`
- CloudEvents spec: https://cloudevents.io
- Kafka KRaft: https://kafka.apache.org/documentation/#kraft
- pgvector: https://github.com/pgvector/pgvector
- Bedrock metadata format: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.VectorDB.html

---

## License

TBD (learning project, not production software)

## Contact

TBD
