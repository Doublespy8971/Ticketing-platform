# Flash-Sale Ticketing Platform — Architecture Index

This index references the architecture specifications partitioned across `docs/arch/`:

- [docs/arch/01-services.md](arch/01-services.md): Service responsibilities, ports, system topology, and ownership boundaries.
- [docs/arch/02-saga.md](arch/02-saga.md): Order saga choreography, state machines, transition tables, and edge-case handling.
- [docs/arch/03-kafka.md](arch/03-kafka.md): Kafka broker configuration, KRaft mode, domain topics, and retry/DLT topic conventions.
- [docs/arch/04-events.md](arch/04-events.md): Common event envelope specification and JSON payload schemas for all domain events.
- [docs/arch/05-outbox-idempotency.md](arch/05-outbox-idempotency.md): Transactional outbox polling protocol and HTTP/Kafka idempotency controls.
- [docs/arch/06-api.md](arch/06-api.md): REST API contracts, JWT authentication, and endpoint specifications for all services.
- [docs/arch/07-database.md](arch/07-database.md): PostgreSQL 16 database schemas, DDL, constraints, and anti-overselling guarantees.
- [docs/arch/08-redis.md](arch/08-redis.md): Redis key models, atomic Lua scripts, TTL policies, and PostgreSQL-authoritative hold expiry.
- [docs/arch/09-conventions.md](arch/09-conventions.md): Cross-cutting package layout, telemetry, logging, dependency versions, and SLO targets.
- [docs/arch/10-failures.md](arch/10-failures.md): Comprehensive failure scenarios matrix and verification/testing strategies.
- [docs/DECISIONS.md](DECISIONS.md): Architectural Decision Records (ADRs), core assumptions, and future work.
