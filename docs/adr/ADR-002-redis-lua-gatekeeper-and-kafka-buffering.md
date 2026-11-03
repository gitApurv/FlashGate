# ADR-002: In-Memory Redis Lua Gatekeeper and Asynchronous Kafka Write Buffering

| Metadata | Details |
| :--- | :--- |
| **Status** | **Accepted** |
| **Date** | 2026-11-03 |
| **Deciders** | Senior Java Architect |
| **Context** | FlashGate Reservation Pipeline Architecture |

---

## 1. Context and Problem Statement

In a flash-sale event (e.g., 500 tickets offered to 50,000 competing buyers), $99\%$ of requests will ultimately fail to secure a ticket. 

If all requests hit the relational database (PostgreSQL):
- **Naive JPA Read-Modify-Write:** Fails due to uncoordinated concurrent transactions resulting in massive overselling.
- **Optimistic Locking (`@Version`):** Fails because thousands of concurrent transactions attempt to increment the same entity version counter, causing over $95\%$ aborts and massive CPU waste on rollbacks and retries.
- **Pessimistic Locking (`SELECT FOR UPDATE`):** Fails because serializing 50,000 transactions through a single row-level lock creates a massive queue, rapidly exhausting the HikariCP connection pool and resulting in $2,000\text{ms}+$ tail latencies and `SQLTransientConnectionException` timeouts.

The system requires an architectural mechanism that can instantly reject excess traffic, guarantee atomic decrement with zero overselling, and decouple high-throughput client requests from relational persistence.

## 2. Decision

We implement a two-tier decoupled architecture:
1. **Tier 1: Atomic In-Memory Gatekeeper via Redis Lua:**
   - Preload campaign stock, sale window, and user eligibility rules into Redis.
   - Execute an atomic Redis Lua script (`reserve_ticket.lua`) on each reservation request. Because Redis processes commands sequentially on its single-threaded execution core, checking eligibility, verifying stock, decrementing the counter, and storing the reservation execute with zero lock contention in under $1\text{ ms}$.
   - Requests arriving when stock is 0 are immediately rejected with `HTTP 410 Gone` without touching Kafka or PostgreSQL.
2. **Tier 2: Asynchronous Kafka Write Buffering:**
   - Once Redis verifies stock decrement, the application node publishes a `TicketReservedEvent` to an Apache Kafka topic (`ticket-reserved-events`) and returns `HTTP 202 Accepted` to the client.
   - A dedicated Kafka batch consumer group pulls messages and persists `PENDING_PAYMENT` records to PostgreSQL in tuned batches of 50 via JDBC batching (`hibernate.jdbc.batch_size=50`).

## 3. Consequences

### Positive Consequences
- **Instant Gatekeeping:** Over $99\%$ of traffic is deflected at the Redis layer in $<1\text{ ms}$.
- **Protected Relational Database:** PostgreSQL never sees more than the exact number of successful reservations (e.g., 500 records), eliminating row lock contention and connection pool exhaustion.
- **Strict Single-Item-per-User:** Redis `SISMEMBER` / `SADD` provides $O(1)$ duplicate user prevention across the entire campaign cluster.
- **Sub-20ms p99 Latency:** The synchronous path only involves Redis Lua and Kafka publish, easily meeting the sub-$20\text{ ms}$ latency SLO.

### Negative Consequences & Mitigations
- **Eventual Consistency:** The client receives `HTTP 202 Accepted` before the row is physically committed to PostgreSQL.
  - *Mitigation:* The API provides a `statusCheckUrl` (`GET /api/v1/reservations/{reservationId}`) allowing clients to poll or await WebSocket push notification for final confirmation.
- **Dual-Write Risk (App Node Crash):** If an application node crashes after Redis decrements stock but before Kafka receives the event, stock could be permanently leaked.
  - *Mitigation:* `reserve_ticket.lua` records `reservation_id -> user_id:timestamp` in a Redis Hash (`pending_sync`). A background reconciliation process (`PendingSyncReconciler`) audits entries older than 30 seconds to restore leaked stock or republish to Kafka.
