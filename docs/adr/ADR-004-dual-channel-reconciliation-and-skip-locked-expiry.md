# ADR-004: Dual-Channel Expiration and Abandoned Cart Restocking via SKIP LOCKED

| Metadata | Details |
| :--- | :--- |
| **Status** | **Accepted** |
| **Date** | 2026-11-03 |
| **Deciders** | Senior Java Architect |
| **Context** | Reclaiming Unpaid Flash-Sale Reservations After 10-Minute TTL |

---

## 1. Context and Problem Statement

When a buyer successfully reserves a ticket, they receive a 10-minute window to complete payment (`status = PENDING_PAYMENT`). If the user abandons checkout or payment fails, the reserved ticket must be returned to the campaign pool immediately so other buyers can purchase it.

We evaluated two architectural patterns for handling TTL expiration:
1. **Option A: Redis Keyspace Notifications (Expired Key Events):** Setting a 10-minute TTL on Redis keys and subscribing via Redis Pub/Sub to `__keyevent@*__:expired`.
2. **Option B: Database Polling with `FOR UPDATE SKIP LOCKED`:** A scheduled background worker polling PostgreSQL for expired rows using `FOR UPDATE SKIP LOCKED`.

### Analysis of Option A (Redis Keyspace Notifications)
- Redis Pub/Sub is fire-and-forget; if the subscriber node is restarting, network partitions occur, or the buffer overflows, the expiration event is lost forever.
- Redis keyspace notifications do not fire at the exact expiration second; Redis purges expired keys lazily (on access or via probabilistic background sampling), leading to unpredictable delays (sometimes minutes late).
- Redis notifications carry only the key name, not the associated transaction metadata required for relational state reconciliation.

## 2. Decision

We choose **Option B: Scheduled Database Polling using PostgreSQL `FOR UPDATE SKIP LOCKED`** backed by a partial index, augmented with an atomic Lua release script.

### 2.1 Implementation Mechanics
1. **Database Schema & Partial Indexing:**
   ```sql
   CREATE INDEX idx_reservations_pending_expiry 
       ON reservations (expires_at) 
       WHERE status = 'PENDING_PAYMENT';
   ```
2. **Periodic Polling Query (Every 5 seconds):**
   ```sql
   SELECT r.id, r.campaign_id, r.user_id 
   FROM reservations r 
   WHERE r.status = 'PENDING_PAYMENT' 
     AND r.expires_at <= NOW() 
   ORDER BY r.expires_at ASC 
   LIMIT 100 
   FOR UPDATE SKIP LOCKED;
   ```
3. **Atomic Redis Restock Script (`release_ticket.lua`):**
   - For each reclaimed row:
     - Update PostgreSQL status: `PENDING_PAYMENT` $\rightarrow$ `EXPIRED`.
     - Execute `release_ticket.lua` against Redis:
       ```lua
       local removed = redis.call("SREM", KEYS[2], ARGV[1])
       if removed == 1 then
           redis.call("INCR", KEYS[1])
           return 1
       end
       return 0
       ```
   - Only if the user was still present in `campaign:{id}:users` is `campaign:{id}:stock` incremented, ensuring strict idempotency and avoiding duplicate restocks.

## 3. Consequences

### Positive Consequences
- **Guaranteed At-Least-Once Reclaim:** No expired reservation can slip through; every row is durably tracked in PostgreSQL.
- **Zero Lock Contention Across Worker Nodes:** `SKIP LOCKED` allows multiple application instances or threads to poll simultaneously without blocking or deadlocking each other.
- **Index Efficiency:** The partial index only contains unexpired pending rows (a very small working set), ensuring index lookups execute in microseconds.
- **Idempotent Restocking:** Even if a scheduler run is interrupted or repeated, `release_ticket.lua` verifies user presence in the Set before incrementing stock, preventing phantom inventory inflation.

### Negative Consequences & Mitigations
- **Slight Polling Latency:** Stock is reclaimed within 5 seconds of actual TTL expiration rather than instantaneously.
  - *Assessment:* A 5-second window is well within acceptable business tolerances for a 10-minute checkout TTL.
- **Periodic DB Load:** The scheduler polls the database every 5 seconds.
  - *Mitigation:* Because the partial index only indexes `PENDING_PAYMENT` rows, empty scans cost sub-millisecond CPU time on PostgreSQL.
