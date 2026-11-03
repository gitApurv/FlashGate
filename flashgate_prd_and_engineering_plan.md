# Product & Engineering Requirements Document (PRD): FlashGate

| Document Metadata | Details |
| :--- | :--- |
| **Project Title** | **FlashGate** — High-Concurrency Flash Sale & Ticketing Engine |
| **Version** | 1.0.0 |
| **Status** | Approved for Implementation |
| **Target Stack** | Java 21 (Virtual Threads), Spring Boot 3.4+, PostgreSQL 16, Redis 7.2, Apache Kafka (KRaft), Flyway, Testcontainers, Micrometer/Prometheus, Grafana, k6 |
| **Primary Objective** | Process $50,000+$ concurrent reservation requests for $500$ limited-stock items with **zero overselling**, **strict per-user purchase limits**, and **sub-$20\text{ ms}$ p99 latency**. |

---

## 1. Executive Summary & Problem Statement

Standard e-commerce backends rely on synchronous relational database transactions (`SELECT` followed by `UPDATE`) to verify and decrement inventory. During a flash sale or high-demand event ticket drop, traffic spikes $100\times$ within a single millisecond. Synchronous database operations immediately exhaust the HikariCP connection pool, trigger severe row-level lock contention in PostgreSQL, and lead to cascading timeouts, node crashes, or oversold inventory.

**FlashGate** solves this by decoupling **high-speed inventory reservation** from **durable order persistence**:
1. **In-Memory Atomic Gatekeeper:** Redis Lua scripts validate user eligibility and decrement stock atomically in less than $1\text{ ms}$, rejecting $99\%$ of excess traffic before it ever reaches the database.
2. **Non-Blocking Concurrency:** Java 21 Virtual Threads handle tens of thousands of concurrent open sockets without reactive callback complexity.
3. **Asynchronous Write Buffering:** Apache Kafka buffers successful reservations so PostgreSQL writes occur at a steady, predictable batch rate.
4. **Self-Healing Inventory Recovery:** A background expiration worker reclaims unpaid reservations after a $10\text{-minute}$ TTL and restores stock to Redis automatically.

To maximize engineering and portfolio value, FlashGate implements the **Strategy Pattern** to allow runtime switching between four concurrency control modes (`NAIVE`, `OPTIMISTIC_LOCK`, `PESSIMISTIC_LOCK`, and `REDIS_KAFKA_PIPELINE`) for side-by-side load-testing benchmarks.

---

## 2. System Scope & Actors

### 2.1 System Actors
* **Buyer (Client):** Authenticated user attempting to reserve $1$ unit of a flash-sale item at the exact start timestamp.
* **Platform Admin:** Internal operator who creates flash-sale campaigns, configures inventory counts, and triggers pre-sale Redis cache warmups.
* **Payment Provider (Webhook Simulator):** External service that notifies FlashGate whether a buyer completed or failed checkout within the reservation window.
* **Expiration & Reconciliation Worker:** Internal scheduled background process that reclaims stock from expired unpaid reservations and reconciles Redis state against PostgreSQL.

### 2.2 Scope Boundaries
* **In Scope:**
  * Campaign creation and pre-sale Redis cache warming.
  * Multi-strategy concurrency control (`v1` through `v4`).
  * Idempotency key enforcement and $1\text{-item-per-user}$ verification.
  * Asynchronous Kafka order persistence and state machine enforcement.
  * $10\text{-minute}$ unpaid reservation TTL expiry and automatic Redis restocking.
  * Simulated HMAC-signed payment webhook ingestion.
  * Prometheus/Grafana observability dashboards and k6 load-testing scripts.
* **Out of Scope:**
  * Frontend web/mobile UI.
  * Full OAuth2/OIDC identity provider implementation (requests authenticate via a lightweight JWT or `X-User-Id` header).
  * Live third-party payment gateway integration (simulated via webhook endpoints).
  * Physical shipping and fulfillment workflows.

---

## 3. Target System Architecture

```text
[Buyer Client]
   │  1. POST /api/v1/campaigns/{id}/reserve (Headers: X-User-Id, Idempotency-Key)
   ▼
[Spring Boot 3.4 + Java 21 Virtual Threads]
   │
   ├──► Local Clock Check (Reject with HTTP 403 if before sale_starts_at or after sale_ends_at)
   │
   ├──► [Redis 7.2: reserve_ticket.lua] (Single-threaded atomic check & decrement in < 1ms)
   │       ├── Return -1 (Duplicate User) ──► HTTP 409 Conflict
   │       ├── Return  0 (Stock == 0)     ──► HTTP 410 Gone (99% of spike traffic stops here)
   │       └── Return  1 (Stock Decremented & Stream Entry Written)
   │
   ├──► [Apache Kafka: topic "ticket-reserved-events"]
   │       └── Returns HTTP 202 Accepted + reservationId to Client immediately
   │
   ▼
[Kafka Batch Consumer Worker]
   ├──► Inserts PENDING_PAYMENT row into PostgreSQL 16 (expires_at = NOW() + 10 mins)
   │
   ▼
[Resolution Path A: Payment Webhook]           [Resolution Path B: Expiry Scheduler (Every 5s)]
   ├── POST /api/v1/webhooks/payments             ├── Polls expired PENDING_PAYMENT rows (SKIP LOCKED)
   └── Marks row CONFIRMED in PostgreSQL          ├── Marks row EXPIRED in PostgreSQL
                                                  └── Runs release_ticket.lua in Redis (+1 stock)
```

---

## 4. Functional Requirements (FRs)

### FR-1: Campaign & Inventory Management
* **FR-1.1:** Admin can create a flash-sale campaign specifying `title`, `total_stock`, `price_cents`, `sale_starts_at`, and `sale_ends_at`.
* **FR-1.2:** Admin can invoke a **Cache Warmup** endpoint prior to `sale_starts_at`, loading `available_stock` and campaign window timestamps into Redis and local application memory cache (Caffeine).

### FR-2: High-Throughput Ticket Reservation Gatekeeper
* **FR-2.1 (Window Enforcement):** Requests arriving before `sale_starts_at` or after `sale_ends_at` must be rejected in memory (`HTTP 403 Forbidden`) using local clock checks against cached campaign metadata without querying PostgreSQL or Redis.
* **FR-2.2 (Idempotency):** Every reservation request must supply an `Idempotency-Key` header. Retrying an identical request must return the original reservation response without double-decrementing inventory.
* **FR-2.3 (One-Per-User Rule):** A single `user_id` may hold at most one active (`PENDING_PAYMENT`) or completed (`CONFIRMED`) reservation per `campaign_id`.
* **FR-2.4 (Atomic Lua Execution):** In `REDIS_KAFKA_PIPELINE` mode, checking user eligibility, verifying stock is greater than $0$, decrementing stock, recording the user ID, and writing a recovery log entry must execute inside a single atomic Redis Lua script.
* **FR-2.5 (Runtime Strategy Switching):** The request header `X-Concurrency-Mode` (or application default config) must route requests to one of four implementations:
  1. `NAIVE` (Standard JPA Read-Modify-Write)
  2. `OPTIMISTIC_LOCK` (JPA `@Version` with retry)
  3. `PESSIMISTIC_LOCK` (PostgreSQL `SELECT ... FOR UPDATE`)
  4. `REDIS_KAFKA_PIPELINE` (Redis Lua + Kafka + Virtual Threads)

### FR-3: Asynchronous Persistence & Reservation State Machine
* **FR-3.1:** Upon receiving return code `1` from Redis, the API generates a deterministic or UUIDv7 `reservation_id`, publishes a `TicketReservedEvent` to Kafka, and returns `HTTP 202 Accepted`.
* **FR-3.2:** A Kafka consumer group persists reservations in batches to PostgreSQL with status `PENDING_PAYMENT` and `expires_at = created_at + 10 minutes`.
* **FR-3.3:** Reservations must obey strict state transitions:
  * `PENDING_PAYMENT` $\rightarrow$ `CONFIRMED` (Payment succeeded)
  * `PENDING_PAYMENT` $\rightarrow$ `EXPIRED` (TTL elapsed without payment)
  * `PENDING_PAYMENT` $\rightarrow$ `CANCELLED` (Buyer explicitly cancelled)
  * Terminal states (`CONFIRMED`, `EXPIRED`, `CANCELLED`) are immutable.

### FR-4: Abandoned Cart Expiry & Automatic Restock
* **FR-4.1:** A background worker runs every $5\text{ seconds}$ querying PostgreSQL for rows where `status = 'PENDING_PAYMENT' AND expires_at <= NOW()` using `FOR UPDATE SKIP LOCKED`.
* **FR-4.2:** For each expired row, the worker transitions the status to `EXPIRED` and executes `release_ticket.lua` in Redis to increment `available_stock` by $1$ and remove `user_id` from the campaign's reserved set.

### FR-5: Payment Webhook Processing
* **FR-5.1:** Expose `/api/v1/webhooks/payments` accepting simulated payment events (`PAYMENT_SUCCEEDED`, `PAYMENT_FAILED`) verified via an `X-Webhook-Signature` HMAC-SHA256 header.
* **FR-5.2:** If `PAYMENT_SUCCEEDED` arrives for an active `PENDING_PAYMENT` reservation, atomically update the reservation status to `CONFIRMED` and decrement the permanent `available_stock` counter in PostgreSQL.
* **FR-5.3:** If a payment webhook arrives *after* the reservation was already marked `EXPIRED`, reject the transition with `HTTP 409 Conflict` and emit a `PaymentRefundRequiredEvent`.

---

## 5. Non-Functional Requirements (NFRs) & SLOs

| Metric / Dimension | Target Service Level Objective (SLO) | Verification Method |
| :--- | :--- | :--- |
| **Inventory Accuracy** | **$0\%$ Overselling & $0\%$ Duplicate User Purchases.** `COUNT(CONFIRMED + PENDING_PAYMENT)` must never exceed `total_stock`. | Automated SQL assertion at the end of every k6 test run. |
| **Peak Throughput** | Sustain $\ge 10,000\text{ requests/sec}$ on a single local Docker host in `REDIS_KAFKA_PIPELINE` mode. | k6 spike test ($10,000$ virtual users firing simultaneously). |
| **Tail Latency** | **p95 $\le 10\text{ ms}$** and **p99 $\le 25\text{ ms}$** for `/reserve` endpoint in `v4` mode. | Prometheus `http_server_requests_seconds` histogram. |
| **DB Connection Safety** | HikariCP pool capped at **maximum $20$ connections** with zero `SQLTransientConnectionException` timeouts under spike load. | Grafana HikariCP connection pool panel. |
| **Consumer Idempotency** | Zero duplicate rows or exceptions when Kafka replays messages under at-least-once delivery. | Testcontainers chaos test restarting Kafka consumer mid-flight. |

---

## 6. Database Schema & Redis Lua Specifications

### 6.1 PostgreSQL Schema (`V1__init_schema.sql`)

```sql
-- 1. Flash Sale Campaigns Table
CREATE TABLE campaigns (
    id UUID PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    total_stock INT NOT NULL CHECK (total_stock > 0),
    available_stock INT NOT NULL CHECK (available_stock >= 0),
    price_cents BIGINT NOT NULL CHECK (price_cents >= 0),
    version BIGINT NOT NULL DEFAULT 0, -- Used for JPA @Version Optimistic Locking (v2)
    sale_starts_at TIMESTAMPTZ NOT NULL,
    sale_ends_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_stock_bounds CHECK (available_stock <= total_stock),
    CONSTRAINT chk_sale_window CHECK (sale_ends_at > sale_starts_at)
);

-- 2. Reservations Table (Order + Idempotency Ledger)
CREATE TABLE reservations (
    id UUID PRIMARY KEY,
    campaign_id UUID NOT NULL REFERENCES campaigns(id),
    user_id UUID NOT NULL,
    idempotency_key VARCHAR(64) NOT NULL UNIQUE,
    status VARCHAR(24) NOT NULL, -- PENDING_PAYMENT, CONFIRMED, EXPIRED, CANCELLED
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_campaign_user UNIQUE (campaign_id, user_id)
);

-- Partial index optimized for the Expiration Polling Worker (SKIP LOCKED)
CREATE INDEX idx_reservations_pending_expiry 
    ON reservations (expires_at) 
    WHERE status = 'PENDING_PAYMENT';
```

### 6.2 Redis Atomic Lua Scripts

#### Script 1: `reserve_ticket.lua`
```lua
-- KEYS[1]: campaign stock key        (e.g., "campaign:{id}:stock")
-- KEYS[2]: campaign reserved users   (e.g., "campaign:{id}:users")
-- KEYS[3]: idempotency key           (e.g., "idemp:{idempotency_key}")
-- KEYS[4]: pending reconciliation    (e.g., "campaign:{id}:pending_sync")
-- ARGV[1]: user_id
-- ARGV[2]: reservation_id
-- ARGV[3]: current_timestamp_ms

-- 1. Check idempotency key first (return existing reservation_id if retry)
local existing_res = redis.call("GET", KEYS[3])
if existing_res then
    return existing_res
end

-- 2. Check if user already holds a reservation for this campaign
if redis.call("SISMEMBER", KEYS[2], ARGV[1]) == 1 then
    return "-1" -- ERR_ALREADY_RESERVED
end

-- 3. Verify available stock
local stock = tonumber(redis.call("GET", KEYS[1]))
if not stock or stock <= 0 then
    return "0" -- ERR_SOLD_OUT
end

-- 4. Atomically decrement stock, register user, store idempotency, and log pending sync
redis.call("DECR", KEYS[1])
redis.call("SADD", KEYS[2], ARGV[1])
redis.call("SET", KEYS[3], ARGV[2], "EX", 3600)
redis.call("HSET", KEYS[4], ARGV[2], ARGV[1] .. ":" .. ARGV[3])

return "1" -- SUCCESS
```

#### Script 2: `release_ticket.lua` (Compensating Restock)
```lua
-- KEYS[1]: campaign stock key        (e.g., "campaign:{id}:stock")
-- KEYS[2]: campaign reserved users   (e.g., "campaign:{id}:users")
-- ARGV[1]: user_id

-- Only increment stock if the user was actually in the reserved set (idempotent release)
local removed = redis.call("SREM", KEYS[2], ARGV[1])
if removed == 1 then
    redis.call("INCR", KEYS[1])
    return 1
end
return 0
```

---

## 7. API Contracts

### 7.1 Reserve Ticket Endpoint
* **Path:** `POST /api/v1/campaigns/{campaignId}/reserve`
* **Headers:**
  * `X-User-Id`: `UUID` (Required)
  * `Idempotency-Key`: `String (max 64 chars)` (Required)
  * `X-Concurrency-Mode`: `NAIVE | OPTIMISTIC_LOCK | PESSIMISTIC_LOCK | REDIS_KAFKA_PIPELINE` (Optional, defaults to `REDIS_KAFKA_PIPELINE`)

**Success Response (`202 Accepted`):**
```json
{
  "reservationId": "0192f3a4-8c1e-7000-8000-4a9b1c2d3e4f",
  "campaignId": "8f9d2a10-4b3c-4e1a-9f0c-123456789abc",
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "QUEUED_FOR_CONFIRMATION",
  "expiresAt": "2026-11-03T18:10:00Z",
  "statusCheckUrl": "/api/v1/reservations/0192f3a4-8c1e-7000-8000-4a9b1c2d3e4f"
}
```

**Error Response Format (`ProblemDetail` RFC 7807):**
* `403 Forbidden`: Sale window is not currently open.
* `409 Conflict`: User has already reserved an item in this campaign.
* `410 Gone`: Campaign inventory is sold out.

---

## 8. 4-Week Engineering Execution Plan

### Sprint 1 (Week 1): Foundation, Schema & Relational Locking (`v1`–`v3`)
* [ ] **T-1.1:** Initialize Spring Boot 3.4 + Java 21 project with Virtual Threads enabled (`spring.threads.virtual.enabled=true`).
* [ ] **T-1.2:** Create `docker-compose.yml` provisioning PostgreSQL 16, Redis 7.2, Apache Kafka (KRaft mode), Prometheus, and Grafana.
* [ ] **T-1.3:** Author Flyway migration `V1__init_schema.sql` and JPA entities.
* [ ] **T-1.4:** Implement `ReservationStrategy` interface and build:
  * `NaiveReservationStrategy` (`v1`)
  * `OptimisticLockReservationStrategy` (`v2` with `@Version` and `@Retryable`)
  * `PessimisticLockReservationStrategy` (`v3` with `SELECT ... FOR UPDATE`)
* [ ] **T-1.5:** Write k6 spike-test script (`5,000 VUs`, `100 stock`) and capture baseline metrics showing overselling in `v1` and database lock contention in `v2`/`v3`.

### Sprint 2 (Week 2): Redis Lua Gatekeeper & Kafka Async Pipeline (`v4`)
* [ ] **T-2.1:** Build Admin Campaign creation and `/warmup` endpoint preloading Redis keys and local Caffeine metadata cache.
* [ ] **T-2.2:** Implement `RedisLuaReservationStrategy` (`v4`) executing `reserve_ticket.lua` via `StringRedisTemplate`.
* [ ] **T-2.3:** Configure Kafka producer (`ticket-reserved-events` topic) and `@KafkaListener` batch consumer using JDBC batch inserts (`hibernate.jdbc.batch_size=50`).
* [ ] **T-2.4:** Verify zero overselling and sub-$20\text{ ms}$ p99 latency under $10,000$ concurrent users in k6.

### Sprint 3 (Week 3): TTL Expiry Worker, Payment Webhooks & Crash Recovery
* [ ] **T-3.1:** Build `ReservationExpiryScheduler` polling expired `PENDING_PAYMENT` rows using `FOR UPDATE SKIP LOCKED`, updating status to `EXPIRED`, and calling `release_ticket.lua`.
* [ ] **T-3.2:** Implement `/api/v1/webhooks/payments` controller with HMAC signature validation and atomic state transitions.
* [ ] **T-3.3:** Build `PendingSyncReconciler` job that inspects `campaign:{id}:pending_sync` hash entries older than $30\text{ seconds}$ (recovering leaked stock if a node crashed between Redis Lua execution and Kafka publish).

### Sprint 4 (Week 4): Observability, Testcontainers & Portfolio Polish
* [ ] **T-4.1:** Write comprehensive integration tests using **Testcontainers** (PostgreSQL, Redis, Kafka) verifying multi-threaded race conditions, idempotency retries, and TTL restocking.
* [ ] **T-4.2:** Expose custom Micrometer metrics (`flashgate.reservations.result`) and bundle a pre-configured Grafana dashboard JSON in Docker Compose.
* [ ] **T-4.3:** Complete `README.md` with system architecture diagrams, Architecture Decision Records (ADRs), and the final `v1`–`v4` benchmark table.

---

## 9. Risk Register & Failure Mode Mitigations

| Failure Scenario | System Impact | Mitigation Strategy in FlashGate |
| :--- | :--- | :--- |
| **App Node Crash After Redis Decrement, Before Kafka Publish** | $1$ unit of stock is decremented in Redis, but no reservation row reaches PostgreSQL ("phantom sold out"). | `reserve_ticket.lua` writes `reservation_id -> user_id:timestamp` into a Redis Hash (`pending_sync`). The Kafka consumer removes the hash entry once persisted to PostgreSQL. A background reconciler scans entries older than $30\text{s}$ and either republishes to Kafka or rolls back Redis stock. |
| **Buyer Cancels Order Before Kafka Consumer Persists Row** | Cancellation request hits PostgreSQL before the async Kafka consumer has inserted the `PENDING_PAYMENT` row. | Cancellation endpoint checks if the row exists in PostgreSQL; if not found, writes a tombstone marker in Redis and publishes a cancellation event keyed by `reservation_id` to the same Kafka partition. |
| **Single Redis Key CPU Saturation ($>100,000\text{ RPS}$)** | All buyers hit the exact same `campaign:{id}:stock` key, saturating a single Redis thread. | Document **Inventory Slot Bucketing** in ADR-003: partition a $1,000\text{-item}$ campaign across $10$ Redis keys (`stock:bucket_0`..`bucket_9` with $100$ items each) routing users via `hash(user_id) % 10`. |

---

## 10. Benchmark Comparison Matrix (Template for `README.md`)

Run the included `k6/flash_sale_spike.js` script against each strategy and record your hardware results below:

| Strategy Mode | Virtual Users | Initial Stock | Total Recorded Sales | Oversold Count | Throughput (RPS) | p95 Latency | p99 Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`v1`: `NAIVE` (JPA Read-Modify-Write)** | $10,000$ | $500$ | *TBD (e.g., 1,380)* | *TBD (+880)* | *TBD* | *TBD* | *TBD* |
| **`v2`: `OPTIMISTIC_LOCK` (`@Version`)** | $10,000$ | $500$ | $500$ | $0$ | *TBD* | *TBD* | *TBD* |
| **`v3`: `PESSIMISTIC_LOCK` (`FOR UPDATE`)** | $10,000$ | $500$ | $500$ | $0$ | *TBD* | *TBD* | *TBD* |
| **`v4`: `REDIS_KAFKA_PIPELINE` (Target)** | $10,000$ | $500$ | $500$ | $0$ | *TBD* | *TBD* | *TBD* |