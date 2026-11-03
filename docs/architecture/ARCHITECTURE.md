# FlashGate — System Architecture & Technical Design Document (TDD)

| Document Details | Value |
| :--- | :--- |
| **System** | **FlashGate** — High-Concurrency Flash Sale & Ticketing Engine |
| **Document Version** | 1.0.0 |
| **Target Runtime** | Java 21 (Virtual Threads), Spring Boot 3.4.x |
| **Data & Messaging** | PostgreSQL 16, Redis 7.2, Apache Kafka (KRaft) |
| **Author** | Senior Java & Distributed Systems Architect |

---

## 1. Architectural Vision & Core Principles

FlashGate is engineered to solve the flash-sale concurrency paradox: **handling massive spikes (50,000+ requests/sec) against severely limited inventory (e.g., 500 tickets) with zero overselling, strict single-item-per-user enforcement, sub-20ms p99 latency, and resilient database protection.**

### Core Tenets
1. **Decouple Gatekeeping from Persistence:** Under extreme concurrency, relational databases fail when acting as real-time gatekeepers due to row locks and connection pool starvation. In FlashGate, Redis Lua scripts act as the atomic, single-threaded in-memory gatekeeper, rejecting >99% of excess traffic in sub-millisecond execution.
2. **Asynchronous Write Buffering:** Relational persistence is moved out of the hot critical path into an asynchronous, batched Kafka consumer pipeline. PostgreSQL receives a smoothed, steady stream of writes.
3. **Thread-per-Task Simplicity with Virtual Threads:** Leveraging Project Loom (Java 21 Virtual Threads) eliminates the complexity of reactive programming (e.g., Project Reactor, WebFlux) while providing the ability to hold tens of thousands of concurrent client connections with negligible memory overhead.
4. **Self-Healing State Reconcilers:** Dual-path recovery ensures consistency across Redis and PostgreSQL:
   - Polling scheduler with `SKIP LOCKED` reclaims unpaid expired inventory after 10 minutes.
   - Redis `pending_sync` reconciler heals edge-case crashes between in-memory decrements and messaging publish.
5. **Multi-Strategy Comparative Benchmark Platform:** Built-in runtime Strategy Pattern enables switching between naive, optimistic, pessimistic, and decoupled Redis-Kafka modes to demonstrate performance, lock contention, and failure mechanics under identical load tests.

---

## 2. High-Level Component & Network Topology

```
                                  ┌──────────────────────────────┐
                                  │      Load Generator (k6)     │
                                  └──────────────┬───────────────┘
                                                 │ 50,000 req/s
                                                 ▼
                                  ┌──────────────────────────────┐
                                  │  Nginx / Reverse Proxy / LB  │
                                  └──────────────┬───────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Spring Boot 3.4 Application Cluster (Java 21 Virtual Threads: spring.threads.virtual.enabled=true) │
│                                                                                                 │
│  ┌───────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ HTTP Ingestion Layer (Tomcat Virtual Threads Worker Pool)                                 │  │
│  │  - Header Validation: X-User-Id, Idempotency-Key, X-Concurrency-Mode                      │  │
│  │  - Caffeine Local Cache: Pre-sale Clock Check (Reject 403 before starts_at / after ends_at) │  │
│  └─────────────────────────────────────────────┬─────────────────────────────────────────────┘  │
│                                                │                                                │
│                                                ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ ReservationStrategyFactory & Routing Engine                                               │  │
│  └───────┬─────────────────────────┬─────────────────────────┬─────────────────────────┬─────┘  │
│          │                         │                         │                         │        │
│          ▼ (Mode: NAIVE)           ▼ (Mode: OPTIMISTIC_LOCK) ▼ (Mode: PESSIMISTIC_LOCK)│        │
│    ┌───────────────┐         ┌───────────────────┐     ┌───────────────────┐           │        │
│    │ NaiveStrategy │         │ OptimisticLock    │     │ PessimisticLock   │           │        │
│    │ (Read/Modify/ │         │ Strategy          │     │ Strategy          │           │        │
│    │  Write JPA)   │         │ (@Version + Retry)│     │ (FOR UPDATE)      │           │        │
│    └───────┬───────┘         └─────────┬─────────┘     └─────────┬─────────┘           │        │
│            │                           │                         │                     │        │
│            └───────────────────────────┼─────────────────────────┘                     │        │
│                                        ▼ Direct DB Writes                              │        │
│                         ┌─────────────────────────────┐                                │        │
│                         │   PostgreSQL 16 Database    │◄──────────────────────────┐    │        │
│                         └─────────────────────────────┘                           │    │        │
│                                                                                   │    │        │
│                                                 ▼ (Mode: REDIS_KAFKA_PIPELINE)    │    │        │
│                              ┌──────────────────────────────────────┐             │    │        │
│                              │ RedisLuaReservationStrategy (Target) │             │    │        │
│                              └──────────────────┬───────────────────┘             │    │        │
│                                                 │                                 │    │        │
└─────────────────────────────────────────────────┼─────────────────────────────────┼────┘        │
                                                  │                                 │             │
                    ┌─────────────────────────────┴──────────┐                      │             │
                    │ 1. Atomic Lua Execution (< 1ms)        │ 2. Return 202 Accepted│             │
                    ▼                                        ▼ to HTTP Client       │             │
       ┌────────────────────────┐              ┌──────────────────────────┐         │             │
       │    Redis 7.2 Engine    │              │ Apache Kafka Cluster     │         │             │
       │                        │              │ Topic:                   │         │             │
       │ - stock counter        │              │  "ticket-reserved-events"│         │             │
       │ - reserved users set   │              └─────────────┬────────────┘         │             │
       │ - idempotency lookup   │                            │                      │             │
       │ - pending_sync hash    │                            │ Stream event         │             │
       └────────────────────────┘                            ▼                      │             │
                   ▲                           ┌──────────────────────────┐         │             │
                   │ Restock via Lua           │ Batch Consumer Worker    │         │             │
                   │                           │ (@KafkaListener batch=50)│─────────┘             │
                   │                           └──────────────────────────┘                       │
                   │                                         ▲                                    │
                   │                                         │ Polls Expired Rows                 │
        ┌──────────┴───────────────┐               ┌─────────┴────────────────────────┐           │
        │ Expiration Worker        │               │ Scheduled Background Reconcilers │           │
        │ (Every 5s, SKIP LOCKED)  │               │ - ReservationExpiryScheduler     │           │
        └──────────────────────────┘               │ - PendingSyncReconciler          │           │
                                                   └──────────────────────────────────┘           │
```

---

## 3. Package Structure & Modular Decomposition

FlashGate follows domain-driven packaging with clean separation between the web boundary, strategy engines, infrastructure integrations, and background reconcilers.

```
com.flashgate
├── FlashGateApplication.java
├── common/
│   ├── config/
│   │   ├── AsyncConfig.java                 // Virtual Threads TaskExecutor configs
│   │   ├── CacheConfig.java                 // Caffeine local in-memory cache definitions
│   │   ├── KafkaConfig.java                 // Kafka Producer, Consumer, and Topic setup
│   │   ├── RedisConfig.java                 // RedisTemplate, ConnectionFactory, Lua Script beans
│   │   └── WebhookSecurityConfig.java       // HMAC-SHA256 signature verifier
│   ├── exception/
│   │   ├── FlashGateException.java          // Base domain exception
│   │   ├── SaleClosedException.java         // 403 Forbidden (Window closed)
│   │   ├── AlreadyReservedException.java    // 409 Conflict (User already holds reservation)
│   │   ├── SoldOutException.java            // 410 Gone (Inventory exhausted)
│   │   ├── InvalidSignatureException.java   // 401 Unauthorized (Webhook HMAC invalid)
│   │   ├── ReservationNotFoundException.java// 404 Not Found
│   │   └── GlobalExceptionHandler.java      // RFC 7807 ProblemDetail translator
│   ├── metrics/
│   │   └── FlashGateMetrics.java            // Micrometer Counter, Gauge, and Timer registry
│   └── util/
│       ├── ClockHolder.java                 // Time-provider interface for deterministic testing
│       └── HmacUtils.java                   // Constant-time HMAC-SHA256 verification
│
├── domain/
│   ├── campaign/
│   │   ├── entity/
│   │   │   └── Campaign.java                // JPA Entity with optimistic versioning
│   │   ├── repository/
│   │   │   └── CampaignRepository.java      // Includes FOR UPDATE and optimistic queries
│   │   ├── service/
│   │   │   ├── CampaignService.java         // Campaign CRUD and Redis cache warmup logic
│   │   │   └── CampaignCacheService.java    // Caffeine local cache provider
│   │   ├── controller/
│   │   │   └── CampaignAdminController.java // POST /campaigns, POST /campaigns/{id}/warmup
│   │   └── dto/
│   │       ├── CreateCampaignRequest.java
│   │       ├── CampaignResponse.java
│   │       └── CampaignCacheDto.java
│   │
│   ├── reservation/
│   │   ├── entity/
│   │   │   ├── Reservation.java             // JPA Entity representing order ledger
│   │   │   └── ReservationStatus.java       // PENDING_PAYMENT, CONFIRMED, EXPIRED, CANCELLED
│   │   ├── repository/
│   │   │   └── ReservationRepository.java   // Custom query for SKIP LOCKED batch expiry
│   │   ├── controller/
│   │   │   └── ReservationController.java   // POST /campaigns/{id}/reserve, GET /reservations/{id}
│   │   ├── dto/
│   │   │   ├── ReservationRequest.java
│   │   │   ├── ReservationResponse.java
│   │   │   └── ConcurrencyMode.java         // NAIVE, OPTIMISTIC_LOCK, PESSIMISTIC_LOCK, REDIS_KAFKA_PIPELINE
│   │   ├── event/
│   │   │   ├── TicketReservedEvent.java     // Kafka event payload
│   │   │   ├── ReservationKafkaProducer.java
│   │   │   └── ReservationKafkaConsumer.java// Batch listener with idempotent persistence
│   │   └── strategy/
│   │       ├── ReservationStrategy.java     // Strategy interface
│   │       ├── ReservationStrategyFactory.java
│   │       ├── NaiveReservationStrategy.java
│   │       ├── OptimisticLockReservationStrategy.java
│   │       ├── PessimisticLockReservationStrategy.java
│   │       └── RedisLuaReservationStrategy.java
│   │
│   ├── payment/
│   │   ├── controller/
│   │   │   └── PaymentWebhookController.java// POST /api/v1/webhooks/payments
│   │   ├── service/
│   │   │   └── PaymentWebhookService.java   // Transitions PENDING_PAYMENT -> CONFIRMED/EXPIRED
│   │   └── dto/
│   │       ├── PaymentWebhookRequest.java
│   │       └── PaymentWebhookResult.java
│   │
│   └── reconciliation/
│       ├── ReservationExpiryScheduler.java  // Runs every 5s, reclaims expired inventory
│       └── PendingSyncReconciler.java       // Heals Redis pending_sync entries > 30s
```

---

## 4. Concurrency Control Strategies & Benchmarking Architecture

FlashGate implements the **Strategy Pattern** to compare distinct concurrency mechanisms under identical load profiles.

```
                  ┌───────────────────────────────┐
                  │    <<ReservationStrategy>>    │
                  ├───────────────────────────────┤
                  │ + reserve(...) : ResResponse  │
                  │ + getMode() : ConcurrencyMode │
                  └───────────────▲───────────────┘
                                  │
      ┌───────────────────┬───────┴───────────┬──────────────────────┐
      │                   │                   │                      │
┌─────┴───────────┐ ┌─────┴───────────┐ ┌─────┴──────────────┐ ┌─────┴──────────────────┐
│  NaiveStrategy  │ │ OptimisticLock  │ │  PessimisticLock   │ │  RedisLuaPipeline      │
│     (`v1`)      │ │ Strategy (`v2`) │ │  Strategy (`v3`)   │ │  Strategy (`v4`)       │
├─────────────────┤ ├─────────────────┤ ├────────────────────┤ ├────────────────────────┤
│ - JPA read      │ │ - @Version col  │ │ - SELECT FOR UPDATE│ │ - Redis Lua script     │
│ - In-memory -1  │ │ - @Retryable    │ │ - Row lock in DB   │ │ - Kafka event publish  │
│ - JPA save      │ │ - Aborts on     │ │ - Connection pool  │ │ - Async batch consumer │
│ - Severe races  │ │   high conflict │ │   bottleneck       │ │ - Sub-20ms p99 SLO     │
└─────────────────┘ └─────────────────┘ └────────────────────┘ └────────────────────────┘
```

### Detailed Breakdown of Strategies:

1. **`v1: NAIVE` (Read-Modify-Write JPA):**
   - Retrieves campaign via `campaignRepository.findById(id)`.
   - Checks `if (campaign.getAvailableStock() > 0)`.
   - Decrements stock `campaign.setAvailableStock(stock - 1)`.
   - Saves campaign and creates new `Reservation`.
   - **Expected Failure:** Lost updates due to uncoordinated concurrent transactions. Overselling occurs immediately under concurrent requests.

2. **`v2: OPTIMISTIC_LOCK` (JPA `@Version` + Exponential Backoff Retry):**
   - Entities have a `@Version private Long version` column.
   - Spring `@Retryable(retryFor = OptimisticLockingFailureException.class, maxAttempts = 3, backoff = @Backoff(delay = 10, multiplier = 2.0))`.
   - Hibernate checks `WHERE id = ? AND version = ?`. If version changed, `OptimisticLockException` is thrown.
   - **Expected Failure:** Zero overselling, but massive CPU churn and catastrophic drop in throughput (>90% transaction aborts) when thousands of requests fight over the same version counter.

3. **`v3: PESSIMISTIC_LOCK` (`SELECT ... FOR UPDATE`):**
   - Method `campaignRepository.findCampaignForUpdate(id)` executes with `@Lock(LockModeType.PESSIMISTIC_WRITE)`.
   - PostgreSQL locks the single campaign row until transaction commit.
   - **Expected Failure:** Zero overselling and guaranteed consistency, but serialized execution causes HikariCP connection pool exhaustion, elevated queuing delays, and p99 latency spikes > 2,000ms.

4. **`v4: REDIS_KAFKA_PIPELINE` (Production Target):**
   - In-memory gatekeeper validates sale window in Caffeine cache.
   - Executes atomic `reserve_ticket.lua` in Redis (<1ms).
   - If stock decremented, publishes `TicketReservedEvent` to Kafka topic `ticket-reserved-events`.
   - Returns `HTTP 202 Accepted` to client immediately.
   - Kafka batch listener inserts `PENDING_PAYMENT` rows in chunks of 50 via JDBC batching.
   - **Expected Performance:** Zero overselling, zero DB lock contention, p99 < 20ms, sustaining 10,000+ RPS on a single node.

---

## 5. Redis Data Structures & Atomic Lua Specification

Redis keys are namespaced by campaign ID to prevent collision and support slot-based clustering.

### 5.1 Redis Key Layout
| Key | Type | Description | TTL |
| :--- | :--- | :--- | :--- |
| `campaign:{id}:stock` | `String (Integer)` | In-memory atomic stock counter. | 24 Hours post-sale |
| `campaign:{id}:users` | `Set<UUID>` | Set of user IDs who reserved or purchased tickets. | 24 Hours post-sale |
| `idemp:{idempotency_key}` | `String (UUID)` | Idempotency lookup returning existing `reservation_id`. | 1 Hour (3600s) |
| `campaign:{id}:pending_sync` | `Hash<UUID, String>` | Field: `reservation_id`, Value: `user_id:timestamp_ms`. | Cleaned by consumer / reconciler |

### 5.2 Atomic Lua Execution Logic (`reserve_ticket.lua`)

```lua
-- KEYS[1]: campaign stock key        ("campaign:{id}:stock")
-- KEYS[2]: campaign reserved users   ("campaign:{id}:users")
-- KEYS[3]: idempotency key           ("idemp:{idempotency_key}")
-- KEYS[4]: pending reconciliation    ("campaign:{id}:pending_sync")
-- ARGV[1]: user_id
-- ARGV[2]: reservation_id
-- ARGV[3]: current_timestamp_ms

-- Step 1: Idempotency Check
local existing_res = redis.call("GET", KEYS[3])
if existing_res then
    return existing_res
end

-- Step 2: One-ticket-per-user Check
if redis.call("SISMEMBER", KEYS[2], ARGV[1]) == 1 then
    return "-1" -- ERR_ALREADY_RESERVED
end

-- Step 3: Stock Availability Check
local stock = tonumber(redis.call("GET", KEYS[1]))
if not stock or stock <= 0 then
    return "0" -- ERR_SOLD_OUT
end

-- Step 4: Atomic State Mutation
redis.call("DECR", KEYS[1])
redis.call("SADD", KEYS[2], ARGV[1])
redis.call("SET", KEYS[3], ARGV[2], "EX", 3600)
redis.call("HSET", KEYS[4], ARGV[2], ARGV[1] .. ":" .. ARGV[3])

return "1" -- SUCCESS
```

### 5.3 Compensating Restock Script (`release_ticket.lua`)

```lua
-- KEYS[1]: campaign stock key        ("campaign:{id}:stock")
-- KEYS[2]: campaign reserved users   ("campaign:{id}:users")
-- ARGV[1]: user_id

-- Remove user; if user was present, increment stock
local removed = redis.call("SREM", KEYS[2], ARGV[1])
if removed == 1 then
    redis.call("INCR", KEYS[1])
    return 1
end
return 0
```

---

## 6. Asynchronous Messaging & Persistence Pipeline (Kafka)

### 6.1 Kafka Topic Configuration
- **Topic Name:** `ticket-reserved-events`
- **Partitions:** 6 (scaled to match consumer thread count)
- **Replication Factor:** 1 (local dev / Docker Compose) or 3 (production)
- **Partition Key:** `campaignId.toString()` (guarantees strict per-campaign ordering if necessary)
- **Message Value:** JSON-serialized `TicketReservedEvent`

### 6.2 Kafka Batch Consumer Design
- **Consumer Group:** `flashgate-reservation-persisters`
- **Ack Mode:** `MANUAL_IMMEDIATE` or `BATCH` (committed only after DB batch insert succeeds)
- **Batch Size:** `hibernate.jdbc.batch_size=50`
- **Persistence Logic:**
  1. Consumer receives `List<ConsumerRecord<String, TicketReservedEvent>>`.
  2. Constructs `Reservation` entities with `status = PENDING_PAYMENT` and `expires_at = event.createdAt + 10 minutes`.
  3. Executes `reservationRepository.saveAll(entities)` within a single `@Transactional` method.
  4. On successful transaction commit:
     - Deletes entries from Redis `campaign:{id}:pending_sync` hash.
     - Acknowledges Kafka message batch offset.
  5. On DB failure: Triggers exponential backoff retry; persistent failures route to Dead Letter Queue (`ticket-reserved-events.DLQ`).

---

## 7. State Machine & Resolution Lifecycles

```
               [Redis Lua Success]
                       │
                       ▼
             (TicketReservedEvent)
                       │
                       ▼
             [ PENDING_PAYMENT ] ── (expires_at = NOW + 10m)
               │             │
               │             │
  Payment      │             │  TTL Elapsed (10 min)
  Webhook      │             │  ReservationExpiryScheduler
               ▼             ▼
       [ CONFIRMED ]    [ EXPIRED ]
                             │
                             └──► release_ticket.lua (+1 stock in Redis)
```

### Transition Integrity Matrix
| Current State | Target State | Trigger | Permitted? | Actions Taken |
| :--- | :--- | :--- | :--- | :--- |
| *None* | `PENDING_PAYMENT` | Kafka consumer insert | **Yes** | Inserts row into PostgreSQL. |
| `PENDING_PAYMENT` | `CONFIRMED` | Webhook: `PAYMENT_SUCCEEDED` | **Yes** | Updates status, decrements permanent DB `available_stock`. |
| `PENDING_PAYMENT` | `EXPIRED` | Expiry Scheduler (10m TTL) | **Yes** | Updates status, calls `release_ticket.lua` (+1 Redis stock). |
| `PENDING_PAYMENT` | `CANCELLED` | User explicit cancel | **Yes** | Updates status, calls `release_ticket.lua` (+1 Redis stock). |
| `EXPIRED` | `CONFIRMED` | Late Webhook | **NO (409 Conflict)** | Emits `PaymentRefundRequiredEvent`. DB status remains `EXPIRED`. |
| `CONFIRMED` | *Any* | Any | **NO** | Terminal state is strictly immutable. |

---

## 8. Database Schema & Flyway Migration Plan

Flyway script: `V1__init_schema.sql`

```sql
-- 1. Flash Sale Campaigns Table
CREATE TABLE campaigns (
    id UUID PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    total_stock INT NOT NULL CHECK (total_stock > 0),
    available_stock INT NOT NULL CHECK (available_stock >= 0),
    price_cents BIGINT NOT NULL CHECK (price_cents >= 0),
    version BIGINT NOT NULL DEFAULT 0,
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
    status VARCHAR(24) NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_campaign_user UNIQUE (campaign_id, user_id)
);

-- Partial index for high-efficiency SKIP LOCKED polling
CREATE INDEX idx_reservations_pending_expiry 
    ON reservations (expires_at) 
    WHERE status = 'PENDING_PAYMENT';
```

---

## 9. Failure Modes & Crash Recovery (Self-Healing Architecture)

### 9.1 Scenario 1: Node Dies Between Redis Lua and Kafka Publish
- **Problem:** Redis stock decremented, user added to set, but no Kafka event sent ("phantom sold out").
- **Solution (`PendingSyncReconciler`):**
  - `reserve_ticket.lua` atomically writes `reservation_id -> user_id:timestamp_ms` to `campaign:{id}:pending_sync`.
  - Every 10 seconds, `PendingSyncReconciler` scans this hash for entries with `timestamp < NOW - 30 seconds`.
  - For each entry:
    - Checks PostgreSQL `reservations` table by `id`.
    - If found: removes entry from hash (sync complete).
    - If NOT found: checks Kafka or republishes `TicketReservedEvent` to Kafka. If retries fail or message is determined dead, executes `release_ticket.lua` to rollback stock.

### 9.2 Scenario 2: Unpaid Reservation Expiry
- **Problem:** User reserves ticket but closes browser without paying. Stock remains locked.
- **Solution (`ReservationExpiryScheduler`):**
  - Runs every 5 seconds.
  - Queries:
    ```sql
    SELECT r.id, r.campaign_id, r.user_id 
    FROM reservations r 
    WHERE r.status = 'PENDING_PAYMENT' 
      AND r.expires_at <= NOW() 
    LIMIT 100 
    FOR UPDATE SKIP LOCKED;
    ```
  - `SKIP LOCKED` prevents lock contention across multiple scheduler instances or app nodes.
  - Updates status to `EXPIRED`.
  - Calls `release_ticket.lua` for each record, atomically returning +1 stock to Redis and allowing other users to reserve.

### 9.3 Scenario 3: Late Payment Webhook on Expired Reservation
- **Problem:** Payment gateway arrives at minute 11, after scheduler marked reservation `EXPIRED`.
- **Solution:**
  - `PaymentWebhookService` checks current status under row lock.
  - Detects `EXPIRED` status.
  - Rejects state transition with `HTTP 409 Conflict`.
  - Emits `PaymentRefundRequiredEvent` to ensure automated refund to buyer.

---

## 10. Performance Tuning & Virtual Threads Safety

1. **Virtual Threads (`spring.threads.virtual.enabled=true`):**
   - Java 21 mounts virtual threads onto carrier threads (`ForkJoinPool`).
   - **Pinning Hazard Mitigation:** Ensure no code executes blocking I/O inside `synchronized` blocks. Replaced with `java.util.concurrent.locks.ReentrantLock` where needed.
2. **HikariCP Pool Sizing:**
   - Capped at `maximum-pool-size: 20`, `minimum-idle: 10`.
   - In `v4`, virtual threads do not touch the DB on `/reserve`. Only the Kafka batch consumer and schedulers touch the database, preventing pool exhaustion under 50k RPS.
3. **Hibernate Batching:**
   ```properties
   spring.jpa.properties.hibernate.jdbc.batch_size=50
   spring.jpa.properties.hibernate.order_inserts=true
   spring.jpa.properties.hibernate.order_updates=true
   ```
4. **Redis Connection Pool:**
   - Lettuce client configured with non-blocking I/O and connection pooling for multi-threaded virtual thread throughput.

---

## 11. Observability, Metrics & Dashboards

### 11.1 Micrometer Metrics
- `flashgate.reservations.requests (counter)`: tagged with `strategy`, `result` (`SUCCESS`, `SOLD_OUT`, `ALREADY_RESERVED`, `CLOSED`, `ERROR`).
- `flashgate.reservations.latency (timer)`: tagged with `strategy`.
- `flashgate.redis.lua.latency (timer)`: Redis Lua script execution duration.
- `flashgate.kafka.publish.latency (timer)`: Kafka send duration.
- `flashgate.stock.available (gauge)`: Periodic gauge reporting Redis `campaign:{id}:stock`.

### 11.2 Dashboards
- Pre-configured Grafana dashboard (`grafana/dashboards/flashgate-dashboard.json`) displaying:
  - Real-time RPS by Strategy.
  - Tail Latency (p50, p95, p99).
  - Inventory Depletion Velocity curve.
  - HikariCP Active vs Idle connections.
  - Kafka Consumer Lag & Processing Rate.
