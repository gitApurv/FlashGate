# FlashGate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build FlashGate, a high-concurrency flash sale & ticketing engine capable of processing 50,000+ concurrent reservation requests with zero overselling, strict single-item-per-user limits, and sub-20ms p99 latency across 4 comparative concurrency strategies.

**Architecture:** Spring Boot 3.4 on Java 21 Virtual Threads implementing a Strategy Pattern for concurrency modes (`v1` Naive, `v2` Optimistic Lock, `v3` Pessimistic Lock, and `v4` Redis Lua Gatekeeper + Kafka Asynchronous Batch Persistence). State machine transitions `PENDING_PAYMENT` to `CONFIRMED` or `EXPIRED` (with `SKIP LOCKED` scheduled polling + compensating Lua restock) and self-heals in-flight crashes via a Redis `pending_sync` reconciler.

**Tech Stack:** Java 21, Spring Boot 3.4.x, PostgreSQL 16, Redis 7.2 (Lettuce + Lua), Apache Kafka (KRaft mode), Flyway, Caffeine Cache, Micrometer/Prometheus, Grafana, Testcontainers, k6.

**Spec:** `flashgate_prd_and_engineering_plan.md`, `docs/architecture/ARCHITECTURE.md`, `docs/api/openapi.yaml`, `docs/adr/ADR-001-virtual-threads-vs-reactive.md`, `docs/adr/ADR-002-redis-lua-gatekeeper-and-kafka-buffering.md`.

## Global Constraints

- Java 21 LTS with Virtual Threads enabled (`spring.threads.virtual.enabled=true`).
- PostgreSQL HikariCP connection pool strictly capped at `maximum-pool-size: 20`.
- Inventory safety: `COUNT(CONFIRMED + PENDING_PAYMENT)` must never exceed `total_stock` under any test profile.
- Zero pinning hazard: No blocking I/O calls inside `synchronized` blocks.
- RFC 7807 `ProblemDetail` responses for all error conditions (403, 404, 409, 410, 500).

## Review Focus

1. **Race Condition in Naive Strategy:** Concurrent requests deplete stock below 0; verify baseline overselling reproduction in `v1`.
2. **Optimistic Locking Retry Exhaustion:** High contention causes `OptimisticLockingFailureException` retry loops to abort without overselling in `v2`.
3. **Pessimistic Lock Queuing & Timeout:** `SELECT ... FOR UPDATE` serializes requests and exhausts HikariCP connections in `v3`.
4. **Node Crash Between Redis Lua Decrement & Kafka Publish:** Stock is decremented in Redis, but no event reaches Kafka; verify `PendingSyncReconciler` heals the orphaned reservation.
5. **Late Payment Webhook on Expired Reservation:** Webhook arrives after 10m TTL expired; verify rejection with 409 Conflict and emission of refund event.

---

### Task 1: Project Initialization & Infrastructure Scaffolding (Sprint 1)

**Files:**
- Create: `pom.xml`
- Create: `docker-compose.yml`
- Create: `src/main/resources/application.yml`
- Create: `src/main/resources/application-test.yml`
- Create: `src/main/java/com/flashgate/FlashGateApplication.java`
- Test: `src/test/java/com/flashgate/FlashGateApplicationTests.java`

**Interfaces:**
- Consumes: Maven build system, Docker Compose environment
- Produces: Running Spring Boot 3.4 application context with Virtual Threads enabled

- [ ] **Step 1: Write test to verify Spring Application context loads with Virtual Threads**

```java
package com.flashgate;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
class FlashGateApplicationTests {
    @Test
    void contextLoads() {}
}
```

- [ ] **Step 2: Run test to verify it fails (no pom or class)**

Run: `mvn test`
Expected: BUILD FAILURE (no pom.xml)

- [ ] **Step 3: Create `pom.xml`, `docker-compose.yml`, `application.yml`, and `FlashGateApplication.java`**

Configure dependencies:
- `spring-boot-starter-web`
- `spring-boot-starter-data-jpa`
- `spring-boot-starter-data-redis`
- `spring-kafka`
- `com.github.ben-manes.caffeine:caffeine`
- `org.flywaydb:flyway-core`, `org.flywaydb:flyway-database-postgresql`
- `org.postgresql:postgresql`
- `org.springframework.retry:spring-retry`, `spring-boot-starter-aop`
- `io.micrometer:micrometer-registry-prometheus`, `spring-boot-starter-actuator`
- `org.testcontainers:testcontainers`, `postgresql`, `kafka`

Configure `docker-compose.yml` with:
- PostgreSQL 16 (port 5432)
- Redis 7.2 (port 6379)
- Apache Kafka KRaft (port 9092)
- Prometheus & Grafana

Enable Virtual Threads in `application.yml`:
```yaml
spring:
  threads:
    virtual:
      enabled: true
  datasource:
    hikari:
      maximum-pool-size: 20
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test`
Expected: BUILD SUCCESS (Context loads cleanly)

- [ ] **Step 5: Commit**

```bash
git add pom.xml docker-compose.yml src/
git commit -m "chore: initialize Spring Boot 3.4 project with Java 21 Virtual Threads and docker-compose"
```

---

### Task 2: Flyway Schema Migration & Domain Entities (Sprint 1)

**Files:**
- Create: `src/main/resources/db/migration/V1__init_schema.sql`
- Create: `src/main/java/com/flashgate/domain/campaign/entity/Campaign.java`
- Create: `src/main/java/com/flashgate/domain/reservation/entity/Reservation.java`
- Create: `src/main/java/com/flashgate/domain/reservation/entity/ReservationStatus.java`
- Create: `src/main/java/com/flashgate/domain/campaign/repository/CampaignRepository.java`
- Create: `src/main/java/com/flashgate/domain/reservation/repository/ReservationRepository.java`
- Test: `src/test/java/com/flashgate/domain/repository/SchemaAndEntityRepositoryTest.java`

**Interfaces:**
- Consumes: PostgreSQL Flyway migrations
- Produces: `CampaignRepository`, `ReservationRepository` with `@Lock(LockModeType.PESSIMISTIC_WRITE)` and `SKIP LOCKED` queries

- [ ] **Step 1: Write integration test for Campaign and Reservation JPA repositories**

```java
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class SchemaAndEntityRepositoryTest {
    @Autowired private CampaignRepository campaignRepository;
    @Autowired private ReservationRepository reservationRepository;

    @Test
    void shouldPersistCampaignAndEnforceCheckConstraints() {
        // Assert campaign persists with positive stock and valid sale window
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `mvn test -Dtest=SchemaAndEntityRepositoryTest`
Expected: FAIL (Classes not found)

- [ ] **Step 3: Implement `V1__init_schema.sql`, JPA entities, and repositories**

- `V1__init_schema.sql`: tables `campaigns` and `reservations` matching Section 6.1 of PRD.
- `Campaign.java`: `@Entity`, `@Id UUID id`, `totalStock`, `availableStock`, `@Version Long version`, `saleStartsAt`, `saleEndsAt`.
- `Reservation.java`: `@Entity`, `@Id UUID id`, `campaignId`, `userId`, `idempotencyKey`, `@Enumerated ReservationStatus status`, `expiresAt`.
- `CampaignRepository`: `findCampaignForUpdate(UUID id)` with `@Lock(LockModeType.PESSIMISTIC_WRITE)`.
- `ReservationRepository`: `findExpiredReservationsForUpdate(Instant now, Pageable pageable)` with native SQL `SELECT ... FOR UPDATE SKIP LOCKED`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=SchemaAndEntityRepositoryTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/resources/db/migration/ src/main/java/com/flashgate/domain/ src/test/
git commit -m "feat: author Flyway schema migration and JPA entities with optimistic and pessimistic locking"
```

---

### Task 3: Concurrency Strategy Framework & Relational Locking Strategies (Sprint 1: v1–v3)

**Files:**
- Create: `src/main/java/com/flashgate/domain/reservation/strategy/ReservationStrategy.java`
- Create: `src/main/java/com/flashgate/domain/reservation/strategy/ReservationStrategyFactory.java`
- Create: `src/main/java/com/flashgate/domain/reservation/strategy/NaiveReservationStrategy.java` (`v1`)
- Create: `src/main/java/com/flashgate/domain/reservation/strategy/OptimisticLockReservationStrategy.java` (`v2`)
- Create: `src/main/java/com/flashgate/domain/reservation/strategy/PessimisticLockReservationStrategy.java` (`v3`)
- Create: `src/main/java/com/flashgate/domain/reservation/controller/ReservationController.java`
- Test: `src/test/java/com/flashgate/domain/reservation/strategy/RelationalStrategiesTest.java`

**Interfaces:**
- Consumes: `CampaignRepository`, `ReservationRepository`
- Produces: `ReservationStrategy` implementations for `NAIVE`, `OPTIMISTIC_LOCK`, and `PESSIMISTIC_LOCK`

- [ ] **Step 1: Write concurrent test demonstrating overselling in `v1` vs consistency in `v3`**

```java
@SpringBootTest
class RelationalStrategiesTest {
    @Test
    void naiveStrategyShouldDemonstrateOversellingUnderConcurrentThreads() {
        // Run 20 concurrent threads attempting to reserve 5 items
    }
    @Test
    void pessimisticStrategyShouldGuaranteeZeroOverselling() {
        // Run 20 concurrent threads attempting to reserve 5 items
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `mvn test -Dtest=RelationalStrategiesTest`
Expected: FAIL

- [ ] **Step 3: Implement strategy interface, factory, and v1–v3 strategies**

- `NaiveReservationStrategy`: read -> check stock -> decrement -> save.
- `OptimisticLockReservationStrategy`: `@Retryable(retryFor = OptimisticLockingFailureException.class, maxAttempts = 3)` with `@Version`.
- `PessimisticLockReservationStrategy`: uses `findCampaignForUpdate` inside `@Transactional`.
- `ReservationController`: accepts `X-Concurrency-Mode` header and dispatches via `ReservationStrategyFactory`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=RelationalStrategiesTest`
Expected: PASS (`v1` demonstrates overselling, `v3` maintains exact inventory bounds)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/flashgate/domain/reservation/ src/test/
git commit -m "feat: implement strategy pattern with v1 Naive, v2 Optimistic, and v3 Pessimistic locking"
```

---

### Task 4: Campaign Administration & Pre-Sale Cache Warmup (Sprint 2)

**Files:**
- Create: `src/main/java/com/flashgate/domain/campaign/service/CampaignService.java`
- Create: `src/main/java/com/flashgate/domain/campaign/service/CampaignCacheService.java`
- Create: `src/main/java/com/flashgate/domain/campaign/controller/CampaignAdminController.java`
- Test: `src/test/java/com/flashgate/domain/campaign/CampaignAdminServiceTest.java`

**Interfaces:**
- Consumes: `CampaignRepository`, Redis `StringRedisTemplate`, Caffeine Cache
- Produces: `POST /api/v1/campaigns`, `POST /api/v1/campaigns/{id}/warmup`

- [ ] **Step 1: Write test for campaign creation and Redis cache warmup**

```java
@SpringBootTest
class CampaignAdminServiceTest {
    @Test
    void shouldWarmupRedisStockAndCaffeineWindowCache() {
        // Verify Redis campaign:{id}:stock equals availableStock and Caffeine holds window
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `mvn test -Dtest=CampaignAdminServiceTest`
Expected: FAIL

- [ ] **Step 3: Implement Campaign service, cache service, and admin endpoints**

- `POST /api/v1/campaigns`: validates `totalStock > 0`, `saleEndsAt > saleStartsAt`.
- `POST /api/v1/campaigns/{id}/warmup`:
  - Sets `campaign:{id}:stock` in Redis with `campaign.getAvailableStock()`.
  - Warms local Caffeine cache with `CampaignCacheDto(startsAt, endsAt)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=CampaignAdminServiceTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/flashgate/domain/campaign/ src/test/
git commit -m "feat: implement campaign creation and pre-sale Redis and Caffeine warmup"
```

---

### Task 5: Redis Lua Atomic Gatekeeper & Kafka Producer/Consumer Pipeline (Sprint 2: v4)

**Files:**
- Create: `src/main/resources/lua/reserve_ticket.lua`
- Create: `src/main/resources/lua/release_ticket.lua`
- Create: `src/main/java/com/flashgate/common/config/RedisConfig.java`
- Create: `src/main/java/com/flashgate/domain/reservation/event/TicketReservedEvent.java`
- Create: `src/main/java/com/flashgate/domain/reservation/event/ReservationKafkaProducer.java`
- Create: `src/main/java/com/flashgate/domain/reservation/event/ReservationKafkaConsumer.java`
- Create: `src/main/java/com/flashgate/domain/reservation/strategy/RedisLuaReservationStrategy.java` (`v4`)
- Test: `src/test/java/com/flashgate/domain/reservation/strategy/RedisLuaReservationStrategyTest.java`

**Interfaces:**
- Consumes: Redis `DefaultRedisScript`, Kafka `KafkaTemplate`, Caffeine cache
- Produces: High-throughput `v4` reservation pipeline returning HTTP 202 Accepted and async batch DB persistence

- [ ] **Step 1: Write integration test for Lua gatekeeper and Kafka batch persistence**

```java
@SpringBootTest
class RedisLuaReservationStrategyTest {
    @Test
    void shouldAtomicallyReserveAndPublishToKafkaWithZeroOverselling() {
        // Send 100 concurrent requests for 10 items; assert exactly 10 succeed, 90 return sold out
    }
    @Test
    void shouldReturnExistingReservationOnIdempotencyRetry() {
        // Retry with same Idempotency-Key; verify same reservationId returned
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `mvn test -Dtest=RedisLuaReservationStrategyTest`
Expected: FAIL

- [ ] **Step 3: Implement Lua scripts, Kafka producer, batch consumer, and RedisLuaReservationStrategy**

- Author `reserve_ticket.lua` and `release_ticket.lua` as specified in Section 6.2 of PRD.
- `RedisLuaReservationStrategy`:
  - Checks Caffeine cache window (throws `SaleClosedException` if closed).
  - Executes Lua script via `redisTemplate.execute(reserveScript, keys, args)`.
  - Translates:
    - `"0"` -> `SoldOutException` (410)
    - `"-1"` -> `AlreadyReservedException` (409)
    - `"1"` -> Generates UUIDv7 `reservationId`, publishes `TicketReservedEvent` to Kafka, returns `HTTP 202 Accepted`.
    - Existing UUID string -> Returns existing reservation (Idempotency hit).
- `ReservationKafkaConsumer`:
  - Listens on `ticket-reserved-events` in batches (`batchListener=true`).
  - Batch saves to PostgreSQL with `status = PENDING_PAYMENT`.
  - Removes processed `reservation_id` from Redis `campaign:{id}:pending_sync` hash.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=RedisLuaReservationStrategyTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/resources/lua/ src/main/java/com/flashgate/ src/test/
git commit -m "feat: implement Redis Lua atomic gatekeeper and Kafka asynchronous batch persistence (v4)"
```

---

### Task 6: TTL Expiry Scheduler, Payment Webhooks & Crash Recovery Reconciler (Sprint 3)

**Files:**
- Create: `src/main/java/com/flashgate/domain/reconciliation/ReservationExpiryScheduler.java`
- Create: `src/main/java/com/flashgate/domain/payment/controller/PaymentWebhookController.java`
- Create: `src/main/java/com/flashgate/domain/payment/service/PaymentWebhookService.java`
- Create: `src/main/java/com/flashgate/domain/reconciliation/PendingSyncReconciler.java`
- Test: `src/test/java/com/flashgate/domain/reconciliation/ReconciliationAndWebhookTest.java`

**Interfaces:**
- Consumes: PostgreSQL `SKIP LOCKED` query, Redis `release_ticket.lua`, HMAC verification
- Produces: Automated abandoned cart restocking, payment confirmation, and crash recovery

- [ ] **Step 1: Write integration tests for TTL expiry restocking, webhook HMAC, and crash recovery**

```java
@SpringBootTest
class ReconciliationAndWebhookTest {
    @Test
    void shouldReclaimExpiredReservationsAndIncrementRedisStock() {}
    @Test
    void shouldConfirmPaymentWithValidHmac() {}
    @Test
    void shouldRejectPaymentForExpiredReservationAndRequireRefund() {}
    @Test
    void pendingSyncReconcilerShouldRecoverOrRollbackOrphanedRedisReservations() {}
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `mvn test -Dtest=ReconciliationAndWebhookTest`
Expected: FAIL

- [ ] **Step 3: Implement Expiry Scheduler, Payment Webhook, and PendingSyncReconciler**

- `ReservationExpiryScheduler`: runs every 5s, selects expired `PENDING_PAYMENT` rows using `FOR UPDATE SKIP LOCKED`, sets `EXPIRED`, executes `release_ticket.lua` in Redis.
- `PaymentWebhookController` & `Service`: verifies `X-Webhook-Signature` using constant-time HMAC-SHA256, transitions `PENDING_PAYMENT` to `CONFIRMED`, decrements DB `available_stock`. Rejects with 409 if status is `EXPIRED`.
- `PendingSyncReconciler`: scans `campaign:{id}:pending_sync` entries > 30s. If DB row exists, cleans Redis hash; if missing, republishes or rolls back Redis stock.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=ReconciliationAndWebhookTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/flashgate/domain/reconciliation/ src/main/java/com/flashgate/domain/payment/ src/test/
git commit -m "feat: implement TTL expiry scheduler, HMAC payment webhook, and pending_sync crash reconciler"
```

---

### Task 7: Micrometer Observability, Grafana Dashboards & k6 Load Tests (Sprint 4)

**Files:**
- Create: `src/main/java/com/flashgate/common/metrics/FlashGateMetrics.java`
- Create: `docker/grafana/dashboards/flashgate-dashboard.json`
- Create: `docker/grafana/provisioning/dashboards/dashboards.yml`
- Create: `docker/prometheus/prometheus.yml`
- Create: `k6/flash_sale_spike.js`
- Test: `src/test/java/com/flashgate/common/metrics/FlashGateMetricsTest.java`

**Interfaces:**
- Consumes: Micrometer `MeterRegistry`
- Produces: Prometheus metrics endpoints (`/actuator/prometheus`), Grafana dashboard, k6 spike-testing suite

- [ ] **Step 1: Write test for custom Micrometer reservation metrics**

```java
@SpringBootTest
class FlashGateMetricsTest {
    @Autowired private FlashGateMetrics metrics;
    @Autowired private MeterRegistry meterRegistry;

    @Test
    void shouldRecordReservationMetricsByStrategyAndResult() {
        metrics.recordReservation("REDIS_KAFKA_PIPELINE", "SUCCESS", Duration.ofMillis(5));
        assertEquals(1, meterRegistry.counter("flashgate.reservations.requests", "strategy", "REDIS_KAFKA_PIPELINE", "result", "SUCCESS").count());
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `mvn test -Dtest=FlashGateMetricsTest`
Expected: FAIL

- [ ] **Step 3: Implement custom metrics, Prometheus config, Grafana JSON, and k6 script**

- `FlashGateMetrics`: counters, timers, and gauges for requests, latency, Redis Lua execution, and Kafka lag.
- `k6/flash_sale_spike.js`: parameterized script sending 10,000 requests against `/reserve` with customizable `X-Concurrency-Mode` (`NAIVE`, `OPTIMISTIC_LOCK`, `PESSIMISTIC_LOCK`, `REDIS_KAFKA_PIPELINE`).
- Pre-configured Grafana dashboard JSON with RPS, latency percentiles, and stock depletion curves.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=FlashGateMetricsTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/flashgate/common/metrics/ docker/ k6/ src/test/
git commit -m "feat: configure Prometheus metrics, Grafana dashboard, and k6 spike load testing suite"
```

---

### Task 8: Testcontainers End-to-End Chaos & Concurrency Verification (Sprint 4)

**Files:**
- Create: `src/test/java/com/flashgate/e2e/FlashGateE2EChaosTest.java`
- Create: `README.md`

**Interfaces:**
- Consumes: PostgreSQL, Redis, Kafka Testcontainers
- Produces: Automated E2E verification proving zero overselling, consumer crash recovery, and full documentation

- [ ] **Step 1: Write comprehensive Testcontainers E2E test suite**

```java
@Testcontainers
@SpringBootTest
class FlashGateE2EChaosTest {
    @Container static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");
    @Container static GenericContainer<?> redis = new GenericContainer<>("redis:7.2-alpine").withExposedPorts(6379);
    @Container static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Test
    void fullFlashSaleLifecycleShouldGuaranteeZeroOversellingUnderHighConcurrency() {}
    @Test
    void consumerRestartMidFlightShouldProcessIdempotently() {}
}
```

- [ ] **Step 2: Run test to verify failure (before full wiring)**

Run: `mvn test -Dtest=FlashGateE2EChaosTest`
Expected: PASS (once all previous tasks are wired)

- [ ] **Step 3: Author comprehensive `README.md`**

Include:
- System overview and Architecture Diagrams.
- Strategy comparison benchmarks (`v1` to `v4`).
- Quickstart guide with Docker Compose and k6 commands.
- Links to ADRs and API specification.

- [ ] **Step 4: Run full verification suite**

Run: `mvn clean verify`
Expected: BUILD SUCCESS (All tests pass)

- [ ] **Step 5: Commit**

```bash
git add src/test/ README.md
git commit -m "docs: add comprehensive README and Testcontainers E2E verification suite"
```
