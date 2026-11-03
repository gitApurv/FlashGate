# ADR-001: Adoption of Java 21 Virtual Threads over Reactive Programming (WebFlux)

| Metadata | Details |
| :--- | :--- |
| **Status** | **Accepted** |
| **Date** | 2026-11-03 |
| **Deciders** | Senior Java Architect |
| **Context** | FlashGate Concurrency Model Selection |

---

## 1. Context and Problem Statement

FlashGate must handle over $50,000$ concurrent incoming HTTP connections during a flash-sale spike with low tail latency (sub-$20\text{ ms}$ p99). In traditional platform-thread architectures (e.g., standard Tomcat with a thread pool of 200–500 threads), each concurrent connection consumes a native OS thread requiring $1\text{ MB}$ of stack memory. Spike loads quickly saturate the thread pool, leading to queueing delays, thread exhaustion, and connection drops.

Previously, high-concurrency systems turned to reactive programming (Spring WebFlux, Project Reactor, Netty) to handle thousands of concurrent requests on a handful of event loop threads. However, reactive architectures introduce significant operational penalties:
- Cryptic stack traces that make debugging and production troubleshooting difficult.
- Incompatibility with standard synchronous blocking libraries (e.g., Spring Data JPA, JDBC, legacy SDKs).
- High cognitive overhead and steep learning curve for development teams.
- Complex thread-local propagation (MDC logging, security contexts, trace IDs).

## 2. Decision

We choose **Java 21 Virtual Threads (Project Loom)** running on standard **Spring Boot 3.4+ MVC** (`spring.threads.virtual.enabled=true`).

Each HTTP request is served by a dedicated lightweight virtual thread managed directly by the Java Virtual Machine (JVM). Virtual threads carry a memory footprint of less than $1\text{ KB}$, enabling the JVM to sustain hundreds of thousands of active concurrent tasks on modest hardware without OS thread exhaustion.

## 3. Consequences

### Positive Consequences
- **Sequential Imperative Programming Model:** Developers write clean, idiomatic, readable synchronous Java code (`try/catch`, linear control flow) without reactive chaining (`flatMap`, `Mono`, `Flux`).
- **Full Compatibility with Ecosystem:** Works seamlessly with Spring MVC, Hibernate/JPA, Micrometer, and standard blocking JDBC drivers.
- **Superior Observability:** Stack traces remain straightforward, actionable, and linear. Thread dumps accurately pinpoint where a virtual thread is parked.
- **Resource Efficiency:** Eliminates connection queue timeouts caused by platform thread pool starvation.

### Negative Consequences & Mitigations
- **Carrier Thread Pinning Hazard:** If code performs blocking I/O inside a `synchronized` block or native method, the virtual thread is pinned to its underlying OS carrier thread, preventing other virtual threads from executing.
  - *Mitigation:* We audit the codebase and dependencies to ensure blocking synchronization uses `java.util.concurrent.locks.ReentrantLock` instead of `synchronized` statements. Spring Boot 3.4 and Tomcat 10.1+ have resolved internal pinning issues.
- **Database Connection Pool Exhaustion:** While the JVM can run $50,000$ virtual threads, PostgreSQL cannot accept $50,000$ simultaneous database connections.
  - *Mitigation:* In the primary `REDIS_KAFKA_PIPELINE` strategy (`v4`), virtual threads do not touch the database during the `/reserve` request. Relational database writes are isolated to a strictly bounded Kafka batch consumer worker pool (HikariCP capped at 20 connections).
