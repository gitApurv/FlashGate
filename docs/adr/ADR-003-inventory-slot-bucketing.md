# ADR-003: Inventory Partitioning and Slot Bucketing for Hyper-Scale

| Metadata | Details |
| :--- | :--- |
| **Status** | **Accepted (Architectural Blueprint for Hyper-Scale)** |
| **Date** | 2026-11-03 |
| **Deciders** | Senior Java Architect |
| **Context** | Mitigating Single Redis Key Saturation Beyond 100k RPS |

---

## 1. Context and Problem Statement

In the standard single-key Redis architecture, all reservation requests for a campaign target the same Redis key (`campaign:{id}:stock`). 

Although a single Redis 7.2 instance executes basic commands at up to $100,000\text{ RPS}$ on modern hardware, running complex Lua scripts (evaluating sets, hashes, and strings) reduces single-core throughput to approximately $25,000$–$40,000\text{ RPS}$. When concurrent traffic exceeds $100,000\text{ RPS}$, a single Redis key becomes the physical CPU bottleneck, saturating that core and driving up queuing latency.

## 2. Decision

We define an **Inventory Slot Bucketing** architecture to partition campaign stock across multiple independent Redis keys and cluster shards when running beyond $100,000\text{ RPS}$.

### 2.1 Bucketing Mechanics
1. **Inventory Partitioning at Warmup:**
   - Instead of storing stock in a single key `campaign:{id}:stock`, a campaign with $N$ total items is partitioned across $B$ discrete buckets:
     ```
     campaign:{id}:stock:bucket_0  (100 items)
     campaign:{id}:stock:bucket_1  (100 items)
     ...
     campaign:{id}:stock:bucket_9  (100 items)
     ```
   - In a Redis Cluster, hash tags (`campaign:{id}:{bucket_X}:stock`) ensure that different buckets map to different Redis master nodes, distributing CPU load across all CPU cores in the cluster.

2. **Deterministic Sharding by User:**
   - When a reservation request arrives, the application maps the buyer to an initial bucket using consistent hashing:
     $$\text{bucket\_index} = \text{hash}(\text{user\_id}) \pmod B$$
   - The user attempts reservation in their assigned bucket.

3. **Re-Routing & Spacing Strategy:**
   - If the user's assigned bucket reports sold out (`0`), the application attempts an orderly fallback probe to adjacent buckets $(\text{bucket\_index} + 1) \pmod B$ up to a maximum of 2 hops.
   - If all probed buckets report empty or if an authoritative global empty flag is set, the request is definitively rejected as sold out.

## 3. Consequences

### Positive Consequences
- **Horizontal Linear Scalability:** Redis CPU utilization is distributed across $B$ independent CPU cores and cluster nodes. 10 shards can effortlessly process $250,000+$ complex Lua operations/second.
- **Fault Isolation:** Saturation or slowdown on one Redis node does not halt traffic for buyers mapped to other buckets.

### Negative Consequences & Mitigations
- **Non-Uniform Depletion (Bucket Imbalance):** Random hashing may cause Bucket 2 to deplete before Bucket 5, potentially rejecting users while other buckets still hold stock.
  - *Mitigation:* The adjacent bucket fallback probing mechanism allows requests targeting an empty bucket to claim stock from surviving buckets before declaring campaign sellout.
- **Added Script Complexity:** Managing multi-bucket coordination requires disciplined Lua script separation (since Redis Cluster prohibits multi-key transactions across different slot masters).
  - *Mitigation:* For the core benchmark suite, single-key execution is the baseline default; bucketing is activated via configuration (`flashgate.inventory.bucketing.enabled=true`) when scaling beyond 50k RPS.
