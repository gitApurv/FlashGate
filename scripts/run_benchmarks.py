import json
import time
import uuid
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess

BASE_URL = "http://localhost:8080"
TOTAL_STOCK = 50
TOTAL_REQUESTS = 250
CONCURRENCY = 25

STRATEGIES = [
    "NAIVE",
    "OPTIMISTIC_LOCK",
    "PESSIMISTIC_LOCK",
    "REDIS_KAFKA_PIPELINE"
]

def make_request(url, method="GET", data=None, headers=None):
    if headers is None:
        headers = {}
    req_data = None
    if data is not None:
        req_data = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    
    req = urllib.request.Request(url, data=req_data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        return e.code, body
    except Exception as e:
        return 0, str(e)

def create_and_warmup_campaign(title):
    now = datetime.now(timezone.utc)
    start_time = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = (now + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload = {
        "title": title,
        "totalStock": TOTAL_STOCK,
        "priceCents": 10000,
        "saleStartsAt": start_time,
        "saleEndsAt": end_time
    }
    
    status, body = make_request(f"{BASE_URL}/api/v1/campaigns", method="POST", data=payload)
    if status != 201:
        raise RuntimeError(f"Failed to create campaign: {status} {body}")
    
    campaign = json.loads(body)
    campaign_id = campaign["id"]
    
    # Warmup
    warmup_status, warmup_body = make_request(f"{BASE_URL}/api/v1/campaigns/{campaign_id}/warmup", method="POST")
    print(f"Created & warmed campaign {campaign_id} (title: '{title}'): status={warmup_status}")
    return campaign_id

def send_reserve_request(campaign_id, strategy, user_id, idempotency_key):
    headers = {
        "X-User-Id": str(user_id),
        "Idempotency-Key": idempotency_key,
        "X-Concurrency-Mode": strategy
    }
    url = f"{BASE_URL}/api/v1/campaigns/{campaign_id}/reserve"
    start_ns = time.perf_counter_ns()
    status, body = make_request(url, method="POST", headers=headers)
    duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
    return status, duration_ms, body

def query_db_state(campaign_id):
    sql = f"""
    SELECT 
        c.total_stock, 
        c.available_stock, 
        COALESCE(r.res_count, 0) as reservation_count
    FROM campaigns c
    LEFT JOIN (
        SELECT campaign_id, count(*) as res_count 
        FROM reservations 
        GROUP BY campaign_id
    ) r ON c.id = r.campaign_id
    WHERE c.id = '{campaign_id}';
    """
    cmd = ["docker", "exec", "flashgate-postgres", "psql", "-U", "flashgate", "-d", "flashgate", "-t", "-A", "-F", ",", "-c", sql]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    line = res.stdout.strip()
    parts = line.split(",")
    total_stock = int(parts[0])
    available_stock = int(parts[1])
    reservation_count = int(parts[2])
    return total_stock, available_stock, reservation_count

def run_benchmark_for_strategy(strategy):
    print(f"\n=======================================================")
    print(f"STARTING BENCHMARK FOR STRATEGY: {strategy}")
    print(f"Total Stock: {TOTAL_STOCK} | Requests: {TOTAL_REQUESTS} | Workers: {CONCURRENCY}")
    print(f"=======================================================")
    
    campaign_id = create_and_warmup_campaign(f"Benchmark Campaign - {strategy}")
    
    # Pre-generate request payloads
    tasks = []
    for i in range(TOTAL_REQUESTS):
        u_id = uuid.uuid4()
        idemp = f"idemp-{strategy}-{i}-{uuid.uuid4().hex[:8]}"
        tasks.append((campaign_id, strategy, u_id, idemp))
    
    results = []
    bench_start = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(send_reserve_request, *t) for t in tasks]
        for f in as_completed(futures):
            results.append(f.result())
            
    bench_duration = time.perf_counter() - bench_start
    
    # For REDIS_KAFKA_PIPELINE, wait a moment for the Kafka consumer batch flush
    if strategy == "REDIS_KAFKA_PIPELINE":
        print("Waiting 2.5s for Kafka micro-batch persistence to settle in PostgreSQL...")
        time.sleep(2.5)
        
    total_stock, available_stock, reservation_count = query_db_state(campaign_id)
    oversold = max(0, reservation_count - total_stock)
    
    # Calculate latency percentiles
    latencies = sorted([r[1] for r in results])
    status_counts = {}
    for r in results:
        status_counts[r[0]] = status_counts.get(r[0], 0) + 1
        
    def percentile(p):
        idx = int(len(latencies) * p / 100)
        return latencies[min(idx, len(latencies)-1)]
    
    p50 = percentile(50)
    p90 = percentile(90)
    p95 = percentile(95)
    p99 = percentile(99)
    rps = len(results) / bench_duration
    
    summary = {
        "strategy": strategy,
        "campaign_id": campaign_id,
        "total_requests": len(results),
        "bench_duration_s": round(bench_duration, 2),
        "throughput_rps": round(rps, 1),
        "status_distribution": status_counts,
        "latency_ms": {
            "min": round(latencies[0], 2),
            "p50": round(p50, 2),
            "p90": round(p90, 2),
            "p95": round(p95, 2),
            "p99": round(p99, 2),
            "max": round(latencies[-1], 2)
        },
        "db_state": {
            "total_stock": total_stock,
            "available_stock": available_stock,
            "reservation_count": reservation_count,
            "oversold_count": oversold
        }
    }
    
    print("\n--- RESULTS FOR " + strategy + " ---")
    print(f"Throughput: {summary['throughput_rps']} RPS in {summary['bench_duration_s']}s")
    print(f"Status codes: {status_counts}")
    print(f"Latency: p50={summary['latency_ms']['p50']}ms, p95={summary['latency_ms']['p95']}ms, p99={summary['latency_ms']['p99']}ms")
    print(f"DB State: total={total_stock}, available_stock_remaining={available_stock}, reservations_in_db={reservation_count}")
    print(f"OVERSOLD COUNT: {oversold}")
    return summary

if __name__ == "__main__":
    all_summaries = []
    for s in STRATEGIES:
        all_summaries.append(run_benchmark_for_strategy(s))
        time.sleep(1) # brief pause between runs
        
    with open("benchmark_results.json", "w") as f:
        json.dump(all_summaries, f, indent=2)
    print("\n\nAll benchmarks completed! Results saved to benchmark_results.json")
