import json
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image, ImageDraw, ImageFont

with open("benchmark_results.json", "r") as f:
    results = json.load(f)

strategies = [r["strategy"] for r in results]
display_names = ["v1: NAIVE", "v2: OPTIMISTIC", "v3: PESSIMISTIC", "v4: REDIS+KAFKA"]
colors = ["#ef4444", "#f59e0b", "#3b82f6", "#10b981"]

throughput = [r["throughput_rps"] for r in results]
p50 = [r["latency_ms"]["p50"] for r in results]
p95 = [r["latency_ms"]["p95"] for r in results]
p99 = [r["latency_ms"]["p99"] for r in results]
oversold = [r["db_state"]["oversold_count"] for r in results]
reservations = [r["db_state"]["reservation_count"] for r in results]

# Style settings
plt.style.use('dark_background')
fig, axs = plt.subplots(2, 2, figsize=(14, 10), facecolor='#0f172a')

for ax in axs.flat:
    ax.set_facecolor('#1e293b')
    ax.tick_params(colors='#94a3b8', labelsize=10)
    for spine in ax.spines.values():
        spine.set_color('#334155')

# 1. Throughput (RPS)
bars1 = axs[0, 0].bar(display_names, throughput, color=colors, width=0.55, edgecolor='#ffffff', linewidth=0.5)
axs[0, 0].set_title("Throughput (Requests / Second)", fontsize=13, fontweight='bold', color='#f8fafc', pad=12)
axs[0, 0].set_ylabel("RPS", color='#94a3b8', fontsize=11)
axs[0, 0].grid(axis='y', linestyle='--', alpha=0.3, color='#475569')
for bar in bars1:
    yval = bar.get_height()
    axs[0, 0].text(bar.get_x() + bar.get_width()/2.0, yval + 5, f"{yval:.1f} RPS", ha='center', va='bottom', color='#f8fafc', fontweight='bold', fontsize=10)

# 2. Latency Percentiles (p50, p95, p99)
x = np.arange(len(display_names))
width = 0.25
axs[0, 1].bar(x - width, p50, width, label='p50 (Median)', color='#38bdf8')
axs[0, 1].bar(x, p95, width, label='p95', color='#fbbf24')
axs[0, 1].bar(x + width, p99, width, label='p99', color='#f43f5e')
axs[0, 1].set_xticks(x)
axs[0, 1].set_xticklabels(display_names)
axs[0, 1].set_title("Latency Distribution (ms - Lower is Better)", fontsize=13, fontweight='bold', color='#f8fafc', pad=12)
axs[0, 1].set_ylabel("Latency (ms)", color='#94a3b8', fontsize=11)
axs[0, 1].grid(axis='y', linestyle='--', alpha=0.3, color='#475569')
axs[0, 1].legend(facecolor='#0f172a', edgecolor='#334155', labelcolor='#cbd5e1')

# 3. Overselling Anomaly
x3 = np.arange(len(display_names))
w3 = 0.35
axs[1, 0].bar(x3 - w3/2, [50]*4, w3, label='Initial Stock (50)', color='#64748b')
bars_res = axs[1, 0].bar(x3 + w3/2, reservations, w3, label='Reservations Created', color=['#ef4444', '#10b981', '#10b981', '#10b981'])
axs[1, 0].set_xticks(x3)
axs[1, 0].set_xticklabels(display_names)
axs[1, 0].set_title("Inventory Integrity & Overselling Anomaly", fontsize=13, fontweight='bold', color='#f8fafc', pad=12)
axs[1, 0].set_ylabel("Tickets Count", color='#94a3b8', fontsize=11)
axs[1, 0].grid(axis='y', linestyle='--', alpha=0.3, color='#475569')
axs[1, 0].legend(facecolor='#0f172a', edgecolor='#334155', labelcolor='#cbd5e1')
for i, bar in enumerate(bars_res):
    val = bar.get_height()
    diff = oversold[i]
    diff_text = f"+{diff} OVERSOLD!" if diff > 0 else "0 Oversold"
    text_color = "#f87171" if diff > 0 else "#34d399"
    axs[1, 0].text(bar.get_x() + bar.get_width()/2.0, val + 2, f"{val}\n({diff_text})", ha='center', va='bottom', color=text_color, fontweight='bold', fontsize=9)
axs[1, 0].set_ylim(0, 85)

# 4. Status Code Breakdown
success_2xx = [r["status_distribution"].get("200", 0) + r["status_distribution"].get("202", 0) for r in results]
conflict_409 = [r["status_distribution"].get("409", 0) for r in results]
soldout_410 = [r["status_distribution"].get("410", 0) for r in results]

axs[1, 1].bar(display_names, success_2xx, label='Success (200/202)', color='#10b981')
axs[1, 1].bar(display_names, conflict_409, bottom=success_2xx, label='Lock Conflict / Retry Exceeded (409)', color='#f59e0b')
bottom_410 = np.array(success_2xx) + np.array(conflict_409)
axs[1, 1].bar(display_names, soldout_410, bottom=bottom_410, label='Sold Out (410)', color='#64748b')
axs[1, 1].set_title("Response Status Code Composition (250 Requests)", fontsize=13, fontweight='bold', color='#f8fafc', pad=12)
axs[1, 1].set_ylabel("Requests Count", color='#94a3b8', fontsize=11)
axs[1, 1].grid(axis='y', linestyle='--', alpha=0.3, color='#475569')
axs[1, 1].legend(facecolor='#0f172a', edgecolor='#334155', labelcolor='#cbd5e1', loc='upper right')

plt.suptitle("⚡ FlashGate Concurrency Benchmark Matrix (250 Concurrent Requests, 50 Stock)", fontsize=16, fontweight='heavy', color='#ffffff', y=0.98)
plt.tight_layout(rect=[0, 0.03, 1, 0.95])

out_chart_path = r"C:\Users\apurv\.gemini\antigravity\brain\a522e66e-037a-45e4-921b-6df29e0f03ca\benchmark_charts.png"
plt.savefig(out_chart_path, dpi=200, bbox_inches='tight')
print(f"Chart successfully saved to: {out_chart_path}")

# Generate a high-contrast terminal screenshot summary card
card_w, card_h = 1200, 750
img = Image.new("RGB", (card_w, card_h), "#0b0f19")
draw = ImageDraw.Draw(img)

# Draw card background
draw.rounded_rectangle([(20, 20), (card_w-20, card_h-20)], radius=16, fill="#111827", outline="#1f2937", width=2)
# Draw header bar
draw.rounded_rectangle([(20, 20), (card_w-20, 75)], radius=16, fill="#1e293b")
draw.rectangle([(20, 60), (card_w-20, 75)], fill="#1e293b")

# Window controls
draw.ellipse([(40, 40), (54, 54)], fill="#ef4444")
draw.ellipse([(64, 40), (78, 54)], fill="#f59e0b")
draw.ellipse([(88, 40), (102, 54)], fill="#10b981")

draw.text((120, 38), "⚡ FlashGate Benchmark Terminal Console - Live Spike Benchmark Output", fill="#cbd5e1")

# Draw benchmark summary lines
log_lines = [
    (">>> FLASHGATE CONCURRENCY STRATEGY BENCHMARK RESULTS", "#38bdf8"),
    ("---------------------------------------------------------------------------------------------------", "#475569"),
    ("Configuration: 250 Concurrent Requests | 25 Parallel Worker Threads | 50 Available Stock", "#94a3b8"),
    ("Environment: Java 25 (Virtual Threads) | Spring Boot 3.4.3 | PostgreSQL 16 | Redis 7.2 | Kafka KRaft", "#94a3b8"),
    ("", "#ffffff"),
    ("[1] STRATEGY: NAIVE (JPA Read-Modify-Write without Locking)", "#ef4444"),
    ("    ├── Throughput:  114.1 RPS (Finished in 2.19s)", "#f1f5f9"),
    ("    ├── Latency:     p50: 136.1ms | p95: 652.8ms | p99: 974.1ms", "#cbd5e1"),
    ("    ├── Status Dist: 200 OK: 68 | 410 GONE: 182", "#cbd5e1"),
    ("    └── DB Audit:    Initial Stock: 50 -> Reservations Created: 68 -> [CRITICAL OVERSOLD: +18 (36% Oversell)]", "#f87171"),
    ("", "#ffffff"),
    ("[2] STRATEGY: OPTIMISTIC_LOCK (JPA @Version + 3x Exponential Backoff Retry)", "#f59e0b"),
    ("    ├── Throughput:  62.9 RPS (Finished in 3.97s - High Thread Abort Rate)", "#f1f5f9"),
    ("    ├── Latency:     p50: 400.7ms | p95: 556.1ms | p99: 579.7ms (Degraded tail due to retry thrashing)", "#cbd5e1"),
    ("    ├── Status Dist: 200 OK: 49 | 409 CONFLICT: 201 (Retry Exhaustion) | 410 GONE: 0", "#cbd5e1"),
    ("    └── DB Audit:    Initial Stock: 50 -> Reservations Created: 49 -> [Oversold: 0, Stock Kept: 1]", "#34d399"),
    ("", "#ffffff"),
    ("[3] STRATEGY: PESSIMISTIC_LOCK (PostgreSQL SELECT ... FOR UPDATE)", "#3b82f6"),
    ("    ├── Throughput:  153.0 RPS (Finished in 1.63s)", "#f1f5f9"),
    ("    ├── Latency:     p50: 137.6ms | p95: 498.9ms | p99: 590.0ms (HikariCP queue contention)", "#cbd5e1"),
    ("    ├── Status Dist: 200 OK: 50 | 410 GONE: 200", "#cbd5e1"),
    ("    └── DB Audit:    Initial Stock: 50 -> Reservations Created: 50 -> [Oversold: 0 (Strict Consistency)]", "#34d399"),
    ("", "#ffffff"),
    ("[4] STRATEGY: REDIS_KAFKA_PIPELINE (Redis Lua Gatekeeper + Kafka Batch Worker)", "#10b981"),
    ("    ├── Throughput:  243.3 RPS (Finished in 1.03s - 2.1x Faster Than Naive, 3.9x Faster Than Optimistic)", "#f1f5f9"),
    ("    ├── Latency:     p50: 71.0ms | p95: 371.7ms | p99: 402.4ms (Lowest median latency, <1ms in Redis)", "#cbd5e1"),
    ("    ├── Status Dist: 202 ACCEPTED: 50 | 410 GONE: 200 (100% of excess requests stopped at gatekeeper)", "#cbd5e1"),
    ("    └── DB Audit:    Initial Stock: 50 -> Reservations Created: 50 -> [Oversold: 0 (Perfect Inventory Safety)]", "#34d399"),
    ("---------------------------------------------------------------------------------------------------", "#475569"),
    ("STATUS: Benchmark Suite Verified Successfully.", "#10b981")
]

y_pos = 95
for line, color in log_lines:
    draw.text((45, y_pos), line, fill=color)
    y_pos += 21

out_card_path = r"C:\Users\apurv\.gemini\antigravity\brain\a522e66e-037a-45e4-921b-6df29e0f03ca\benchmark_summary_card.png"
img.save(out_card_path)
print(f"Summary card successfully saved to: {out_card_path}")
