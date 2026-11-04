import matplotlib.pyplot as plt
import matplotlib.patches as patches

fig, ax = plt.subplots(figsize=(16, 9), facecolor='#090d16')
ax.set_facecolor('#090d16')
ax.set_xlim(0, 16)
ax.set_ylim(0, 9)
ax.axis('off')

# Style Helpers
def draw_box(x, y, w, h, title, subtitle, color, border='#38bdf8', icon=''):
    rect = patches.FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.12,rounding_size=0.2",
                                  facecolor=color, edgecolor=border, linewidth=1.5)
    ax.add_patch(rect)
    ax.text(x + w/2, y + h*0.62, title, color='#ffffff', fontsize=11, fontweight='bold', ha='center', va='center')
    if subtitle:
        ax.text(x + w/2, y + h*0.28, subtitle, color='#cbd5e1', fontsize=8.5, ha='center', va='center')

def draw_arrow(x1, y1, x2, y2, label="", color='#38bdf8', connectionstyle="arc3,rad=0"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="->,head_width=0.4,head_length=0.6", color=color, lw=2,
                                connectionstyle=connectionstyle))
    if label:
        mx, my = (x1 + x2)/2, (y1 + y2)/2
        ax.text(mx, my + 0.18, label, color=color, fontsize=8.5, fontweight='bold', ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.2", facecolor='#090d16', edgecolor='none'))

# Title Header
ax.text(8, 8.5, "⚡ FlashGate Distributed System Architecture", color='#ffffff', fontsize=18, fontweight='heavy', ha='center')
ax.text(8, 8.1, "Java 21 Virtual Threads  •  Redis 7.2 Lua Gatekeeper  •  Kafka KRaft Buffer  •  PostgreSQL 16", color='#94a3b8', fontsize=11, ha='center')

# 1. Clients
draw_box(0.6, 5.2, 2.5, 1.8, "Buyer Clients", "Spike: 50,000+ RPS\n(Mobile & Web Traffic)", "#1e293b", "#64748b")

# 2. Spring Boot Core (Virtual Threads)
draw_box(4.0, 4.7, 3.4, 2.8, "Spring Boot 3.4 Engine", "Java 21 Virtual Threads (Loom)\nNon-blocking OS thread pooling", "#1e1e38", "#818cf8")
# L1 Caffeine Cache inside
draw_box(4.3, 5.0, 2.8, 0.9, "L1: Caffeine Local Cache", "Sale Window Filter (<1µs, 0 hops)", "#312e81", "#a5b4fc")

# 3. Redis Lua Gatekeeper
draw_box(8.5, 4.7, 3.2, 2.8, "L2: Redis 7.2 Gatekeeper", "Single-Threaded RAM (<1ms)\nreserve_ticket.lua", "#3f1a1a", "#ef4444")
draw_box(8.8, 5.0, 2.6, 0.8, "Atomic Lua Core", "SADD user  •  DECRBY stock", "#7f1d1d", "#f87171")

# Drop Excess Traffic arrow
draw_arrow(10.1, 4.7, 10.1, 2.8, "99% Excess Traffic Dropped\n(HTTP 410 Gone / 409 Conflict)", "#f87171")
draw_box(8.6, 1.8, 3.0, 0.9, "Excess Request Filter", "0 DB Connections Saturation", "#1e293b", "#ef4444")

# 4. Kafka KRaft
draw_box(12.7, 5.2, 2.8, 1.8, "Apache Kafka (KRaft)", "Topic: ticket-reserved-events\nAsynchronous Write Buffer", "#142826", "#10b981")

# 5. Kafka Batch Consumer
draw_box(12.7, 1.6, 2.8, 1.8, "Kafka Batch Consumer", "Drain Rate: 50 rows/batch\nDecoupled smooth persistence", "#172554", "#3b82f6")

# 6. PostgreSQL Database
draw_box(4.5, 1.6, 3.0, 1.8, "PostgreSQL 16 DB", "hibernate.jdbc.batch_size=50\nZero row contention during spike", "#1a2332", "#38bdf8")

# Arrows Flow
draw_arrow(3.1, 6.1, 4.0, 6.1, "1. Reserve API", "#38bdf8")
draw_arrow(7.4, 6.1, 8.5, 6.1, "2. Lua Atomic Exec", "#ef4444")
draw_arrow(11.7, 6.1, 12.7, 6.1, "3. Valid Event", "#10b981")
draw_arrow(14.1, 5.2, 14.1, 3.4, "4. Batch Poll", "#10b981")
draw_arrow(12.7, 2.5, 7.5, 2.5, "5. Batch INSERT (Pending)", "#3b82f6")

# Expiration / Reconciliation loop
draw_arrow(5.5, 3.4, 5.5, 4.7, "Dual-Channel Expiry Scheduler\n(SKIP LOCKED every 5s) -> Redis Restock", "#f59e0b", connectionstyle="arc3,rad=0.2")

plt.tight_layout()
out_arch = r"C:\Users\apurv\OneDrive\Desktop\Downloads\FlashGate\docs\architecture\flashgate-architecture.png"
plt.savefig(out_arch, dpi=200, bbox_inches='tight')
print(f"Architecture diagram saved to {out_arch}")
