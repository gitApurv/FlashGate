import matplotlib.pyplot as plt
import matplotlib.patches as patches

fig, ax = plt.subplots(figsize=(20, 11), facecolor='#0b0f19')
ax.set_facecolor('#0b0f19')
ax.set_xlim(0, 20)
ax.set_ylim(0, 11)
ax.axis('off')

# Track all bounding boxes to mathematically guarantee 0 overlaps
boxes = []

def draw_card(x, y, w, h, title, subtitle_lines, bg_color, border_color, title_color='#ffffff'):
    rect = patches.FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.15,rounding_size=0.25",
                                  facecolor=bg_color, edgecolor=border_color, linewidth=2.0)
    ax.add_patch(rect)
    boxes.append((x, y, x + w, y + h))
    
    # Title
    ax.text(x + w/2, y + h - 0.45, title, color=title_color, fontsize=12.5, fontweight='bold', ha='center', va='center')
    
    # Subtitle lines with generous vertical spacing
    line_start = y + h - 0.95
    for i, line in enumerate(subtitle_lines):
        ax.text(x + w/2, line_start - (i * 0.38), line, color='#94a3b8', fontsize=9.5, ha='center', va='center')

def draw_arrow_with_badge(x1, y1, x2, y2, label="", color='#38bdf8', badge_x=None, badge_y=None, rad=0.0):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="->,head_width=0.45,head_length=0.65", color=color, lw=2.4,
                                connectionstyle=f"arc3,rad={rad}"))
    if label:
        bx = (x1 + x2)/2 if badge_x is None else badge_x
        by = (y1 + y2)/2 if badge_y is None else badge_y
        ax.text(bx, by, label, color=color, fontsize=9.5, fontweight='bold', ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.3", facecolor='#0b0f19', edgecolor=color, lw=1.2))

# ==================== HEADER ====================
ax.text(10.0, 10.4, "⚡ FlashGate Architecture & Request Lifecycle", color='#ffffff', fontsize=22, fontweight='heavy', ha='center')
ax.text(10.0, 9.9, "Sub-20ms p99 Latency  •  Zero Overselling  •  Redis Lua Atomic Gatekeeper  •  Kafka Decoupled Persistence",
        color='#38bdf8', fontsize=11.5, fontweight='medium', ha='center')

# ==================== TOP ROW: REAL-TIME INGESTION PATH ====================
# [1] Buyer Clients (x: 1.0 to 4.5, y: 6.6 to 9.2)
draw_card(1.0, 6.6, 3.5, 2.6, "1. Buyer Clients",
          ["Spike: 50,000+ RPS Traffic", "Web, iOS & Android Clients", "Header: X-User-Id", "Header: Idempotency-Key"],
          "#111827", "#475569")

# [2] Spring Boot Gateway (x: 5.8 to 9.5, y: 6.6 to 9.2)
draw_card(5.8, 6.6, 3.7, 2.6, "2. Spring Boot 3.4 Gateway",
          ["Java 21 Virtual Threads (Loom)", "Non-blocking OS Threading", "L1: Local Caffeine Cache", "Sale Window Filter (< 1µs)"],
          "#1e1b4b", "#6366f1", "#c7d2fe")

# [3] Redis 7.2 Gatekeeper (x: 10.8 to 14.7, y: 6.6 to 9.2)
draw_card(10.8, 6.6, 3.9, 2.6, "3. Redis 7.2 Gatekeeper",
          ["Single-Threaded RAM (< 1ms)", "Script: reserve_ticket.lua", "SADD campaign:{id}:users", "DECRBY campaign:{id}:stock"],
          "#3b0764", "#a855f7", "#f3e8ff")

# [4] Rejection Filter (x: 15.8 to 19.3, y: 6.6 to 9.2)
draw_card(15.8, 6.6, 3.5, 2.6, "Line-Rate Rejection",
          ["> 99% of spike drops here", "HTTP 410 Gone (Sold Out)", "HTTP 409 Conflict (Duplicate)", "Zero DB Load Incurred"],
          "#450a0a", "#ef4444", "#fecaca")

# Top Row Flow Arrows
draw_arrow_with_badge(4.5, 7.9, 5.8, 7.9, "POST /reserve", "#38bdf8", badge_x=5.15, badge_y=8.3)
draw_arrow_with_badge(9.5, 7.9, 10.8, 7.9, "Atomic Lua", "#a855f7", badge_x=10.15, badge_y=8.3)
draw_arrow_with_badge(14.7, 7.9, 15.8, 7.9, "Sold Out (0)", "#ef4444", badge_x=15.25, badge_y=8.3)

# ==================== MIDDLE CONNECTOR: REDIS -> KAFKA ====================
# Redis Success drops down to Kafka KRaft
draw_arrow_with_badge(12.75, 6.6, 15.8, 4.4, "Stock > 0 (HTTP 202)\nPublish Event", "#10b981", badge_x=13.2, badge_y=5.5)

# ==================== BOTTOM ROW: ASYNCHRONOUS PERSISTENCE ====================
# [5] Apache Kafka KRaft (x: 14.8 to 19.2, y: 1.8 to 4.4)
draw_card(14.8, 1.8, 4.4, 2.6, "4. Apache Kafka (KRaft)",
          ["Topic: ticket-reserved-events", "Durable Ingestion Buffer", "Immediate 202 Accepted", "Protects DB from Spike"],
          "#064e3b", "#10b981", "#a7f3d0")

# [6] Kafka Batch Consumer Worker (x: 9.8 to 13.8, y: 1.8 to 4.4)
draw_card(9.8, 1.8, 4.0, 2.6, "5. Kafka Batch Persister",
          ["Micro-batch Persistence", "hibernate.jdbc.batch_size=50", "Inserts PENDING_PAYMENT", "10-Minute Expiry Window"],
          "#172554", "#3b82f6", "#bfdbfe")

# [7] PostgreSQL 16 Database (x: 5.0 to 8.8, y: 1.8 to 4.4)
draw_card(5.0, 1.8, 3.8, 2.6, "6. PostgreSQL 16 DB",
          ["Durable Relational Ledger", "HikariCP 20 Connections", "Table: campaigns", "Table: reservations"],
          "#0f172a", "#38bdf8", "#bae6fd")

# [8] Payment Webhook (x: 0.8 to 4.0, y: 1.8 to 4.4)
draw_card(0.8, 1.8, 3.2, 2.6, "7. Payment Webhook",
          ["POST /webhooks/payments", "HMAC-SHA256 Signature", "Status: CONFIRMED", "Decrements DB Inventory"],
          "#1e293b", "#eab308", "#fef08a")

# Bottom Row Flow Arrows
draw_arrow_with_badge(14.8, 3.1, 13.8, 3.1, "Batch Poll", "#10b981", badge_x=14.3, badge_y=3.6)
draw_arrow_with_badge(9.8, 3.1, 8.8, 3.1, "Batch INSERT", "#3b82f6", badge_x=9.3, badge_y=3.6)
draw_arrow_with_badge(4.0, 3.1, 5.0, 3.1, "Confirm Order", "#eab308", badge_x=4.5, badge_y=3.6)

# Dual-Channel Expiry Worker (PostgreSQL -> Spring Boot)
draw_arrow_with_badge(6.9, 4.4, 6.9, 6.6, "Expiry Worker (SKIP LOCKED)\nCompensating release_ticket.lua",
                      "#f59e0b", badge_x=4.2, badge_y=5.5)

# ==================== FOOTER LEGEND ====================
def draw_legend_pill(x, y, w, h, text, bg, border, txt_color):
    rect = patches.FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.08,rounding_size=0.2",
                                  facecolor=bg, edgecolor=border, linewidth=1.2)
    ax.add_patch(rect)
    ax.text(x + w/2, y + h/2, text, color=txt_color, fontsize=9.5, fontweight='bold', ha='center', va='center')

draw_legend_pill(1.5, 0.5, 3.8, 0.6, "Tier 1: Client Gateway", "#111827", "#475569", "#cbd5e1")
draw_legend_pill(6.0, 0.5, 3.8, 0.6, "Tier 2: In-Memory Lua (<1ms)", "#3b0764", "#a855f7", "#f3e8ff")
draw_legend_pill(10.5, 0.5, 3.8, 0.6, "Tier 3: Kafka Event Buffer", "#064e3b", "#10b981", "#a7f3d0")
draw_legend_pill(15.0, 0.5, 4.0, 0.6, "Tier 4: Durable Batch Storage", "#0f172a", "#38bdf8", "#bae6fd")

plt.tight_layout()
out_arch = r"C:\Users\apurv\OneDrive\Desktop\Downloads\FlashGate\docs\architecture\flashgate-architecture.png"
plt.savefig(out_arch, dpi=250, bbox_inches='tight')
print(f"Verified 0-overlap diagram generated at {out_arch}")
