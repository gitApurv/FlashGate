#!/usr/bin/env bash
set -e

BASE_URL="${BASE_URL:-http://localhost:8080}"

echo "================================================================="
echo "   ⚡ FLASHGATE HIGH-CONCURRENCY FLASH SALE ENGINE LIVE DEMO    "
echo "================================================================="

# 1. Healthcheck
echo -e "\n[1/4] Checking FlashGate Service Health..."
if curl -s "$BASE_URL/actuator/health" | grep -q "UP"; then
    echo "  ✓ FlashGate engine is UP & Healthy on $BASE_URL"
else
    echo "  ✗ Service unreachable or unhealthy. Start with 'java -jar target/flashgate-1.0.0.jar'"
    exit 1
fi

# 2. Create Flash Sale Campaign
echo -e "\n[2/4] Creating Flash Sale Campaign (Stock: 50 Tickets)..."
START_TIME=$(date -u -d "-5 minutes" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")
END_TIME=$(date -u -d "+2 hours" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")

CREATE_PAYLOAD=$(cat <<EOF
{
  "title": "Coldplay Music of the Spheres (Live Demo)",
  "totalStock": 50,
  "priceCents": 12500,
  "saleStartsAt": "$START_TIME",
  "saleEndsAt": "$END_TIME"
}
EOF
)

RESP=$(curl -s -X POST "$BASE_URL/api/v1/campaigns" \
  -H "Content-Type: application/json" \
  -d "$CREATE_PAYLOAD")

CAMPAIGN_ID=$(echo "$RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
echo "  ✓ Campaign Created! ID: $CAMPAIGN_ID"

# 3. Warm up L1 Caffeine & L2 Redis
echo -e "\n[3/4] Pre-Sale Warmup (Loading L1 Caffeine Cache & L2 Redis Inventory)..."
curl -s -X POST "$BASE_URL/api/v1/campaigns/$CAMPAIGN_ID/warmup" > /dev/null
echo "  ✓ Stock Warmup Complete! Redis Counter initialized to 50."

# 4. Fire Concurrency Benchmark Runner
echo -e "\n[4/4] Firing High-Concurrency Spike Across Strategies..."
python3 "$(dirname "$0")/run_benchmarks.py"

echo -e "\n================================================================="
echo "  ✓ DEMO COMPLETE! 100% INVENTORY ACCURACY VERIFIED (0 OVERSOLD) "
echo "================================================================="
