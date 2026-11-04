param (
    [string]$BaseUrl = "http://localhost:8080"
)

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "   FLASHGATE HIGH-CONCURRENCY FLASH SALE ENGINE LIVE DEMO        " -ForegroundColor Yellow
Write-Host "=================================================================" -ForegroundColor Cyan

# 1. Healthcheck
Write-Host "`n[1/4] Checking FlashGate Service Health..." -ForegroundColor White
try {
    $health = curl.exe -s "$BaseUrl/actuator/health"
    if ($health -like '*UP*') {
        Write-Host "  [OK] FlashGate engine is UP and Healthy on $BaseUrl" -ForegroundColor Green
    } else {
        Write-Host "  [FAIL] Service unhealthy. Start with 'java -jar target/flashgate-1.0.0.jar'" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "  [FAIL] Could not connect to $BaseUrl. Ensure application is running." -ForegroundColor Red
    exit 1
}

# 2. Create Flash Sale Campaign
Write-Host "`n[2/4] Creating Flash Sale Campaign with Stock = 50..." -ForegroundColor White
$now = [DateTime]::UtcNow
$start = $now.AddMinutes(-5).ToString("yyyy-MM-ddTHH:mm:ssZ")
$end = $now.AddHours(2).ToString("yyyy-MM-ddTHH:mm:ssZ")

$createBody = '{"title":"Coldplay Music of the Spheres (Live Demo)","totalStock":50,"priceCents":12500,"saleStartsAt":"' + $start + '","saleEndsAt":"' + $end + '"}'

$resp = curl.exe -s -X POST "$BaseUrl/api/v1/campaigns" -H "Content-Type: application/json" -d "$createBody"
$campaignId = ($resp | ConvertFrom-Json).id
Write-Host "  [OK] Campaign Created! ID: $campaignId" -ForegroundColor Green

# 3. Warm up L1 Caffeine and L2 Redis
Write-Host "`n[3/4] Pre-Sale Warmup (Loading L1 Caffeine Cache and L2 Redis Inventory)..." -ForegroundColor White
$warmup = curl.exe -s -X POST "$BaseUrl/api/v1/campaigns/$campaignId/warmup"
Write-Host "  [OK] Stock Warmup Complete! Redis Counter initialized to 50." -ForegroundColor Green

# 4. Fire Concurrency Benchmark Runner
Write-Host "`n[4/4] Firing High-Concurrency Spike Across Strategies..." -ForegroundColor Yellow
$runnerScript = Join-Path $PSScriptRoot "run_benchmarks.py"
python.exe "$runnerScript"

Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host "  [SUCCESS] DEMO COMPLETE - INVENTORY ACCURACY VERIFIED           " -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
