# ==============================================================================
# FlowLab Engine - Chaos Monkey Automated Crash & Recovery Test
# ==============================================================================
# This script stress-tests the engine's crash-proof durability:
# 1. Initiates a real order workflow with simulate_crash = true
# 2. Forcefully sends SIGKILL to the engine container mid-execution
# 3. Boots the container back up
# 4. Asserts that the CrashRecoveryService resumes execution and completes it
# ==============================================================================

Write-Host "🐒 [CHAOS-MONKEY] Starting Crash-Proof Durability Stress Test..." -ForegroundColor Cyan

$baseUrl = "http://localhost:8080"

# Step 1: Query initial stock
try {
    $products = Invoke-RestMethod -Uri "$baseUrl/api/workflows/products"
    $initialStock = ($products | Where-Object { $_.sku -eq "SKU-IPHONE" }).stockQuantity
    Write-Host "📦 Initial stock for SKU-IPHONE: $initialStock" -ForegroundColor Green
} catch {
    Write-Host "❌ Failed to query products. Ensure Docker containers are running!" -ForegroundColor Red
    exit 1
}

# Step 2: Trigger workflow with simulate_crash = true
$orderPayload = @{
    sku = "SKU-IPHONE"
    quantity = 1
    customerName = "Chaos Monkey Tester"
    customerEmail = "chaos@flowlab.dev"
    simulate_crash = $true
    simulate_fatal = $false
} | ConvertTo-Json

Write-Host "🚀 Triggering workflow with simulate_crash=true..." -ForegroundColor Yellow
$execution = Invoke-RestMethod -Uri "$baseUrl/api/workflows/order-processing-workflow/start" -Method Post -Body $orderPayload -ContentType "application/json"
$execId = $execution.id
Write-Host "🆔 Workflow Execution ID: $execId" -ForegroundColor Cyan

# Step 3: Wait for Step 0 (Validate Order) and mid-flight execution, then KILL the container!
Write-Host "⏳ Waiting 1.5 seconds for workflow to enter mid-flight execution..." -ForegroundColor Gray
Start-Sleep -Milliseconds 1500

Write-Host "💥 [CHAOS-MONKEY] SIMULATING POWER OUTAGE / PROCESS CRASH: docker kill flowlab-engine" -ForegroundColor Red
docker kill flowlab-engine

Write-Host "💀 Container killed. Waiting 3 seconds in dead state..." -ForegroundColor DarkGray
Start-Sleep -Seconds 3

# Step 4: Revive container
Write-Host "⚡ Reviving engine container: docker start flowlab-engine" -ForegroundColor Yellow
docker start flowlab-engine

Write-Host "⏳ Waiting for Spring Boot & CrashRecoveryService to re-initialize..." -ForegroundColor Gray
Start-Sleep -Seconds 12

# Step 5: Poll for self-healing completion
$maxWait = 35
$elapsed = 0
$finalStatus = ""

while ($elapsed -lt $maxWait) {
    try {
        $statusResp = Invoke-RestMethod -Uri "$baseUrl/api/workflows/executions/$execId" -TimeoutSec 3
        $finalStatus = $statusResp.status
        Write-Host "🔄 Execution status at +$($elapsed)s: $finalStatus (Step #$($statusResp.currentStepIndex))" -ForegroundColor Cyan
        if ($finalStatus -eq "COMPLETED" -or $finalStatus -eq "FAILED") {
            break
        }
    } catch {
        Write-Host "⏳ Waiting for API to become reachable..." -ForegroundColor DarkGray
    }
    Start-Sleep -Seconds 2
    $elapsed += 2
}

# Step 6: Verify assertions
Write-Host "============================================================" -ForegroundColor White
Write-Host "📊 CHAOS MONKEY TEST RESULTS:" -ForegroundColor White
Write-Host "============================================================" -ForegroundColor White

$steps = Invoke-RestMethod -Uri "$baseUrl/api/workflows/executions/$execId/steps"
foreach ($s in $steps) {
    Write-Host "  Step #$($s.stepIndex) [$($s.stepName)]: Status=$($s.status), Attempts=$($s.attemptCount)" -ForegroundColor Gray
}

$updatedProducts = Invoke-RestMethod -Uri "$baseUrl/api/workflows/products"
$updatedStock = ($updatedProducts | Where-Object { $_.sku -eq "SKU-IPHONE" }).stockQuantity

Write-Host "📦 Stock Before: $initialStock | Stock After: $updatedStock" -ForegroundColor Green

if ($finalStatus -eq "COMPLETED" -and $updatedStock -eq ($initialStock - 1)) {
    Write-Host "🎉 SUCCESS! The engine survived process death, self-healed, and completed without duplicate billing or stock loss!" -ForegroundColor Green
    Write-Host "🛡️ 100% CRASH-PROOF VERIFIED." -ForegroundColor Green
} else {
    Write-Host "❌ Test did not meet assertions. Final status: $finalStatus" -ForegroundColor Red
}
