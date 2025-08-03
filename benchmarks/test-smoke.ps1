# Quick test results - just make sure everything works
Write-Host "Running a quick smoke test..." -ForegroundColor Yellow

# Run a very limited benchmark just to verify everything works
$result = & dotnet run -c Release -- --include=AppendOnly --competitors=EventStore --warmup=1 --target=1 --filter="*AppendConcurrent*"

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Smoke test passed!" -ForegroundColor Green
    Write-Host "The benchmark project is ready to use." -ForegroundColor Green
} else {
    Write-Host "❌ Smoke test failed!" -ForegroundColor Red
    Write-Host "Exit code: $LASTEXITCODE" -ForegroundColor Red
}
