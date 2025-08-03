# Quick test runner - simple benchmarks only
param(
    [switch]$List,
    [switch]$Examples
)

if ($List) {
    Write-Host "Running benchmark list..." -ForegroundColor Yellow
    dotnet run -c Release -- --list-benchmarks
    exit 0
}

if ($Examples) {
    Write-Host "Showing examples..." -ForegroundColor Yellow
    dotnet run -c Release -- --help-examples
    exit 0
}

Write-Host "Running quick smoke test..." -ForegroundColor Yellow
Write-Host

# Build first
dotnet build -c Release
if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}

# Run a simple test
dotnet run -c Release -- --include=AppendOnly --competitors=EventStore,ConcurrentQueue --warmup=1 --target=3

Write-Host
Write-Host "Quick test completed!" -ForegroundColor Green
