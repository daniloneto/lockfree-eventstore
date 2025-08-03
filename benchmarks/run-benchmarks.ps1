# Quick benchmark runner scripts
param(
    [string]$Include = "",
    [string]$Competitors = "",
    [string]$Filter = "",
    [switch]$InProcess,
    [switch]$Disruptor,
    [string]$Artifacts = "artifacts/results"
)

Write-Host "LockFree EventStore Benchmarks" -ForegroundColor Green
Write-Host "==============================" -ForegroundColor Green
Write-Host

# Build the project first
Write-Host "Building project..." -ForegroundColor Yellow
$buildFlags = @("-c", "Release")
if ($Disruptor) {
    $buildFlags += "-p:DISRUPTOR=true"
    Write-Host "Building with Disruptor support..." -ForegroundColor Yellow
}

& dotnet build @buildFlags
if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}

# Prepare run arguments
$runArgs = @("-c", "Release", "--")

if ($Include) {
    $runArgs += "--include=$Include"
}

if ($Competitors) {
    $runArgs += "--competitors=$Competitors"
}

if ($Filter) {
    $runArgs += "--filter=$Filter"
}

if ($InProcess) {
    $runArgs += "--inprocess"
}

if ($Disruptor) {
    $runArgs += "--disruptor"
}

$runArgs += "--artifacts=$Artifacts"

# Show what will be executed
Write-Host "Executing: dotnet run $($runArgs -join ' ')" -ForegroundColor Cyan
Write-Host

# Create artifacts directory
New-Item -ItemType Directory -Force -Path $Artifacts | Out-Null

# Run benchmarks
& dotnet run @runArgs

if ($LASTEXITCODE -eq 0) {
    Write-Host
    Write-Host "Benchmarks completed successfully!" -ForegroundColor Green
    Write-Host "Results saved to: $(Resolve-Path $Artifacts)" -ForegroundColor Green
} else {
    Write-Host "Benchmarks failed!" -ForegroundColor Red
    exit 1
}
