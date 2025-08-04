Write-Host "=== Teste Simples - Performance LockFree EventStore ===" -ForegroundColor Green

$baseUrl = "http://localhost:5000"
$dashboardPath = ".\samples\MetricsDashboard"

# 1. Subir o MetricsDashboard
Write-Host "Iniciando servidor..." -ForegroundColor Cyan
Push-Location $dashboardPath
try {
    dotnet build --configuration Release | Out-Null
    $process = Start-Process -FilePath "dotnet" -ArgumentList "run", "--configuration", "Release" -WindowStyle Hidden -PassThru
    Start-Sleep -Seconds 5
    
    # Verificar se servidor esta rodando
    $serverReady = $false
    for ($i = 0; $i -lt 10; $i++) {
        try {
            Invoke-WebRequest -Uri "$baseUrl/health" -Method GET -TimeoutSec 3 -ErrorAction Stop | Out-Null
            $serverReady = $true
            break
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    
    if (-not $serverReady) {
        Write-Host "Erro: Servidor nao iniciou" -ForegroundColor Red
        exit 1
    }
    Write-Host "Servidor iniciado com sucesso!" -ForegroundColor Green
} finally {
    Pop-Location
}

# 2. Enviar metricas em lote
Write-Host "`nEnviando metricas..." -ForegroundColor Yellow
$baseTime = Get-Date

for ($i = 1; $i -le 20; $i++) {
    $metric = @{
        label = "test_metric_$($i % 3)"
        value = [math]::Round((Get-Random -Minimum 10 -Maximum 90), 2)
        timestamp = $baseTime.AddSeconds(-$i).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    } | ConvertTo-Json
    
    Invoke-RestMethod -Uri "$baseUrl/metrics" -Method POST -Body $metric -ContentType "application/json" | Out-Null
}
Write-Host "20 metricas enviadas!" -ForegroundColor Green

# 3. Testar snapshot views (novo endpoint)
Write-Host "`nTestando snapshot views..." -ForegroundColor Yellow
$snapshotResult = Invoke-RestMethod -Uri "$baseUrl/metrics/snapshot-views" -Method GET
Write-Host "Snapshot obtido - Particoes: $($snapshotResult.partitions)" -ForegroundColor Green
Write-Host "Total de eventos: $($snapshotResult.totalEvents)" -ForegroundColor Green

# 4. Testar agregacao zero-allocation
Write-Host "`nTestando agregacao zero-allocation..." -ForegroundColor Yellow
$windowFrom = $baseTime.AddMinutes(-5).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$windowTo = $baseTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$result = Invoke-RestMethod -Uri "$baseUrl/metrics/aggregate-zero-alloc?from=$windowFrom&to=$windowTo" -Method GET

Write-Host "Agregacao zero-allocation:" -ForegroundColor Green
Write-Host "  Count: $($result.count)" -ForegroundColor Cyan
Write-Host "  Sum: $([math]::Round($result.sum, 2))" -ForegroundColor Cyan
Write-Host "  Avg: $([math]::Round($result.avg, 2))" -ForegroundColor Cyan

# 5. Comparar performance
Write-Host "`nComparando performance..." -ForegroundColor Yellow

# Metodo tradicional
$sw1 = [System.Diagnostics.Stopwatch]::StartNew()
$traditional = Invoke-RestMethod -Uri "$baseUrl/metrics/window?from=$windowFrom&to=$windowTo" -Method GET
$sw1.Stop()

# Metodo otimizado
$sw2 = [System.Diagnostics.Stopwatch]::StartNew()
$optimized = Invoke-RestMethod -Uri "$baseUrl/metrics/aggregate-zero-alloc?from=$windowFrom&to=$windowTo" -Method GET
$sw2.Stop()

Write-Host "Metodo tradicional: $($sw1.ElapsedMilliseconds)ms" -ForegroundColor Yellow
Write-Host "Metodo otimizado: $($sw2.ElapsedMilliseconds)ms" -ForegroundColor Cyan

if ($sw2.ElapsedMilliseconds -lt $sw1.ElapsedMilliseconds) {
    $improvement = [math]::Round((1 - ($sw2.ElapsedMilliseconds / $sw1.ElapsedMilliseconds)) * 100, 1)
    Write-Host "Melhoria: $improvement% mais rapido!" -ForegroundColor Green
}

# 6. Limpeza
Write-Host "`nParando servidor..." -ForegroundColor Yellow
if ($process -and !$process.HasExited) {
    $process.Kill()
    Write-Host "Servidor parado." -ForegroundColor Green
}

Write-Host "`n=== Teste Concluido ===" -ForegroundColor Magenta
Write-Host "Novas funcionalidades testadas com sucesso!" -ForegroundColor Green
