Write-Host "=== Teste Simples - Performance LockFree EventStore ===" -ForegroundColor Green

$baseUrl = "http://localhost:5000"
$dashboardPath = ".\samples\MetricsDashboard"

# Funções auxiliares para métricas e medições
function Get-Percentile {
    param(
        [double[]] $sortedValues,
        [double] $p
    )
    if (!$sortedValues -or $sortedValues.Count -eq 0) { return 0 }
    $idx = [int][math]::Ceiling(($p/100.0) * $sortedValues.Count) - 1
    if ($idx -lt 0) { $idx = 0 }
    if ($idx -ge $sortedValues.Count) { $idx = $sortedValues.Count - 1 }
    return $sortedValues[$idx]
}

function Get-Stats {
    param(
        [double[]] $values
    )
    if (!$values -or $values.Count -eq 0) {
        return [pscustomobject]@{ Count=0; Min=0; Max=0; Avg=0; P50=0; P90=0; P95=0 }
    }
    $sorted = $values | Sort-Object
    $avg = ($values | Measure-Object -Average).Average
    $min = ($values | Measure-Object -Minimum).Minimum
    $max = ($values | Measure-Object -Maximum).Maximum
    $p50 = Get-Percentile -sortedValues $sorted -p 50
    $p90 = Get-Percentile -sortedValues $sorted -p 90
    $p95 = Get-Percentile -sortedValues $sorted -p 95
    return [pscustomobject]@{ Count=$values.Count; Min=$min; Max=$max; Avg=$avg; P50=$p50; P90=$p90; P95=$p95 }
}

function Measure-Endpoint {
    param(
        [string] $uri,
        [int] $warmup = 5,
        [int] $iterations = 50
    )
    # Aquecimento
    for ($i=0; $i -lt $warmup; $i++) { Invoke-RestMethod -Uri $uri -Method GET | Out-Null }

    # Medição
    $times = New-Object System.Collections.Generic.List[double]
    for ($i=0; $i -lt $iterations; $i++) {
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        Invoke-RestMethod -Uri $uri -Method GET | Out-Null
        $sw.Stop()
        [void]$times.Add($sw.Elapsed.TotalMilliseconds)
    }
    return Get-Stats -values $times.ToArray()
}

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

# 2. Enviar metricas em lote (UTC e dentro da janela)
Write-Host "`nEnviando metricas..." -ForegroundColor Yellow
$baseTime = (Get-Date).ToUniversalTime()
$eventsToSend = 200
for ($i = 1; $i -le $eventsToSend; $i++) {
    $metric = @{
        label = "test_metric_$($i % 3)"
        value = [math]::Round((Get-Random -Minimum 10 -Maximum 90), 2)
        timestamp = $baseTime.AddSeconds(-$i).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    } | ConvertTo-Json
    Invoke-RestMethod -Uri "$baseUrl/metrics" -Method POST -Body $metric -ContentType "application/json" -TimeoutSec 10 | Out-Null
}
Write-Host "$eventsToSend metricas enviadas!" -ForegroundColor Green

# 3. Testar snapshot views (novo endpoint)
Write-Host "`nTestando snapshot views..." -ForegroundColor Yellow
$snapshotResult = Invoke-RestMethod -Uri "$baseUrl/metrics/snapshot-views" -Method GET
Write-Host "Snapshot obtido - Particoes: $($snapshotResult.partitions)" -ForegroundColor Green
Write-Host "Total de eventos: $($snapshotResult.totalEvents)" -ForegroundColor Green

# 4. Testar agregacao zero-allocation e janela tradicional com mesma janela fixa (UTC)
Write-Host "`nPreparando janela fixa para comparacao..." -ForegroundColor Yellow
$windowFrom = $baseTime.AddMinutes(-5).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$windowTo = $baseTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")

$traditionalUri = "$baseUrl/metrics/window?from=$windowFrom&to=$windowTo"
$optimizedUri = "$baseUrl/metrics/aggregate-zero-alloc?from=$windowFrom&to=$windowTo"

# Verificar coerencia de resultados uma vez
$traditional = Invoke-RestMethod -Uri $traditionalUri -Method GET
$optimized = Invoke-RestMethod -Uri $optimizedUri -Method GET
Write-Host "`nConferencia de resultados (mesma janela):" -ForegroundColor Yellow
Write-Host "  Traditional -> Count: $($traditional.count), Sum: $([math]::Round($traditional.sum,2))" -ForegroundColor Cyan
Write-Host "  Optimized   -> Count: $($optimized.count), Sum: $([math]::Round($optimized.sum,2))" -ForegroundColor Cyan
$sumDiff = [math]::Abs(($traditional.sum) - ($optimized.sum))
if ($traditional.count -eq $optimized.count -and $sumDiff -lt 0.0001) {
    Write-Host "  OK: resultados consistentes" -ForegroundColor Green
} else {
    Write-Host "  ATENCAO: resultados diferem (Count/Sum)" -ForegroundColor Yellow
}

# 5. Medir desempenho com aquecimento e multiplas iteracoes
Write-Host "`nMedindo desempenho (50 iteracoes, 5 warmup)..." -ForegroundColor Yellow
$tradStats = Measure-Endpoint -uri $traditionalUri -warmup 5 -iterations 50
$optStats = Measure-Endpoint -uri $optimizedUri -warmup 5 -iterations 50

Write-Host "`nAgregacao tradicional (/metrics/window):" -ForegroundColor Green
Write-Host "  Avg: $([math]::Round($tradStats.Avg,2)) ms | P50: $([math]::Round($tradStats.P50,2)) ms | P90: $([math]::Round($tradStats.P90,2)) ms | P95: $([math]::Round($tradStats.P95,2)) ms | Min: $([math]::Round($tradStats.Min,2)) ms | Max: $([math]::Round($tradStats.Max,2)) ms" -ForegroundColor Gray

Write-Host "`nAgregacao otimizada (/metrics/aggregate-zero-alloc):" -ForegroundColor Green
Write-Host "  Avg: $([math]::Round($optStats.Avg,2)) ms | P50: $([math]::Round($optStats.P50,2)) ms | P90: $([math]::Round($optStats.P90,2)) ms | P95: $([math]::Round($optStats.P95,2)) ms | Min: $([math]::Round($optStats.Min,2)) ms | Max: $([math]::Round($optStats.Max,2)) ms" -ForegroundColor Gray

if ($optStats.Avg -gt 0) {
    $speedup = [math]::Round(($tradStats.Avg / $optStats.Avg), 2)
    Write-Host "`nSpeedup medio (trad/opt): ${speedup}x" -ForegroundColor Cyan
}

# 6. Limpeza
Write-Host "`nParando servidor..." -ForegroundColor Yellow
if ($process -and !$process.HasExited) {
    $process.Kill()
    Write-Host "Servidor parado." -ForegroundColor Green
}

Write-Host "`n=== Teste Concluido ===" -ForegroundColor Magenta
Write-Host "Performance comparada com sucesso!" -ForegroundColor Green
