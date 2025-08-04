Write-Host "=== Testando MetricsDashboard ===" -ForegroundColor Green

$baseUrl = "http://localhost:5000"
$dashboardPath = ".\samples\MetricsDashboard"

# 1. Subir o projeto MetricsDashboard
Write-Host "0. Iniciando MetricsDashboard..." -ForegroundColor Cyan
Push-Location $dashboardPath
try {
    dotnet build --configuration Release | Out-Null
    $process = Start-Process -FilePath "dotnet" -ArgumentList "run", "--configuration", "Release" -WindowStyle Hidden -PassThru
    Write-Host "Aguardando servidor inicializar..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
    $maxRetries = 10
    $serverReady = $false
    for ($i = 0; $i -lt $maxRetries; $i++) {
        try {
            Invoke-WebRequest -Uri "$baseUrl/health" -Method GET -TimeoutSec 3 -ErrorAction Stop | Out-Null
            $serverReady = $true
            break
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $serverReady) {
        Write-Host "Erro: Servidor não iniciou a tempo" -ForegroundColor Red
        exit 1
    }
} finally {
    Pop-Location
}

# 2. Enviar métricas
Write-Host "`n1. Enviando métricas..." -ForegroundColor Yellow
$baseTime = Get-Date
$metrics = @(
    @{label="cpu_usage"; value=45.5; timestamp=$baseTime.AddMinutes(-10)},
    @{label="memory_usage"; value=78.2; timestamp=$baseTime.AddMinutes(-8)},
    @{label="cpu_usage"; value=52.1; timestamp=$baseTime.AddMinutes(-5)},
    @{label="disk_io"; value=123.4; timestamp=$baseTime.AddMinutes(-3)},
    @{label="cpu_usage"; value=38.7; timestamp=$baseTime.AddMinutes(-1)},
    @{label="memory_usage"; value=82.1; timestamp=$baseTime.AddMinutes(-1)},
    @{label="network_io"; value=456.8; timestamp=$baseTime.AddMinutes(-15)},
    @{label="cpu_usage"; value=41.2; timestamp=$baseTime}
)
foreach ($metric in $metrics) {
    $body = @{
        label = $metric.label
        value = $metric.value
        timestamp = $metric.timestamp.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    } | ConvertTo-Json
    try {
        Invoke-RestMethod -Uri "$baseUrl/metrics" -Method POST -Body $body -ContentType "application/json"
        Write-Host "Enviado: $($metric.label) = $($metric.value) em $($metric.timestamp.ToString('HH:mm:ss'))" -ForegroundColor Green
    } catch {
        Write-Host "Erro: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# 3. Testar agregação por janela temporal
Write-Host "`n2. Testando agregação por janela temporal..." -ForegroundColor Yellow
$windowFrom = $baseTime.AddMinutes(-10).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$windowTo = $baseTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
try {
    $result = Invoke-RestMethod -Uri "$baseUrl/metrics/window?from=$windowFrom`&to=$windowTo" -Method GET
    Write-Host "Janela 10 min - Count: $($result.count), Sum: $($result.sum), Avg: $([math]::Round($result.avg,2)), Min: $($result.min), Max: $($result.max)" -ForegroundColor Green
} catch {
    Write-Host "Erro na agregação por janela: $($_.Exception.Message)" -ForegroundColor Red
}

# 4. Testar soma otimizada por janela
Write-Host "`n3. Testando soma otimizada por janela..." -ForegroundColor Yellow
try {
    $cpuSumWindow = Invoke-RestMethod -Uri "$baseUrl/metrics/sum-window?label=cpu_usage`&minutes=10" -Method GET
    Write-Host "Soma CPU (janela 10 min): $cpuSumWindow" -ForegroundColor Green
} catch {
    Write-Host "Erro na soma por janela: $($_.Exception.Message)" -ForegroundColor Red
}

# 5. Testar soma tradicional
Write-Host "`n4. Testando soma tradicional..." -ForegroundColor Yellow
try {
    $cpuSum = Invoke-RestMethod -Uri "$baseUrl/metrics/sum?label=cpu_usage" -Method GET
    Write-Host "Soma CPU: $cpuSum" -ForegroundColor Green
} catch {
    Write-Host "Erro na soma tradicional: $($_.Exception.Message)" -ForegroundColor Red
}

# 6. Testar top métricas
Write-Host "`n5. Testando top métricas..." -ForegroundColor Yellow
try {
    $top = Invoke-RestMethod -Uri "$baseUrl/metrics/top?k=3" -Method GET
    Write-Host "Top 3:" -ForegroundColor Green
    $top | Format-Table
} catch {
    Write-Host "Erro no top: $($_.Exception.Message)" -ForegroundColor Red
}

# 7. Cleanup
Write-Host "`nTeste concluído!" -ForegroundColor Green
Write-Host "`nParando servidor..." -ForegroundColor Yellow
try {
    if ($process -and !$process.HasExited) {
        $process.Kill()
        Write-Host "Servidor parado." -ForegroundColor Green
    }
} catch {
    Write-Host "Aviso: Não foi possível parar o servidor automaticamente." -ForegroundColor Yellow
    Write-Host "Use Ctrl+C no terminal do servidor se necessário." -ForegroundColor Yellow
}