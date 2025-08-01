Write-Host "=== Testando MetricsDashboard ===" -ForegroundColor Green

$baseUrl = "http://localhost:5000"

Write-Host "1. Enviando metricas..." -ForegroundColor Yellow

# Enviar metricas usando Invoke-RestMethod
$metrics = @(
    @{label="cpu_usage"; value=45.5},
    @{label="memory_usage"; value=78.2},
    @{label="cpu_usage"; value=52.1},
    @{label="disk_io"; value=123.4}
)

foreach ($metric in $metrics) {
    $body = @{
        label = $metric.label
        value = $metric.value
        timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    } | ConvertTo-Json
    
    try {
        Invoke-RestMethod -Uri "$baseUrl/metrics" -Method POST -Body $body -ContentType "application/json"
        Write-Host "Enviado: $($metric.label) = $($metric.value)" -ForegroundColor Green
    }
    catch {
        Write-Host "Erro: $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host "`n2. Testando soma..." -ForegroundColor Yellow
try {
    $cpuSum = Invoke-RestMethod -Uri "$baseUrl/metrics/sum?label=cpu_usage" -Method GET
    Write-Host "Soma CPU: $cpuSum" -ForegroundColor Green
}
catch {
    Write-Host "Erro na soma: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "`n3. Testando top metricas..." -ForegroundColor Yellow
try {
    $top = Invoke-RestMethod -Uri "$baseUrl/metrics/top?k=3" -Method GET
    Write-Host "Top 3:" -ForegroundColor Green
    $top | Format-Table
}
catch {
    Write-Host "Erro no top: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "`nTeste concluido!" -ForegroundColor Green
