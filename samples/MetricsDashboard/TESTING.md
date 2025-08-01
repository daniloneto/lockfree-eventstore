# Como Testar o MetricsDashboard

O MetricsDashboard é uma aplicação de exemplo que demonstra o uso do LockFree.EventStore para coletar e consultar métricas em tempo real.

## 1. Iniciando a Aplicação

```powershell
cd .\samples\MetricsDashboard\
dotnet run
```

A aplicação iniciará em `http://localhost:5000`

## 2. Endpoints Disponíveis

### POST /metrics
Adiciona uma nova métrica ao store.

**Body (JSON):**
```json
{
  "label": "cpu_usage",
  "value": 45.5,
  "timestamp": "2025-01-31T10:00:00.000Z"
}
```

### GET /metrics/sum
Calcula a soma de valores para um label específico.

**Parâmetros:**
- `label` (obrigatório): Nome da métrica
- `from` (opcional): Data/hora inicial (ISO 8601)
- `to` (opcional): Data/hora final (ISO 8601)

**Exemplo:**
```
GET /metrics/sum?label=cpu_usage&from=2025-01-31T09:00:00.000Z
```

### GET /metrics/top
Retorna as top K métricas por valor total.

**Parâmetros:**
- `k` (obrigatório): Número de resultados
- `from` (opcional): Data/hora inicial
- `to` (opcional): Data/hora final

**Exemplo:**
```
GET /metrics/top?k=5&from=2025-01-31T09:00:00.000Z
```

## 3. Testando com PowerShell

Use o script `test-simple.ps1` incluído no projeto:

```powershell
.\test-simple.ps1
```

## 4. Testando Manualmente

### Enviando métricas:
```powershell
$body = @{
    label = "cpu_usage"
    value = 75.5
    timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:5000/metrics" -Method POST -Body $body -ContentType "application/json"
```

### Consultando soma:
```powershell
Invoke-RestMethod -Uri "http://localhost:5000/metrics/sum?label=cpu_usage" -Method GET
```

### Consultando top métricas:
```powershell
Invoke-RestMethod -Uri "http://localhost:5000/metrics/top?k=3" -Method GET
```

## 5. Testando Concorrência

Para testar a capacidade lock-free do sistema, você pode executar múltiplas instâncias do script ou usar ferramentas como `ab` (Apache Bench):

```powershell
# Instalar Apache Bench ou usar curl para múltiplas requisições
for ($i = 1; $i -le 1000; $i++) {
    $body = @{
        label = "load_test"
        value = $i
        timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    } | ConvertTo-Json
    
    Invoke-RestMethod -Uri "http://localhost:5000/metrics" -Method POST -Body $body -ContentType "application/json"
}
```

## 6. Cenários de Teste

1. **Throughput**: Envie milhares de métricas simultaneamente
2. **Consultas temporais**: Teste filtros por data/hora
3. **Agregações**: Verifique somas e tops com grandes volumes
4. **Particionamento**: Use diferentes labels para testar distribuição

O sistema foi projetado para suportar alta concorrência sem locks, ideal para cenários de monitoramento em tempo real.
