# LockFree.EventStore

Event store em memória, genérico, de baixa latência e lock-free para .NET. Ideal para cenários de monitoramento, métricas e eventos de domínio.

## Principais Recursos
- Escrita MPMC lock-free com descarte FIFO
- Particionamento por chave para alta concorrência
- Snapshots consistentes sem bloquear produtores
- Agregações funcionais e consultas por janela temporal
- Zero dependências externas, pronto para AOT/Trimming
- API fluente para configuração avançada
- Métricas e observabilidade integradas
- Agregações especializadas (Sum, Average, Min, Max)

## Exemplo de Uso Básico
```csharp
var store = new EventStore<Order>();
store.TryAppend(new Order { Id = 1, Amount = 10m, Timestamp = DateTime.UtcNow });

var total = store.Aggregate(() => 0m, (acc, e) => acc + e.Amount,
    from: DateTime.UtcNow.AddMinutes(-10));
```

## Novos Construtores
```csharp
// Capacidade explícita
var store = new EventStore<Order>(capacity: 100_000);

// Capacidade e partições
var store = new EventStore<Order>(capacity: 50_000, partitions: 8);

// Configuração avançada
var store = new EventStore<Order>(new EventStoreOptions<Order>
{
    Capacity = 100_000,
    Partitions = 16,
    OnEventDiscarded = evt => Logger.LogTrace("Event discarded: {Event}", evt),
    OnCapacityReached = () => Metrics.IncrementCounter("eventstore.capacity_reached"),
    TimestampSelector = new OrderTimestampSelector()
});

// API fluente
var store = EventStore.For<Order>()
    .WithCapacity(100_000)
    .WithPartitions(8)
    .OnDiscarded(evt => Log(evt))
    .OnCapacityReached(() => NotifyAdmin())
    .WithTimestampSelector(new OrderTimestampSelector())
    .Create();
```

## Propriedades de Estado
```csharp
store.Count          // Número atual de eventos
store.Capacity       // Capacidade máxima configurada
store.IsEmpty        // Se está vazio
store.IsFull         // Se atingiu capacidade máxima
store.Partitions     // Número de partições
```

## Agregações Especializadas
```csharp
// Contagem por janela temporal
var count = store.Count(from: start, to: end);

// Soma de valores
var sum = store.Sum(evt => evt.Amount, from: start, to: end);

// Média
var avg = store.Average(evt => evt.Value, from: start, to: end);

// Mínimo e máximo
var min = store.Min(evt => evt.Score, from: start, to: end);
var max = store.Max(evt => evt.Score, from: start, to: end);

// Com filtros
var filteredSum = store.Sum(
    evt => evt.Amount, 
    filter: evt => evt.Type == "Payment",
    from: start, 
    to: end
);
```

## Snapshots com Filtros
```csharp
// Snapshot filtrado
var recentEvents = store.Snapshot(
    filter: evt => evt.Timestamp > DateTime.UtcNow.AddMinutes(-5)
);

// Snapshot por janela temporal
var snapshot = store.Snapshot(from: start, to: end);

// Snapshot com filtro e janela temporal
var filtered = store.Snapshot(
    filter: evt => evt.Amount > 100,
    from: start,
    to: end
);
```

## Limpeza e Manutenção
```csharp
// Limpar todos os eventos
store.Clear();
store.Reset(); // Alias para Clear()

// Purgar eventos antigos (requer TimestampSelector)
store.Purge(olderThan: DateTime.UtcNow.AddHours(-1));
```

## Métricas e Observabilidade
```csharp
// Estatísticas detalhadas
store.Statistics.TotalAppended        // Total de eventos adicionados
store.Statistics.TotalDiscarded       // Total de eventos descartados
store.Statistics.AppendsPerSecond     // Taxa atual de adições
store.Statistics.LastAppendTime       // Timestamp da última adição
```

## Samples

### MetricsDashboard
API web completa para coleta e consulta de métricas em tempo real:

```bash
cd .\samples\MetricsDashboard\
dotnet run
```

Endpoints disponíveis:
- `POST /metrics` - Adicionar métrica
- `GET /metrics/sum?label=cpu_usage` - Somar valores por label
- `GET /metrics/top?k=5` - Top K métricas

Veja `samples/MetricsDashboard/TESTING.md` para guia completo de testes.

## API Completa
- `TryAppend(event)` — Adiciona evento, lock-free
- `Aggregate` — Agrega valores por janela temporal
- `Snapshot()` — Retorna cópia imutável dos eventos
- `Count/Sum/Average/Min/Max` — Agregações especializadas
- `Clear/Reset/Purge` — Métodos de limpeza
- `Query` — Consultas flexíveis com filtros
- `Statistics` — Métricas para monitoramento

## Partições
O número de partições padrão é `Environment.ProcessorCount`. É possível forçar a partição usando `TryAppend(e, partition)`.

## Snapshots
`Snapshot()` retorna uma cópia imutável aproximada do estado atual de todas as partições, ordenada do evento mais antigo para o mais novo por partição.

## Performance
Projetado para alta concorrência e baixa latência. A ordem global entre partições é aproximada.

## Limitações
- Ordem global apenas aproximada entre partições
- Capacidade fixa; eventos antigos são descartados ao exceder

## Licença
MIT
