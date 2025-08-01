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

## Construtores Disponíveis
```csharp
// Construtor padrão
var store = new EventStore<Order>();

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

## API Completa
- `TryAppend(event)` — Adiciona evento, lock-free
- `Aggregate` — Agrega valores por janela temporal com filtros opcionais
- `Snapshot()` — Retorna cópia imutável dos eventos com filtros opcionais
- `CountEvents/Sum/Average/Min/Max` — Agregações especializadas
- `Clear/Reset/Purge` — Métodos de limpeza e manutenção
- `Query` — Consultas flexíveis com filtros e janelas temporais
- `Statistics` — Métricas para monitoramento e observabilidade
- `Count/Capacity/IsEmpty/IsFull/Partitions` — Propriedades de estado

## Performance
Projetado para alta concorrência e baixa latência. A ordem global entre partições é aproximada.

## Limitações
- Ordem global apenas aproximada entre partições
- Capacidade fixa; eventos antigos são descartados ao exceder

## Licença
MIT
