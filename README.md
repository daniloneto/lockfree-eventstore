# LockFree.EventStore

Event store em memória, genérico, de baixa latência e sem locks globais. Adequado para cenários de monitoramento, métricas e eventos de domínio em aplicações .NET.

## Features
- Escrita MPMC sem bloqueios com descarte FIFO
- Particionamento por chave para escalar concorrência
- Snapshots consistentes sem interromper produtores
- Agregações funcionais e consultas por janela temporal
- Zero dependências externas, pronto para AOT/Trimming

## Quickstart
```csharp
var store = new EventStore<Order>();
store.TryAppend(new Order { Id = 1, Amount = 10m, Timestamp = DateTime.UtcNow });

var total = store.Aggregate(() => 0m, (acc, e) => acc + e.Amount,
    from: DateTime.UtcNow.AddMinutes(-10));
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

## Partitions
O número de partições padrão é `Environment.ProcessorCount`. É possível forçar a partição usando `TryAppend(e, partition)`.

## Snapshots
`Snapshot()` retorna uma cópia imutável aproximada do estado atual de todas as partições, ordenada do evento mais antigo para o mais novo por partição.

## Performance
A ordem global entre partições é aproximada. Em cenários de alto throughput isso proporciona baixa latência, mas pode não refletir a ordem real de chegada.

## Limitations
- Ordem global apenas aproximada entre partições
- Capacidade fixa; ao exceder, eventos antigos são descartados

## License
MIT
