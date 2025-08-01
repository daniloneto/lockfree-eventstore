# LockFree.EventStore

Event store em memória, genérico, de baixa latência e lock-free para .NET. Ideal para cenários de monitoramento, métricas e eventos de domínio.

## Principais Recursos
- Escrita MPMC lock-free com descarte FIFO
- Particionamento por chave para alta concorrência
- Snapshots consistentes sem bloquear produtores
- Agregações funcionais e consultas por janela temporal
- Zero dependências externas, pronto para AOT/Trimming

## Exemplo de Uso
```csharp
var store = new EventStore<Order>();
store.TryAppend(new Order { Id = 1, Amount = 10m, Timestamp = DateTime.UtcNow });

var total = store.Aggregate(() => 0m, (acc, e) => acc + e.Amount,
    from: DateTime.UtcNow.AddMinutes(-10));
```

## API
- `TryAppend(event)` — Adiciona evento, lock-free
- `Aggregate` — Agrega valores por janela temporal
- `Snapshot()` — Retorna cópia imutável dos eventos
- `Partitions` — Número de partições para concorrência

## Performance
Projetado para alta concorrência e baixa latência. A ordem global entre partições é aproximada.

## Limitações
- Ordem global apenas aproximada entre partições
- Capacidade fixa; eventos antigos são descartados ao exceder

## Licença
MIT
