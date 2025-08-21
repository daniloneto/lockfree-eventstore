# <img src="https://raw.githubusercontent.com/daniloneto/lockfree-eventstore/refs/heads/main/lockfreeeventstore.png" />
[![CI](https://github.com/daniloneto/lockfree-eventstore/actions/workflows/ci.yml/badge.svg)](https://github.com/daniloneto/lockfree-eventstore/actions)
[![NuGet](https://img.shields.io/nuget/v/LockFree.EventStore.svg)](https://www.nuget.org/packages/LockFree.EventStore)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=daniloneto_lockfree-eventstore&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=daniloneto_lockfree-eventstore)
#
**Um banco de eventos em memória, rodando como serviço, para sincronizar e validar operações entre múltiplas instâncias com alta concorrência e sem travas.**

---

## 🚀 Comece em 3 passos

### 1. Suba o servidor
```bash
docker run --rm -p 7070:7070 daniloneto/lockfree-eventstore:latest
```

### 2. Adicione o cliente
```bash
dotnet add package LockFree.EventStore
```

### 3. Escreva e leia
```csharp
var es = new EventStoreClient("http://localhost:7070");
await es.Append("gateway/orders", new OrderCreated { Id = "o-1", Valor = 123 });
await foreach (var ev in es.Read("gateway/orders", from: 0))
{
    /* tratar evento */
}
```

### 🔁 Sample de Cliente
Veja `samples/ClientSample` para um exemplo que:
- Envia eventos em paralelo para `gateway/orders`
- Lê os eventos de volta
- Calcula agregações locais

Para executar:
```bash
docker run --rm -p 7070:7070 daniloneto/lockfree-eventstore:latest
cd samples/ClientSample
 dotnet run
```

## 🌐 Exemplo com múltiplos Gateways (docker-compose)

Subir 1 EventStore, 3 gateways e Nginx balanceando:
```bash
docker compose up --build
```
Testar envio de pedidos (balanceado entre gateways):
```bash
curl -X POST http://localhost:8080/orders
curl -X POST 'http://localhost:8080/orders/bulk?n=50'
```
Ver estatísticas:
```bash
curl http://localhost:8080/stats/local    # stats de um gateway (um dos 3)
curl http://localhost:8080/stats/global   # consolidação global (via leitura central)
```
---

## 💡 Por que usar
- **Concorrência real:** múltiplos gravadores sem mutex.
- **Integridade garantida:** ordenação consistente, append condicional e idempotência.
- **Operação simples:** sem coordenação externa, sem dependências.

---

## 📌 Cenário típico
Dois (ou mais) gateways atrás de um balanceador de carga precisam registrar operações no mesmo stream.  
O **Lockfree.EventStore** garante ordem e integridade mesmo sob alto paralelismo, sem depender de locks, mantendo todo o estado em memória.

---

## 📚 Documentação completa

A seguir, a documentação técnica completa da API, recursos avançados, benchmarks e exemplos de uso.

## Principais Recursos
- Escrita MPMC lock-free com descarte FIFO
- Particionamento por chave para alta concorrência
- Snapshots consistentes sem bloquear produtores
- Agregações funcionais e consultas por janela temporal
- Zero dependências externas, pronto para AOT/Trimming
- API fluente para configuração avançada
- Métricas e observabilidade integradas
- Agregações especializadas (Soma, Média, Mínimo, Máximo)

## Exemplo de Uso Básico
```csharp
var store = new EventStore<Pedido>();
store.TryAppend(new Pedido { Id = 1, Valor = 10m, Timestamp = DateTime.UtcNow });

var total = store.Aggregate(() => 0m, (acc, e) => acc + e.Valor,
    from: DateTime.UtcNow.AddMinutes(-10));
```

## Novos Construtores
```csharp
// Capacidade explícita
var store = new EventStore<Pedido>(capacidade: 100_000);

// Capacidade e partições
var store = new EventStore<Pedido>(capacidade: 50_000, particoes: 8);

// Configuração avançada
var store = new EventStore<Pedido>(new EventStoreOptions<Pedido>
{
    Capacidade = 100_000,
    Particoes = 16,
    OnEventDiscarded = evt => Logger.LogTrace("Evento descartado: {Event}", evt),
    OnCapacityReached = () => Metrics.IncrementCounter("eventstore.capacidade_atingida"),
    TimestampSelector = new PedidoTimestampSelector()
});

// API fluente
var store = EventStore.For<Pedido>()
    .WithCapacity(100_000)
    .WithPartitions(8)
    .OnDiscarded(evt => Log(evt))
    .OnCapacityReached(() => NotificarAdmin())
    .WithTimestampSelector(new PedidoTimestampSelector())
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
var count = store.Count(from: inicio, to: fim);

// Soma de valores
var sum = store.Sum(evt => evt.Valor, from: inicio, to: fim);

// Média
var avg = store.Average(evt => evt.Valor, from: inicio, to: fim);

// Mínimo e máximo
var min = store.Min(evt => evt.Pontuacao, from: inicio, to: fim);
var max = store.Max(evt => evt.Pontuacao, from: inicio, to: fim);

// Com filtros
var filteredSum = store.Sum(
    evt => evt.Valor, 
    filter: evt => evt.Tipo == "Pagamento",
    from: inicio, 
    to: fim
);
```

## Snapshots com Filtros
```csharp
// Snapshot filtrado
var eventosRecentes = store.Snapshot(
    filter: evt => evt.Timestamp > DateTime.UtcNow.AddMinutes(-5)
);

// Snapshot por janela temporal
var snapshot = store.Snapshot(from: inicio, to: fim);

// Snapshot com filtro e janela temporal
var filtrado = store.Snapshot(
    filter: evt => evt.Valor > 100,
    from: inicio,
    to: fim
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

## Exemplos

### MetricsDashboard
API web completa para coleta e consulta de métricas em tempo real:

```bash
cd .\samples\MetricsDashboarddotnet run
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

---

## Benchmarks de Performance

### Tipos por Valor vs Tipos por Referência

| Operação                  | Tipo Valor    | Tipo Referência | Melhoria   |
|---------------------------|---------------|-----------------|------------|
| Adição de Evento          | 560 ms        | 797 ms          | 42% mais rápido |
| Iteração de Eventos       | 35.8 ns       | 132.5 ns        | 74% mais rápido |
| Consultas de Eventos      | 393.5 ns      | 1,749.1 ns      | 77% mais rápido |

### Structure of Arrays (SoA) vs Array of Structures (AoS)

| Operação                  | SoA           | AoS             | Melhoria   |
|---------------------------|---------------|-----------------|------------|
| Agregação por Chave       | 55.2 ms       | 74.6 ms         | 26% mais rápido |
| Uso de Memória            | Menor         | Maior           | Variável   |

**Conclusões:**
1. Tipos por valor são significativamente mais rápidos que tipos por referência para leitura e escrita.
2. SoA melhora cache locality e reduz pressão de memória.
3. Para alto throughput, a implementação `EventStoreV2` é recomendada.

```csharp
// Usando EventStoreV2 com tipos por valor
var store = new EventStoreV2(capacidade: 1_000_000, particoes: 16);
store.Add("sensor1", 25.5, DateTime.UtcNow.Ticks);
double media = store.Average("sensor1");
```

## Limitações
- Ordem global apenas aproximada entre partições
- Capacidade fixa; eventos antigos são descartados ao exceder

## Licença
MIT
