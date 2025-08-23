# <img src="https://raw.githubusercontent.com/daniloneto/lockfree-eventstore/refs/heads/main/lockfreeeventstore.png" />
[![CI](https://github.com/daniloneto/lockfree-eventstore/actions/workflows/ci.yml/badge.svg)](https://github.com/daniloneto/lockfree-eventstore/actions)
[![NuGet](https://img.shields.io/nuget/v/LockFree.EventStore.svg)](https://www.nuget.org/packages/LockFree.EventStore)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=daniloneto_lockfree-eventstore&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=daniloneto_lockfree-eventstore)

# LockFree.EventStore

Um armazenador de eventos em memória, super rápido, **sem travas (lock-free)**, com partições e suporte a consultas por janelas de tempo.

---

## ✨ O que é

Pense em uma fila onde várias pessoas colocam bilhetes (eventos).  
Cada bilhete tem uma **chave** (quem enviou) e um **horário** (quando chegou).

O **LockFree.EventStore** organiza esses bilhetes de forma eficiente e previsível, pronto para cenários de alta velocidade.

---

## 🚀 O que ele faz de especial

- **Guardar eventos muito rápido**  
  Em vez de uma lista única e gigante, divide em **partições por chave**.  
  Isso reduz a concorrência e aumenta a velocidade.

- **Esquecer automaticamente os mais antigos**  
  Cada partição tem **tamanho fixo**.  
  Quando enche, os eventos mais antigos são descartados.  
  Mantém o sistema leve e sempre pronto.

- **Consultas por tempo (janelas)**  
  Perguntas como:  
  - “Quantos eventos chegaram nos últimos 5 segundos?”  
  - “Qual foi o valor máximo nos últimos 10 segundos?”  

  São respondidas rápido graças a **buckets de tempo** que guardam estatísticas (`count`, `sum`, `min`, `max`).

- **Zero lixo de memória (GC-free)**  
  Evita gerar objetos desnecessários para não acionar o coletor de lixo (GC).  

  Técnicas usadas:  
  - Reaproveitamento de arrays (`ArrayPool`)  
  - Uso de blocos de memória (`Span<T>`, `ReadOnlySpan<T>`)  
  - Nada de LINQ/reflection em caminhos críticos  

---

## ⚙️ Como funciona

### ➕ Adicionar evento (append)
1. Descobre a partição correta pela chave.  
2. Insere no próximo espaço livre (ou substitui o mais antigo).  
3. Atualiza os **buckets de tempo** se o recurso de janela estiver ligado.  

### 🔍 Consultar janela (window query)
1. Em vez de ler todos os eventos, pega apenas os **buckets** do intervalo.  
2. Junta as estatísticas e responde quase de imediato.  

---

## 🏆 Por que importa

Em sistemas de **alta velocidade** (bolsa de valores, jogos online, IoT), cada microssegundo conta.  

Essa biblioteca mostra como pensar em **estruturas de dados** e no uso consciente da memória para garantir **desempenho previsível**.

> Princípio: **não guarde mais do que precisa**.  
> Se só importam os últimos X segundos, não faz sentido acumular meses de histórico.

---

## 👉 Essência

O **LockFree.EventStore** é:

**ephemeral & fast — rápido, previsível e focado apenas no que realmente importa.**

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
    TimestampSelector = new PedidoTimestampSelector(),
    // RFC 002: desative tracking quando não precisar de janelas
    EnableWindowTracking = false
});

// API fluente
var store = EventStore.For<Pedido>()
    .WithCapacity(100_000)
    .WithPartitions(8)
    .OnDiscarded(evt => Log(evt))
    .OnCapacityReached(() => NotificarAdmin())
    .WithTimestampSelector(new PedidoTimestampSelector())
    // RFC 002
    .WithEnableWindowTracking(false)
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

Nota: Consultas temporais (from/to) exigem `EnableWindowTracking = true`. Quando desativado, será lançada InvalidOperationException: "Window tracking is disabled. EnableWindowTracking must be true to use window queries."

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

## Limitações
- Ordem global apenas aproximada entre partições
- Capacidade fixa; eventos antigos são descartados ao exceder
- Sem persistência

## Licença
MIT
