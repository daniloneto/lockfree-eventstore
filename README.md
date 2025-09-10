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
var store = new EventStore<Pedido>(capacity: 100_000);

// Capacidade e partições
var store = new EventStore<Pedido>(capacity: 50_000, partitions: 8);

// Configuração avançada
var store = new EventStore<Pedido>(new EventStoreOptions<Pedido>
{
    Capacity = 100_000,
    Partitions = 16,
    OnEventDiscarded = evt => Logger.LogTrace("Evento descartado: {Event}", evt),
    OnCapacityReached = () => Metrics.IncrementCounter("eventstore.capacidade_atingida"),
    TimestampSelector = new PedidoTimestampSelector(),
    // RFC 002: desative tracking quando não precisar de janelas
    EnableWindowTracking = false
});

// API fluente
var store = new EventStoreBuilder<Pedido>()
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
cd .\samples\MetricsDashboard
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

---

## Limitações
- Ordem global apenas aproximada entre partições
- Capacidade fixa; eventos antigos são descartados ao exceder

## Persistência opcional por snapshots (RFC005)
A partir da RFC005 o `EventStore<Event>` pode ser configurado para **persistir snapshots por partição** de forma opcional, sem penalizar a latência de append quando o recurso está ocioso.

### Objetivos
- Captura estável não bloqueante de cada partição (tentativas limitadas)
- Gravação atômica em disco (`temp` + `rename`)
- Retry com backoff exponencial e contabilização de falhas
- Pruning determinístico (ordenação por `Version` desc depois `TakenAt` desc)
- Restauração rápida na inicialização (fail‑fast se `ExpectedSchemaVersion` definido)
- Métricas enriquecidas por partição (incluindo `HeadVersion` e `CurrentBuffered`)
- Passo final opcional de snapshot em desligamento gracioso
- Tracing local opcional via `ActivitySource`

### Quando usar
Use quando quiser durabilidade eventual / recuperação rápida em restart, mantendo o hot path de escrita lock‑free em memória. Não substitui um event log completo – é um checkpoint periódico.

### Configuração básica
```csharp
var store = new EventStore<Event>(new EventStoreOptions<Event>
{
    CapacityPerPartition = 1024,
    Partitions = 8
});

var snapshots = store.ConfigureSnapshots(
    new SnapshotOptions
    {
        Enabled = true,
        Interval = TimeSpan.FromSeconds(10),      // gatilho temporal
        MinEventsBetweenSnapshots = 5_000,        // gatilho por volume
        SnapshotsToKeep = 3,                      // retenção por partição
        MaxConcurrentSnapshotJobs = 2,
        MaxPendingSnapshotJobs = 64,
        MaxSaveAttempts = 5,
        BackoffBaseDelay = TimeSpan.FromMilliseconds(100),
        BackoffFactor = 2.0,
        StableCaptureMaxAttempts = 8,
        FinalSnapshotOnShutdown = true,
        FinalSnapshotTimeout = TimeSpan.FromSeconds(5),
        EnableLocalTracing = true // gera Activity "snapshot.save"
    },
    serializer: new BinarySnapshotSerializer(),
    store: new FileSystemSnapshotStore("./snapshots")
);
```

### Execução em background
Você pode usar o hosted service pronto:
```csharp
var hosted = new SnapshotHostedService(snapshots);
await hosted.StartAsync(ct);
// ... aplicação roda ...
await hosted.StopAsync(CancellationToken.None); // dispara passagem final se configurado
```
Ou acionar manualmente:
```csharp
await snapshots.RunAsync(ct); // laço cooperativo respeitando o CancellationToken
```

### Restauração
```csharp
var restored = await store.RestoreFromSnapshotsAsync();
// retorna número de partições restauradas
```
- Se `ExpectedSchemaVersion` estiver definido e houver divergência: lança `InvalidOperationException` (fail‑fast).
- Sem `ExpectedSchemaVersion`, snapshots com `SchemaVersion != 1` são ignorados silenciosamente (modo tolerante legado).

### Atomicidade no FileSystem
`FileSystemSnapshotStore` grava em `*.snap.tmp` e depois `File.Move(temp, final, overwrite:true)` garantindo que snapshots visíveis são sempre completos. Arquivos `.tmp` ou desconhecidos são ignorados na carga e pruning.

### Política de pruning
Mantém os `N` mais recentes segundo ordenação: `Version DESC`, depois `TakenAt DESC`. Isso garante que em caso de versões duplicadas (mesmo valor lógico) fica a mais nova no tempo.

### Métricas
```csharp
if (store.TryGetSnapshotMetrics(out var m))
{
    foreach (var p in m.Partitions)
    {
        Console.WriteLine($"Partição={p.PartitionKey} LastVersion={p.LastVersion} HeadVersion={p.HeadVersion} Buffered={p.CurrentBuffered}");
    }
    Console.WriteLine($"DroppedJobs={m.DroppedJobs} StableCaptureFailures={m.StableCaptureFailures}");
}
```
Campos principais em `PartitionSnapshotInfo`:
- `LastVersion`: versão persistida do último snapshot salvo
- `HeadVersion`: versão lógica viva (aprox.) no momento da coleta de métricas
- `EventsSinceLastSnapshot`: delta de versão entre snapshots salvos
- `CurrentBuffered`: quantidade aproximada de eventos atualmente no buffer da partição
- `StableCaptureFailedCount`: quantas vezes a captura estável falhou (contensão)

Invariantes esperadas:
- `HeadVersion >= LastVersion`
- `0 <= CurrentBuffered <= CapacityPerPartition`

### Tracing
Quando `EnableLocalTracing = true` é ativado, cada persistência gera uma Activity (`snapshot.save`) com tags:
- `partition`
- `version`
- `bytes`
- `attempts`
- `outcome` (`success` | `failure` | `stable-capture-failed`)
- `error` (quando falha definitiva)
E dentro do mesmo contexto é emitido um evento `snapshot.prune` (quando pruning ocorre) com tags:
- `prune.partition`
- `prune.deleted`
- `prune.kept`
Falhas de captura estável incrementam métricas (`StableCaptureFailures`). Futuras extensões podem adicionar spans adicionais.

### Performance & Overhead
- Nenhuma degradação perceptível no hot path quando o recurso está habilitado mas ocioso (meta: regressão < 2% de latência de append p50/p99). Teste de regressão incluso.
- Captura estável tenta até `StableCaptureMaxAttempts`; se falhar, contabiliza e reprograma (evita backpressure a produtores).
- Trabalho de serialização/IO é offloaded para tasks paralelas com limite `MaxConcurrentSnapshotJobs` e fila limitada (`MaxPendingSnapshotJobs`). Excesso resulta em `DroppedJobs`.

### Estratégias de tuning
| Objetivo | Ajuste | Efeito |
|----------|--------|--------|
| Reduzir frequência | Aumentar `Interval` e/ou `MinEventsBetweenSnapshots` | Menos IO e CPU, snapshots mais distantes |
| Menos contenção | Aumentar `StableCaptureMaxAttempts` | Maior chance de captura estável sob escrita intensa |
| Menos latência de gravação | Reduzir `MaxConcurrentSnapshotJobs` | Menos threads de IO simultâneas |
| Garantir último estado no shutdown | `FinalSnapshotOnShutdown=true` | Passagem final bloqueante dentro do timeout |

### Limitações atuais
- Apenas tipo `Event` suporta snapshots (generic constraint lógica)
- Deltas incrementais ainda não implementados (`IEventDeltaWriter`/`Reader` placeholders)
- `CompactBeforeSnapshot` reservado (não usado)

### Exemplo de verificação pós‑restore
```csharp
var restored = await store.RestoreFromSnapshotsAsync();
if (restored > 0 && store.TryGetSnapshotMetrics(out var metrics))
{
    foreach (var p in metrics.Partitions)
        Debug.Assert(p.HeadVersion >= p.LastVersion);
}
```

### Erros comuns
- `InvalidOperationException` ao configurar duas vezes: cada instância só suporta um snapshotter.
- `ArgumentOutOfRangeException` em validação: revise limites mínimos (`MaxSaveAttempts >=1`, etc.).
- Falha de schema: defina `ExpectedSchemaVersion` somente quando a versão de serialização estiver definitivamente estável.

### Exemplos de Snapshots
Dois projetos de exemplo demonstram o uso prático do subsistema de snapshots persistentes:

#### 1. SnapshotSensors (Console)
Workload sintético de sensores (temperatura + umidade) em alta frequência demonstrando:
- Warm start: restaura o ring buffer a partir dos snapshots mais recentes no boot
- Capturas periódicas (gatilho de tempo + contagem de eventos)
- Snapshot final gracioso no shutdown (`FinalSnapshotOnShutdown=true`)
- Escrita atômica (`.snap.tmp` → rename para `.snap`)
- Pruning mantendo somente os N últimos por partição
- Métricas impressas periodicamente (Append, Dropped, SnapshotBytes, DroppedJobs, StableFailures)

Executar:
```bash
dotnet run --project samples/SnapshotSensors/SnapshotSensors.csproj
```
Interrompa (Ctrl+C), execute novamente e observe a linha:
```
[BOOT] Partitions restauradas de snapshot: X
```
Se X > 0 houve warm start.

Principais parâmetros (Program.cs):
- Interval = 5s
- MinEventsBetweenSnapshots = 100.000
- SnapshotsToKeep = 3
- FinalSnapshotOnShutdown = true (timeout 3s)
- Compressão habilitada (`BinarySnapshotSerializer(compress: true)`)

#### 2. SnapshotSensorsApi (Minimal API)
API HTTP que recebe leituras JSON e expõe estado e métricas:
- POST /sensor → gera dois eventos (temperatura chave=1, umidade chave=2) distribuídos por partições
- GET /state → agregados (min/max/avg/count) + contadores aproximados
- GET /metrics → métricas internas + snapshot metrics
- Restauração antes de iniciar o processamento (`RestoreFromSnapshotsAsync`)
- Snapshotter em background + impressão periódica

Executar:
```bash
dotnet run --project samples/SnapshotSensorsApi/SnapshotSensorsApi.csproj
```
Enviar leitura:
```bash
curl -X POST http://localhost:5000/sensor \
  -H "Content-Type: application/json" \
  -d '{"deviceId":"dev-1","temperature":22.5,"humidity":48.2}'
```
Consultar estado/métricas:
```bash
curl http://localhost:5000/state
curl http://localhost:5000/metrics
```
Configuração principal (Program.cs):
- Interval = 10s
- MinEventsBetweenSnapshots = 50.000
- MaxConcurrentSnapshotJobs = max(2, partitions/4)
- SnapshotsToKeep = 3
- FinalSnapshotOnShutdown = true (timeout 5s)
- Compressão habilitada

Ambos os exemplos evidenciam que o snapshot não bloqueia appends e que arquivos parciais nunca aparecem (renome atômico). Ajuste `Interval`, `MinEventsBetweenSnapshots` ou habilite `fsyncDirectory` (Unix) para explorar trade-offs.

## Licença
MIT
