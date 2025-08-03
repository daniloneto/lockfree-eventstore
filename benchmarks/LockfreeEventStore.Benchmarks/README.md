# LockFree EventStore Benchmarks

Este projeto contém benchmarks abrangentes para a biblioteca LockFree.EventStore, comparando seu desempenho com alternativas do .NET em cenários de alta concorrência.

## 🎯 Objetivo

Medir **throughput**, **latência** e **alocações** em cenários MPMC (Multiple Producer, Multiple Consumer) incluindo:
- Operações de `Append` concorrente
- Consultas por janelas temporais
- Agregações com e sem filtros
- Snapshots concorrentes

## 🏗️ Estrutura do Projeto

```
LockfreeEventStore.Benchmarks/
├── Benchmarks/
│   ├── AppendOnlyBenchmarks.cs         # Append puro sem consumidores
│   ├── AppendWithSnapshotBenchmarks.cs # Append + snapshots periódicos
│   ├── WindowAggregateBenchmarks.cs    # Agregações por janela temporal
│   └── MixedHotColdKeysBenchmarks.cs   # Distribuição Zipf (hot/cold keys)
├── Competitors/
│   └── Competitors.cs                  # Implementações equivalentes para comparação
├── Utils/
│   ├── WorkloadGenerator.cs            # Gerador de carga com distribuição Zipf
│   └── TimingUtils.cs                  # Utilitários de timing e sincronização
├── BenchmarkConfig.cs                  # Configuração do BenchmarkDotNet
├── BenchmarkEvent.cs                   # Modelo de evento para benchmarks
├── BenchmarkOptions.cs                 # Opções de linha de comando
└── Program.cs                          # Entry point e CLI
```

## 🚀 Pré-requisitos

- **.NET 9.0** ou superior
- **Windows/Linux/macOS** (hardware counters dependem do OS)
- **Modo Release** recomendado para resultados precisos

### Dependências NuGet

- `BenchmarkDotNet` - Framework de benchmarking
- `CommandLineParser` - Parsing de argumentos CLI
- `HdrHistogram` - Medição precisa de latência
- `Disruptor` - Opcional, habilitado via `DISRUPTOR=true`

## 🎮 Como Executar

### Execução Básica

```bash
# Todos os benchmarks
dotnet run -c Release

# Categorias específicas
dotnet run -c Release -- --include=AppendOnly,WindowAggregate

# Competidores específicos
dotnet run -c Release -- --competitors=EventStore,ChannelBounded
```

### Opções Avançadas

```bash
# Filtrar por padrão
dotnet run -c Release -- --filter="*Append*"

# Execução in-process (mais rápida, menos precisa)
dotnet run -c Release -- --inprocess

# Personalizar iterações
dotnet run -c Release -- --warmup=5 --target=10

# Especificar diretório de resultados
dotnet run -c Release -- --artifacts=meus-resultados

# Desabilitar hardware counters
dotnet run -c Release -- --hardware-counters=false
```

### Habilitar Disruptor (Opcional)

```bash
# Compilar com suporte ao Disruptor
dotnet build -c Release -p:DISRUPTOR=true

# Executar incluindo Disruptor
dotnet run -c Release -- --competitors=EventStore,Disruptor
```

## 🏁 Cenários de Benchmark

### 1. AppendOnly
- **Foco**: Throughput puro de append
- **Produtores**: 1, 2, 4, 8, 16 threads
- **Capacidade**: 1K, 100K, 1M eventos
- **Distribuição**: Uniforme vs Zipf (skew 0.8)

### 2. AppendWithSnapshot
- **Foco**: Append concorrente com snapshots
- **Padrão**: Produtores + snapshots a cada 100ms
- **Métrica**: Impacto dos snapshots no throughput

### 3. WindowAggregate
- **Foco**: Agregações em janela temporal
- **Operações**: Count, Sum, Average, Min, Max
- **Janelas**: 100ms, 1s, 5s
- **Filtros**: 10% vs sem filtro

### 4. MixedHotColdKeys
- **Foco**: Distribuição de chaves realística
- **Skew**: 0.0 (uniforme), 0.5 (moderado), 0.9 (hot keys)
- **Workload**: Produtores + queries mistas + snapshots

## 🏆 Competidores

| Competidor | Descrição |
|------------|-----------|
| **EventStore** | LockFree.EventStore (esta biblioteca) |
| **ChannelBounded** | `System.Threading.Channels` com capacidade limitada |
| **ConcurrentQueue** | `ConcurrentQueue<T>` com descarte manual |
| **Disruptor** | Disruptor.NET ring buffer (opcional) |

## 📊 Métricas Coletadas

### Performance
- **Throughput**: Operações/segundo
- **Latência**: Mean, Median, P95, P99 (nanossegundos)
- **Throughput sustentado**: Durante operações concorrentes

### Recursos
- **Memory**: Alocações por operação (via MemoryDiagnoser)
- **GC**: Collections Gen0/1/2
- **Threading**: Contenção e context switches

### Hardware (se suportado)
- **Branch Mispredictions**
- **Cache Misses** 
- **Instructions Retired**

## 📁 Resultados

Os resultados são salvos automaticamente em `artifacts/results/`:

```
artifacts/results/
├── BenchmarkDotNet.Artifacts/
│   └── results/
│       ├── *-report.md      # Relatório em Markdown
│       ├── *-report.csv     # Dados em CSV
│       └── *-report.json    # Dados em JSON
```

## 🎯 Parâmetros de Benchmark

| Parâmetro | Valores | Descrição |
|-----------|---------|-----------|
| `ProducerCount` | 1, 2, 4, 8, 16 | Número de threads produtoras |
| `Capacity` | 1K, 100K, 1M | Capacidade do event store |
| `WindowMs` | 100, 1000, 5000 | Janela temporal em ms |
| `Skew` | 0.0, 0.8 | Distribuição Zipf (0=uniforme) |
| `FilterSelectivity` | 0.1, 1.0 | % de eventos que passam no filtro |
| `Competitor` | EventStore, Channel, etc. | Implementação a testar |

## 🔧 Configuração do BenchmarkDotNet

- **Runtime**: .NET 9.0
- **Mode**: Release only
- **Diagnosers**: Memory, Threading
- **Exporters**: Markdown, CSV, JSON
- **Hardware Counters**: Habilitados quando suportados
- **Process**: OutOfProcess (padrão) ou InProcess

## 🎲 Geração de Workload

### Distribuição Zipf
```csharp
// Skew = 0.0: distribuição uniforme
// Skew = 0.8: 20% das chaves recebem 80% dos acessos
// Skew = 0.9: distribuição muito enviesada (hot keys)
var generator = new ZipfGenerator(keyCount: 1000, skew: 0.8);
```

### Filtros de Seletividade
```csharp
// 10% dos eventos passam no filtro
var filter = WorkloadGenerator.CreateSelectivityFilter(0.1);
```

## 📈 Interpretação dos Resultados

### Throughput
- **Ops/s mais alto** = melhor performance
- **Scaling linear** com threads = boa concorrência
- **Degradação mínima** com filtros = implementação eficiente

### Latência
- **P99 baixo** = latência consistente
- **Mean próximo do Median** = distribuição uniforme
- **Spikes em P99** = contenção ou GC

### Memory
- **Allocated/Op baixo** = menos pressure no GC
- **Gen0 collections** = frequência de alocações pequenas
- **Gen2 collections** = objetos de longa duração

## 🚨 Boas Práticas para Benchmarks

### Ambiente
```bash
# Configurar prioridade alta do processo
# Fechar aplicações desnecessárias
# Desabilitar Turbo Boost para consistência
# Usar isolamento de CPU se possível
```

### Validação
```bash
# Verificar informações do ambiente
dotnet --info

# Validar resultados com múltiplas execuções
dotnet run -c Release -- --target=20

# Comparar com baseline conhecida
dotnet run -c Release -- --competitors=EventStore
```

## 🐛 Troubleshooting

### Hardware Counters não funcionam
```bash
# Executar com privilégios elevados (Windows)
# Ou desabilitar
dotnet run -c Release -- --hardware-counters=false
```

### Disruptor não compila
```bash
# Verificar se está disponível para sua plataforma
# Usar flag de compilação
dotnet build -c Release -p:DISRUPTOR=true
```

### OutOfMemory em benchmarks grandes
```bash
# Usar capacidades menores ou in-process
dotnet run -c Release -- --inprocess
```

## 🤝 Contribuição

Para adicionar novos cenários:

1. Implemente `IEventStoreCompetitor` para novos competidores
2. Adicione classe em `Benchmarks/` seguindo o padrão existente
3. Registre no `Program.cs` para CLI
4. Documente no README

## 📄 Licença

Este projeto segue a mesma licença da biblioteca principal LockFree.EventStore.
