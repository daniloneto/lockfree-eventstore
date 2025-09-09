# SnapshotSensors Sample

Demonstra a feature de snapshots (RFC005) em um cenário de aquecimento rápido (warm start) de sensores IoT.

Objetivos:
- Restaurar o estado (últimos eventos por partição) logo no boot.
- Produzir leituras em alta taxa sem bloquear o caminho de Append.
- Salvar snapshots periódicos em disco usando gravação atômica (write-temp + rename).
- Efetuar pruning automático, mantendo somente os N mais recentes por partição.
- Gerar snapshot final no shutdown limpo.

## Executando

Dentro da raiz do repositório:

```
dotnet run --project samples/SnapshotSensors/SnapshotSensors.csproj
```

Pare (Ctrl+C) após alguns segundos, execute novamente e observe a linha:

```
[BOOT] Partitions restauradas de snapshot: X
```

Se X > 0 houve warm start (estado reaplicado a partir de snapshots).

## Parâmetros principais (Program.cs)
- Intervalo: 5s
- MinEventsBetweenSnapshots: 100.000
- SnapshotsToKeep: 3
- FinalSnapshotOnShutdown: true (timeout 3s)
- Compressão GZip habilitada

## Estrutura de diretórios
Snapshots ficam em `./samples/SnapshotSensors/bin/<config>/<tfm>/snapshots/<partition>/` com nome:
```
{partition}_{version}_{ticks}.snap
```

Arquivos `.tmp` não aparecem porque são renomeados de forma atômica ao final.

## Métricas
A cada 10s imprime:
- Append / Dropped / SnapshotBytes
- Métricas do snapshotter (DroppedJobs, FailedJobs, StableFailures)

## Ajustes sugeridos
- Reduza Interval para 1s e veja concorrência de jobs.
- Aumente capacidade por partição para testar wrap-around.
- Habilite fsyncDirectory=true (Unix) para maior durabilidade (trade-off desempenho).
