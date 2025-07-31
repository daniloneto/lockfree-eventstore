# lockfree-eventstore
Armazenamento em memória de alta performance para eventos (ex.: transações, métricas). 

Usa ConcurrentQueue + operações atômicas (Interlocked, Volatile) para ingestão sem locks, snapshot O(1) de contadores e soma (em centavos) e agregação por intervalo de tempo ao iterar a fila.

Sem dependências, compatível com AOT/Trimming.
