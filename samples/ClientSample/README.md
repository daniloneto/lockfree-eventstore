# ClientSample

Exemplo de cliente que consome o LockFree.EventStore rodando em um container:

Passos:

```bash
# 1. Suba o servidor (já publicado no Docker Hub)
docker run --rm -p 7070:7070 daniloneto/lockfree-eventstore:latest

# 2. Rode o sample
cd samples/ClientSample
 dotnet run
```

Variáveis de ambiente:
- `EVENTSTORE_URL` (default: http://localhost:7070)

O código faz:
1. Gera e envia 1000 eventos em paralelo para o stream `gateway/orders`.
2. Lê os eventos de volta.
3. Calcula a soma do campo `Valor`.

Ajuste os endpoints conforme a API real exposta pelo servidor.
