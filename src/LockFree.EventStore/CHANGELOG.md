# Changelog

## 1.0.3 - 2025-08-10
### Added
- Endpoints genéricos de streams (/streams/{*stream} POST/GET, agregação via ?aggregate=true)
- Campo durationMs nas respostas de agregação
- Endpoints administrativos /admin/clear, /admin/reset, /admin/purge
- Sample GatewayClient com endpoints /orders, /orders/bulk, /stats/local, /stats/global
- docker-compose com serviço escalável de gateway e servidor central
- Configuração Nginx para balanceamento (least_conn + resolução dinâmica)
- Script de cenário run-gateway-scenario.ps1
- Dockerfiles multi-stage (server e gateway)
- Dependência Ulid para geração de IDs

### Changed
- Ajuste rota agregação: uso de query ?aggregate=true (catch-all de streams)
- Melhorias README (em andamento) e estrutura de samples

### Fixed
- Suporte a nomes de stream com barra (catch-all)
- 500 em agregação devido a rota anterior inválida
- Avisos de documentação XML marcando tipos sample como internal

## 1.0.1 / 1.0.2
- Metadados e exemplos atualizados, melhorias de README

### Features
- Enhance telemetry with statistics tracking and callback support
- Add anti-false sharing protection and improve performance
- Complete zero-allocation optimization with advanced features
- Implement zero-allocation methods using pooled buffers
- Add Buffers utility for zero-allocation array pooling
- Implement KeyId and KeyMap for hot path optimization in event store
- Implement zero-allocation event processing and add related tests
- Add high-performance batch operations and optimizations
- Implement snapshot view-based with chunks (P1)
- Implement incremental window aggregation (P1)
- New features

### Refactor
- Refactor EventStore implementation and introduce SpecializedEventStore

### Release
- release 1.0

## 0.1.0
- Implementação inicial com ring buffer lock-free, particionamento e agregações.
