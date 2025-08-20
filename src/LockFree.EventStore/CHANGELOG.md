# Changelog

## 1.0.6 - 2025-08-20
### Added
- Refactor for SonarQube
- .env and .env.example files in samples (ClientSample and GatewayClient)

### Changed
- Refactoring of code structure for better readability and maintainability
- Adjustments in samples for configuration via environment variables
- Documentation: README translated to English

## 1.0.3 - 2025-08-10
### Added
- Generic stream endpoints (/streams/{*stream} POST/GET, aggregation via ?aggregate=true)
- durationMs field in aggregation responses
- Administrative endpoints /admin/clear, /admin/reset, /admin/purge
- GatewayClient sample with endpoints /orders, /orders/bulk, /stats/local, /stats/global
- docker-compose with scalable gateway service and central server
- Nginx configuration for load balancing (least_conn + dynamic resolution)
- Scenario script run-gateway-scenario.ps1
- Multi-stage Dockerfiles (server and gateway)
- Ulid dependency for ID generation

### Changed
- Aggregation route adjustment: use of query ?aggregate=true (catch-all for streams)
- README improvements (in progress) and sample structure

### Fixed
- Support for stream names with slashes (catch-all)
- 500 error in aggregation due to previous invalid route
- XML documentation warnings marking sample types as internal

## 1.0.1 / 1.0.2
- Updated metadata and examples, README improvements

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
- Initial implementation with lock-free ring buffer, partitioning, and aggregations.
