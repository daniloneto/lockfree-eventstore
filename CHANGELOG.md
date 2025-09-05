# Changelog

## 1.0.12 - 2025-09-04
### Added
- Significantly expanded test coverage: invalid options, additional validations, window/aggregation (bucketed & runtime), statistics callbacks, zero-allocation paths, concurrency, internal ring buffer and partitions.
- Tests for additional construction scenarios (EventStoreBuilder) and internal branching.
### Changed
- Validation exceptions in `EventStoreOptions` now use `InvalidOperationException` for clarity and consistency.
- Standardized local variable declarations to `var` where appropriate.
- Internal refactors (ring buffer / partitioners) for readability and micro-optimizations.
### Performance
- Maintained zero-allocation optimizations and reinforced validations to keep hot path overhead low.
### Tests
- Consolidated and expanded suites increasing branch and line coverage.

## 1.0.11 - 2025-08-26
### Added
- `MetricsDashboard` tests and integration improvements (testable `Program`).
### Changed
- Enhanced performance metrics and error handling in PowerShell scripts.
- Standardized UTC timestamps in scripts and metrics.
- Improved aggregation tests.
### Fixed
- Sample adjustments and fixes.
### Performance
- Fine tuning of metrics and aggregations reducing latency under load.

## 1.0.10 - 2025-08-25
### Removed
- Obsolete code and tests (OptimizedPartition, Padding, SpecializedEventStore duplicates / redundant paths) reducing complexity and build/test time.
### Changed
- Slimmed test suite focusing on active paths while retaining essential coverage.

## 1.0.9 - 2025-08-24
### Added
- Runtime window tracking option with associated tests.
- Bucketed window aggregation for more precise temporal analysis.
- Explicit validation of `EventStoreOptions` (constructors perform early validation).
- Additional zero-allocation optimizations (snapshots, aggregations, query methods) plus padded counters to reduce false sharing.
### Changed
- Readability/performance refactors in stores and telemetry counters.
- README revised for clearer explanation of features, windows and performance.
### Performance
- Adoption of zero-allocation query methods in tests and hot execution paths.
### Tests
- Expanded coverage: window tracking, ring buffer, aggregations, snapshots and validation paths.

## 1.0.8 - 2025-08-21

New Features

Batch append operations and filtered snapshots with per-partition snapshot views.
Factory helpers to quickly create optimized event stores.
Specialized store gains string-key add/query and zero-allocation APIs.
Metrics Dashboard reorganized with explicit startup, health endpoint, expanded metrics and streams routes, and consolidated admin actions.
Client sample introduces a MetricEvent type.
Chores

Added SonarScanner tool and ignores for its artifacts.
Refactor

Standardized argument validation and reorganized route mapping.
Tests

Centralized test expectations and enabled MSBuild-based coverage.

Tests
Added comprehensive unit tests expanding coverage across event store statistics, key/id mapping, lock-free ring buffers, optimized partitions (AoS/SoA), padded ring buffers, and store statistics.
Validates construction edge cases, capacity handling, enqueue/discard behavior, window/view creation (including wrap-around), zero-allocation paths, snapshots (partial and full), ordering, and epoch/state tracking.
Confirms value semantics, bidirectional mappings, and formatted outputs.
Improves reliability by exercising normal and boundary scenarios without changing public APIs.


## 1.0.7 - 2025-08-20

New Features

Richer analytics: batch appends, key-id hot-paths, and time-windowed queries/aggregations.
Samples

Samples now include .env/.env.example and read EVENTSTORE_URL from environment; HTTP clients use DI.
Documentation

README translated to English; CHANGELOG updated for release 1.0.6 (2025-08-20).
Refactor

Code reorganized for readability, maintainability and SonarQube.
Tests

Large expansion of unit tests covering windowing, zero-allocation paths, ring buffers, partitions and helpers.
Chores

Added code-coverage and tooling configuration.

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
