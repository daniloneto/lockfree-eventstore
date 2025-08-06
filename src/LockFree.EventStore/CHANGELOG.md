# Changelog

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
