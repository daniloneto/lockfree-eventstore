using LockFree.EventStore.Snapshots;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace LockFree.EventStore.Tests.TestDoubles;

/// <summary>JSON based serializer for tests (not optimized).</summary>
internal sealed class InMemoryJsonSnapshotSerializer : ISnapshotSerializer
{
    public async ValueTask SerializeAsync(Stream destination, PartitionState state, CancellationToken ct = default)
    {
        await JsonSerializer.SerializeAsync(destination, state, cancellationToken: ct);
    }

    public async ValueTask<PartitionState> DeserializeAsync(Stream source, CancellationToken ct = default)
    {
        var ps = await JsonSerializer.DeserializeAsync<PartitionState>(source, cancellationToken: ct);
        return ps!; // tests control schema
    }
}
