using System.Collections.Concurrent;
using LockFree.EventStore.Snapshots;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System;
using System.Runtime.CompilerServices;

namespace LockFree.EventStore.Tests.TestDoubles;

/// <summary>
/// In-memory snapshot store for tests (no I/O, deterministic ordering).
/// </summary>
internal sealed class InMemorySnapshotStore : ISnapshotStore
{
    private readonly ConcurrentDictionary<string, List<(SnapshotMetadata Meta, byte[] Data)>> _data = new();

    public ValueTask SaveAsync(SnapshotMetadata meta, Stream data, CancellationToken ct = default)
    {
        using var ms = new MemoryStream();
        data.CopyTo(ms);
        var arr = ms.ToArray();
        var list = _data.GetOrAdd(meta.PartitionKey, _ => new List<(SnapshotMetadata, byte[])>());
        lock (list)
        {
            list.Add((meta, arr));
            list.Sort((a,b) => a.Meta.Version.CompareTo(b.Meta.Version));
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask<(SnapshotMetadata Meta, Stream Data)?> TryLoadLatestAsync(string partitionKey, CancellationToken ct = default)
    {
        if (!_data.TryGetValue(partitionKey, out var list) || list.Count == 0)
        {
            return ValueTask.FromResult<(SnapshotMetadata, Stream)?>(null);
        }
        lock (list)
        {
            var last = list[^1];
            return ValueTask.FromResult<(SnapshotMetadata, Stream)?>( ( last.Meta, new MemoryStream(last.Data, writable:false) ) );
        }
    }

    public async IAsyncEnumerable<string> ListPartitionKeysAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var k in _data.Keys.OrderBy(k=>k))
        {
            if (ct.IsCancellationRequested) yield break;
            yield return k;
            await Task.Yield();
        }
    }

    public ValueTask PruneAsync(string partitionKey, int snapshotsToKeep, CancellationToken ct = default)
    {
        if (!_data.TryGetValue(partitionKey, out var list)) return ValueTask.CompletedTask;
        lock (list)
        {
            // Clamp snapshotsToKeep to non-negative and compute bounded removal count
            snapshotsToKeep = Math.Max(0, snapshotsToKeep);
            var remove = Math.Max(0, list.Count - snapshotsToKeep);
            if (remove > 0)
            {
                if (remove > list.Count) remove = list.Count; // extra safety
                list.RemoveRange(0, remove);
            }
        }
        return ValueTask.CompletedTask;
    }
}
