using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using LockFree.EventStore;
using LockFree.EventStore.Snapshots;
using Xunit;
using LockFree.EventStore.Tests.TestDoubles;

namespace LockFree.EventStore.Tests;

public class SnapshotsAdditionalEdgeTests
{
    [Fact]
    public void BinarySnapshotSerializer_Compressed_Roundtrip()
    {
        var ser = new BinarySnapshotSerializer(compress: true);
        var state = new PartitionState
        {
            PartitionKey = "0",
            Version = 123,
            Events = new[] { new Event(new KeyId(1), 10.5, 111), new Event(new KeyId(2), 20.5, 222) },
            TakenAt = DateTimeOffset.UtcNow,
            SchemaVersion = 1
        };
        using var ms = new MemoryStream();
        ser.SerializeAsync(ms, state).AsTask().GetAwaiter().GetResult();
        ms.Position = 0;
        var restored = ser.DeserializeAsync(ms).AsTask().GetAwaiter().GetResult();
        Assert.Equal(state.Version, restored.Version);
        Assert.Equal(state.Events.Length, restored.Events.Length);
    }

    [Theory]
    [InlineData(-5, 1, 1, 0)] // negative count
    [InlineData(10_000_001, 1, 1, 0)] // count > limit
    public void BinarySnapshotSerializer_InvalidCounts_Throw(int count, long version, long ticks, int schema)
    {
        using var ms = new MemoryStream();
        using (var bw = new BinaryWriter(ms, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            bw.Write(schema);
            bw.Write("0");
            bw.Write(version);
            bw.Write(ticks);
            bw.Write(count);
        }
        ms.Position = 0;
        var ser = new BinarySnapshotSerializer();
        var ex = Assert.ThrowsAny<Exception>(() => ser.DeserializeAsync(ms).AsTask().GetAwaiter().GetResult());
        Assert.Contains("snapshot", ex.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BinarySnapshotSerializer_InvalidVersion_Ticks_Throw()
    {
        // negative version
        using var ms1 = new MemoryStream();
        using (var bw = new BinaryWriter(ms1, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            bw.Write(1); bw.Write("0"); bw.Write(-1L); bw.Write(DateTimeOffset.UtcNow.UtcTicks); bw.Write(0); // no events
        }
        ms1.Position = 0;
        var ser = new BinarySnapshotSerializer();
        Assert.ThrowsAny<Exception>(() => ser.DeserializeAsync(ms1).AsTask().GetAwaiter().GetResult());

        // out-of-range ticks
        using var ms2 = new MemoryStream();
        using (var bw2 = new BinaryWriter(ms2, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            bw2.Write(1); bw2.Write("0"); bw2.Write(0L); bw2.Write(long.MaxValue); bw2.Write(0);
        }
        ms2.Position = 0;
        Assert.ThrowsAny<Exception>(() => ser.DeserializeAsync(ms2).AsTask().GetAwaiter().GetResult());
    }

    [Fact]
    public void BinarySnapshotSerializer_TruncatedEvents_Throws()
    {
        using var ms = new MemoryStream();
        using (var bw = new BinaryWriter(ms, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            bw.Write(1); bw.Write("0"); bw.Write(1L); bw.Write(DateTimeOffset.UtcNow.UtcTicks); bw.Write(2); // claims 2 events
            // only write one incomplete event (missing timestamp)
            bw.Write(1); // key
            bw.Write(1.23); // value
        }
        ms.Position = 0;
        var ser = new BinarySnapshotSerializer();
        Assert.ThrowsAny<Exception>(() => ser.DeserializeAsync(ms).AsTask().GetAwaiter().GetResult());
    }

    [Fact]
    public void ExponentialBackoffPolicy_ArgumentValidation_And_Edge()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ExponentialBackoffPolicy(TimeSpan.Zero, 2));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ExponentialBackoffPolicy(TimeSpan.FromMilliseconds(1), 0));
        var policy = new ExponentialBackoffPolicy(TimeSpan.FromMilliseconds(5), 2);
        var d1 = policy.NextDelay(0); // coerced to attempt 1
        var d2 = policy.NextDelay(1); // second call (same logical attempt) may be >= d1 due to enforced monotonicity
        var dHuge = policy.NextDelay(10_000); // should overflow -> MaxValue (or extremely large)
        // With symmetric jitter first delay is in [4.5, 5.5) ms, second raw value could be smaller, but CAS logic forces non-decreasing.
        Assert.InRange(d1.TotalMilliseconds, 4.5, 5.5);
        Assert.True(d2 >= d1); // allow equality after CAS adjustment
        Assert.True(dHuge == TimeSpan.MaxValue || dHuge > d2);
    }

    [Fact]
    public async Task FileSystemSnapshotStore_PartitionKey_Validation()
    {
        var root = Path.Combine(Path.GetTempPath(), "snap_fs_keys_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var fs = new FileSystemSnapshotStore(root);
        var ser = new BinarySnapshotSerializer();
        var okMeta = new SnapshotMetadata("validKey", 1, DateTimeOffset.UtcNow, 1);
        using (var ms = new MemoryStream(new byte[] {1,2,3}))
        {
            await fs.SaveAsync(okMeta, ms);
        }
        // Expect UnauthorizedAccessException for traversal / separator cases, ArgumentException for invalid characters
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => {
            using var ms = new MemoryStream(new byte[]{1});
            await fs.SaveAsync(new SnapshotMetadata("..", 1, DateTimeOffset.UtcNow, 1), ms);
        });
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => {
            using var ms = new MemoryStream(new byte[]{1});
            await fs.SaveAsync(new SnapshotMetadata("a/b", 1, DateTimeOffset.UtcNow, 1), ms);
        });
        await Assert.ThrowsAsync<ArgumentException>(async () => {
            using var ms = new MemoryStream(new byte[]{1});
            await fs.SaveAsync(new SnapshotMetadata("with space", 1, DateTimeOffset.UtcNow, 1), ms);
        });
    }

    [Fact]
    public async Task FileSystemSnapshotStore_Save_Move_Failure_CleansTemp()
    {
        var root = Path.Combine(Path.GetTempPath(), "snap_fs_movefail_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var fs = new FileSystemSnapshotStore(root);
        var partition = "p1";
        var taken = new DateTimeOffset(2024,1,1,0,0,0,TimeSpan.Zero);
        var meta = new SnapshotMetadata(partition, 1, taken, 1);
        var finalName = $"{partition}_{meta.Version}_{meta.TakenAt.UtcTicks}.snap";
        var dir = Path.Combine(root, partition);
        Directory.CreateDirectory(dir);
        var finalPath = Path.Combine(dir, finalName);
        File.WriteAllBytes(finalPath, new byte[]{0}); // force move collision
        using var data = new MemoryStream(new byte[]{1,2,3});
        await Assert.ThrowsAsync<IOException>(async () => await fs.SaveAsync(meta, data));
        var tmpExists = Directory.GetFiles(dir, "*.tmp").Any();
        Assert.False(tmpExists); // temp cleaned
    }

    [Fact]
    public async Task FileSystemSnapshotStore_Prune_Zero_Kept()
    {
        var root = Path.Combine(Path.GetTempPath(), "snap_fs_prune_zero_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var fs = new FileSystemSnapshotStore(root);
        var partition = "p";
        for (int v = 1; v <= 3; v++)
        {
            var meta = new SnapshotMetadata(partition, v, DateTimeOffset.UtcNow.AddMinutes(v), 1);
            using var ms = new MemoryStream(new byte[]{1});
            await fs.SaveAsync(meta, ms);
        }
        var before = Directory.GetFiles(Path.Combine(root, partition), "*.snap").Length;
        await fs.PruneAsync(partition, 0);
        var after = Directory.GetFiles(Path.Combine(root, partition), "*.snap").Length;
        Assert.Equal(before, after);
    }

    [Fact]
    public async Task SnapshotHostedService_NoFinalSnapshot_When_Disabled()
    {
        var store = new EventStore<Event>(new EventStoreOptions<Event>{ CapacityPerPartition = 64, Partitions = 1 });
        var ser = new InMemoryJsonSnapshotSerializer();
        var backend = new InMemorySnapshotStore();
        var snap = store.ConfigureSnapshots(new SnapshotOptions { Enabled = true, Interval = TimeSpan.FromMinutes(10), MinEventsBetweenSnapshots = int.MaxValue, FinalSnapshotOnShutdown = false }, ser, backend);
        for (int i=0;i<20;i++) store.TryAppend(new Event(new KeyId(1), i, DateTime.UtcNow.Ticks));
        var hosted = new SnapshotHostedService(snap);
        await hosted.StartAsync(default);
        await hosted.StopAsync(default);
        store.TryGetSnapshotMetrics(out var metrics);
        Assert.All(metrics.Partitions, p => Assert.Equal(0, p.SuccessCount));
    }

    [Fact]
    public async Task NoopEventDeltaWriter_And_Reader()
    {
        var writer = new NoopEventDeltaWriter();
        await writer.AppendAsync("p", ReadOnlyMemory<Event>.Empty, 0);
        var reader = new NoopEventDeltaReader();
        var any = false;
        await foreach (var _ in reader.ReadSinceAsync("p", 0)) any = true;
        Assert.False(any);
    }

    [Fact]
    public void PaddedLockFreeRingBuffer_TryCopyStable_Paths()
    {
        var buf = new PaddedLockFreeRingBuffer<int>(8);
        // Empty fast path
        var okEmpty = buf.TryCopyStable(out var items0, out var ver0);
        Assert.True(okEmpty);
        Assert.Empty(items0);
        Assert.Equal(0, ver0);
        // Populate and try stable copy
        for (int i=0;i<6;i++) buf.TryEnqueue(i+1);
        var ok = buf.TryCopyStable(out var items, out var ver);
        Assert.True(ok);
        Assert.True(items.Length > 0);
        Assert.True(ver >= items.Length);
    }
}
