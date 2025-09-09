using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Buffers;
using System.IO.Compression;

namespace LockFree.EventStore.Snapshots;

/// <summary>
/// Compact binary serializer for <see cref="PartitionState"/> with optional GZip compression.
/// </summary>
public sealed class BinarySnapshotSerializer(bool compress = false) : ISnapshotSerializer
{
    private readonly bool _compress = compress;

    /// <inheritdoc />
    public async ValueTask SerializeAsync(Stream destination, PartitionState state, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(state);
        using var ms = new MemoryStream();
        using (var bw = new BinaryWriter(ms, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            bw.Write(state.SchemaVersion);
            bw.Write(state.PartitionKey);
            bw.Write(state.Version);
            bw.Write(state.TakenAt.UtcDateTime.Ticks);
            bw.Write(state.Events.Length);
            foreach (var e in state.Events)
            {
                bw.Write(e.Key.Value);
                bw.Write(e.Value);
                bw.Write(e.TimestampTicks);
            }
        }
        ms.Position = 0;
        if (_compress)
        {
            using var gz = new GZipStream(destination, CompressionLevel.SmallestSize, leaveOpen: true);
            await ms.CopyToAsync(gz, ct).ConfigureAwait(false);
            await gz.FlushAsync(ct).ConfigureAwait(false);
        }
        else
        {
            await ms.CopyToAsync(destination, ct).ConfigureAwait(false);
            await destination.FlushAsync(ct).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public ValueTask<PartitionState> DeserializeAsync(Stream source, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        return _compress ? DeserializeCompressedAsync(source) : DeserializePlainAsync(source);
    }

    private static ValueTask<PartitionState> DeserializePlainAsync(Stream source)
    {
        using var br = new BinaryReader(source, System.Text.Encoding.UTF8, leaveOpen: true);
        return new ValueTask<PartitionState>(ReadPartition(br));
    }

    private static PartitionState ReadPartition(BinaryReader br)
    {
        var schema = br.ReadInt32();
        var partitionKey = br.ReadString();
        var version = br.ReadInt64();
        var takenAtTicks = br.ReadInt64();
        var count = br.ReadInt32();
        var arr = new Event[count];
        for (var i = 0; i < count; i++)
        {
            var keyVal = br.ReadInt32();
            var value = br.ReadDouble();
            var ts = br.ReadInt64();
            arr[i] = new Event(new KeyId(keyVal), value, ts);
        }
        return new PartitionState
        {
            PartitionKey = partitionKey,
            Version = version,
            SchemaVersion = schema,
            TakenAt = new DateTimeOffset(takenAtTicks, TimeSpan.Zero),
            Events = arr
        };
    }

    private static async ValueTask<PartitionState> DeserializeCompressedAsync(Stream source)
    {
        using var gz = new GZipStream(source, CompressionMode.Decompress, leaveOpen: true);
        using var br = new BinaryReader(gz, System.Text.Encoding.UTF8, leaveOpen: true);
        await Task.Yield(); // ensure async path for symmetry
        return ReadPartition(br);
    }
}
