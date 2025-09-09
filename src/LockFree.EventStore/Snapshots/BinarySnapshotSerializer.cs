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
        var startPos = br.BaseStream.CanSeek ? br.BaseStream.Position : -1;
        string? partitionKey = null;
        try
        {
            var schema = br.ReadInt32();
            partitionKey = br.ReadString();
            var version = br.ReadInt64();
            var takenAtTicks = br.ReadInt64();
            var count = br.ReadInt32();

            // Basic validations
            if (count < 0)
            {
                throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"Negative event count: {count}");
            }
            const int MaxEvents = 10_000_000; // safety upper bound (tunable)
            if (count > MaxEvents)
            {
                throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"Event count {count} exceeds limit {MaxEvents}");
            }
            if (version < 0)
            {
                throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"Negative version: {version}");
            }
            // Ticks sanity (DateTimeOffset supports a wide range; optionally reject far-future/past)
            if (takenAtTicks < DateTimeOffset.MinValue.UtcTicks || takenAtTicks > DateTimeOffset.MaxValue.UtcTicks)
            {
                throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"TakenAt ticks out of range: {takenAtTicks}");
            }

            var arr = new Event[count];
            for (var i = 0; i < count; i++)
            {
                try
                {
                    var keyVal = br.ReadInt32();
                    var value = br.ReadDouble();
                    var ts = br.ReadInt64();
                    arr[i] = new Event(new KeyId(keyVal), value, ts);
                }
                catch (EndOfStreamException eof)
                {
                    throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"Unexpected end of stream reading event {i}", eof);
                }
                catch (IOException ioex)
                {
                    throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"I/O error reading event {i}", ioex);
                }
                catch (ArgumentOutOfRangeException oore)
                {
                    throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"Value out of range reading event {i}", oore);
                }
                catch (FormatException fe)
                {
                    throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"Format error reading event {i}", fe);
                }
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
        catch (EndOfStreamException ex)
        {
            throw new SnapshotDeserializationException(partitionKey, null, startPos, "Unexpected end of stream while reading snapshot header", ex);
        }
        catch (IOException ex)
        {
            throw new SnapshotDeserializationException(partitionKey, null, startPos, "I/O error while reading snapshot", ex);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            throw new SnapshotDeserializationException(partitionKey, null, startPos, "Value out of range while reading snapshot", ex);
        }
        catch (FormatException ex)
        {
            throw new SnapshotDeserializationException(partitionKey, null, startPos, "Format error while reading snapshot", ex);
        }
    }

    private static async ValueTask<PartitionState> DeserializeCompressedAsync(Stream source)
    {
        using var gz = new GZipStream(source, CompressionMode.Decompress, leaveOpen: true);
        using var br = new BinaryReader(gz, System.Text.Encoding.UTF8, leaveOpen: true);
        await Task.Yield(); // ensure async path for symmetry
        return ReadPartition(br);
    }
}

// Added custom exception for clearer snapshot deserialization failures.
internal sealed class SnapshotDeserializationException(string? partitionKey, int? schemaVersion, long startPosition, string message, Exception? inner = null)
    : Exception(message, inner)
{
    public string? PartitionKey { get; } = partitionKey;
    public int? SchemaVersion { get; } = schemaVersion;
    public long StartPosition { get; } = startPosition;

    public override string ToString()
    {
        return string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{base.ToString()} (PartitionKey={PartitionKey ?? "<unknown>"}, Schema={(SchemaVersion.HasValue ? SchemaVersion.Value.ToString(System.Globalization.CultureInfo.InvariantCulture) : "<unknown>")}, StartPos={StartPosition})");
    }
}
