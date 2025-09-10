using System.IO;
using System.IO.Compression;
using System.Text;

namespace LockFree.EventStore.Snapshots;

/// <summary>
/// Compact binary serializer for <see cref="PartitionState"/> with optional GZip compression.
/// Now writes directly to the destination stream (avoids intermediate buffering) and
/// auto-detects GZip magic bytes on deserialize to tolerate configuration changes.
/// </summary>
public sealed class BinarySnapshotSerializer(bool compress = false) : ISnapshotSerializer
{
    private readonly bool _compress = compress;

    /// <inheritdoc />
    public async ValueTask SerializeAsync(Stream destination, PartitionState state, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(state);
        // Write directly to destination to avoid double buffering. Apply compression if configured.
        var target = destination;
        GZipStream? gzip = null;
        if (_compress)
        {
            // SmallestSize for archival snapshot; adjust if throughput preferred.
            gzip = new GZipStream(destination, CompressionLevel.SmallestSize, leaveOpen: true);
            target = gzip;
        }
        // BinaryWriter is sync; previous implementation buffered into MemoryStream synchronously too, then async copied.
        // For very large snapshots this reduces peak memory. Cancellation is checked periodically.
        using (var bw = new BinaryWriter(target, Encoding.UTF8, leaveOpen: true))
        {
            bw.Write(state.SchemaVersion);
            bw.Write(state.PartitionKey);
            bw.Write(state.Version);
            bw.Write(state.TakenAt.UtcDateTime.Ticks);
            bw.Write(state.Events.Length);
            var events = state.Events;
            for (var i = 0; i < events.Length; i++)
            {
                if ((i & 0x3FF) == 0) // every 1024 events
                {
                    ct.ThrowIfCancellationRequested();
                }
                var e = events[i];
                bw.Write(e.Key.Value);
                bw.Write(e.Value);
                bw.Write(e.TimestampTicks);
            }
        }
        if (gzip != null)
        {
            await gzip.FlushAsync(ct).ConfigureAwait(false); // ensures trailer written
            await gzip.DisposeAsync().ConfigureAwait(false); // finish compression asynchronously (leaveOpen: true keeps destination open)
        }
        await destination.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask<PartitionState> DeserializeAsync(Stream source, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        // Auto-detect GZip magic (0x1F, 0x8B) when possible to tolerate config changes between runs.
        bool? detectedGzip = null;
        if (source.CanSeek)
        {
            var pos = source.Position;
            Span<byte> header = stackalloc byte[2];
            var read = source.Read(header);
            source.Position = pos; // rewind
            if (read == 2)
            {
                detectedGzip = header[0] == 0x1F && header[1] == 0x8B;
            }
        }
        // If detection succeeded use it, else fall back to configured behavior.
        var useGzip = detectedGzip ?? _compress;
        return useGzip ? DeserializeCompressedAsync(source) : DeserializePlainAsync(source);
    }

    private static ValueTask<PartitionState> DeserializePlainAsync(Stream source)
    {
        using var br = new BinaryReader(source, Encoding.UTF8, leaveOpen: true);
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
            ValidateHeader(partitionKey, schema, version, takenAtTicks, count, startPos);
            var arr = ReadEvents(br, count, partitionKey, schema, startPos);
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

    private const int MaxEvents = 10_000_000; // safety upper bound (tunable)

    private static void ValidateHeader(string? partitionKey, int schema, long version, long takenAtTicks, int count, long startPos)
    {
        if (count < 0)
        {
            throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"Negative event count: {count}");
        }
        if (count > MaxEvents)
        {
            throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"Event count {count} exceeds limit {MaxEvents}");
        }
        if (version < 0)
        {
            throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"Negative version: {version}");
        }
        if (takenAtTicks < DateTimeOffset.MinValue.UtcTicks || takenAtTicks > DateTimeOffset.MaxValue.UtcTicks)
        {
            throw new SnapshotDeserializationException(partitionKey, schema, startPos, $"TakenAt ticks out of range: {takenAtTicks}");
        }
    }

    private static Event[] ReadEvents(BinaryReader br, int count, string? partitionKey, int schema, long startPos)
    {
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
        return arr;
    }

    private static async ValueTask<PartitionState> DeserializeCompressedAsync(Stream source)
    {
        using var gz = new GZipStream(source, CompressionMode.Decompress, leaveOpen: true);
        using var br = new BinaryReader(gz, Encoding.UTF8, leaveOpen: true);
        await Task.Yield(); // ensure async path for symmetry
        return ReadPartition(br);
    }
}

/// <summary>
/// Exception thrown when a snapshot cannot be deserialized due to invalid or corrupt data.
/// Provides context such as partition key, schema version and the starting position in the stream.
/// </summary>
/// <param name="partitionKey">The partition key read from the snapshot (if available).</param>
/// <param name="schemaVersion">The schema version read from the snapshot header (if available).</param>
/// <param name="startPosition">The starting position of the snapshot within the source stream (if seekable).</param>
/// <param name="message">Error description.</param>
/// <param name="inner">Inner exception that triggered the failure (if any).</param>
public sealed class SnapshotDeserializationException(string? partitionKey, int? schemaVersion, long startPosition, string message, Exception? inner = null)
    : Exception(message, inner)
{
    /// <summary>Partition key of the snapshot (may be null if header parse failed).</summary>
    public string? PartitionKey { get; } = partitionKey;
    /// <summary>Schema version of the snapshot (may be null if header parse failed).</summary>
    public int? SchemaVersion { get; } = schemaVersion;
    /// <summary>The stream position where deserialization began (or -1 if not seekable).</summary>
    public long StartPosition { get; } = startPosition;

    /// <inheritdoc />
    public override string ToString()
    {
        return string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{base.ToString()} (PartitionKey={PartitionKey ?? "<unknown>"}, Schema={(SchemaVersion.HasValue ? SchemaVersion.Value.ToString(System.Globalization.CultureInfo.InvariantCulture) : "<unknown>")}, StartPos={StartPosition})");
    }
}
