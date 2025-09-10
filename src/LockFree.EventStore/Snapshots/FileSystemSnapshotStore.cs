using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace LockFree.EventStore.Snapshots;

/// <summary>
/// File-system snapshot store using atomic write-temp + rename and pruning.
/// </summary>
public sealed partial class FileSystemSnapshotStore : ISnapshotStore // made partial to support GeneratedRegex
{
    private readonly string _root;
    private readonly bool _fsyncDir;
    private static long _tempCounter; // monotonic counter for uniqueness
    // Removed compiled Regex field in favor of source-generated regex for performance & no startup cost.
    [GeneratedRegex("^[A-Za-z0-9_-]+$", RegexOptions.CultureInvariant)]
    private static partial Regex AllowedPartitionKeyRegex();

    /// <summary>Create store rooted at directory (created if absent).</summary>
    public FileSystemSnapshotStore(string rootDirectory, bool fsyncDirectory = false, IBackoffPolicy? backoff = null)
    {
        _root = rootDirectory ?? throw new ArgumentNullException(nameof(rootDirectory));
        _ = Directory.CreateDirectory(_root);
        _fsyncDir = fsyncDirectory;
        _ = backoff; // reserved future use
    }

    private static string PartitionDir(string root, string partitionKey)
    {
        // Validate partitionKey to avoid path traversal or invalid names.
        if (string.IsNullOrWhiteSpace(partitionKey))
        {
            throw new ArgumentException("Partition key must be non-empty", nameof(partitionKey));
        }
        if (Path.IsPathRooted(partitionKey))
        {
            throw new UnauthorizedAccessException("Rooted partition keys are not allowed");
        }
        if (partitionKey.Contains("..", StringComparison.Ordinal))
        {
            throw new UnauthorizedAccessException("Partition key must not contain '..'");
        }
        if (partitionKey.Contains(Path.DirectorySeparatorChar) || partitionKey.Contains(Path.AltDirectorySeparatorChar))
        {
            throw new UnauthorizedAccessException("Partition key must not contain directory separators");
        }
        if (partitionKey.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
        {
            throw new ArgumentException("Partition key contains invalid characters", nameof(partitionKey));
        }
        if (!AllowedPartitionKeyRegex().IsMatch(partitionKey))
        {
            throw new ArgumentException("Partition key contains disallowed characters", nameof(partitionKey));
        }
        // Block Windows reserved device names early for clearer errors (CON, PRN, AUX, NUL, COM1..COM9, LPT1..LPT9)
        if (OperatingSystem.IsWindows())
        {
            var trimmed = partitionKey.TrimEnd('.', ' ');
            var up = trimmed.ToUpperInvariant();
            static bool IsDevice(string u)
            {
                if (u is "CON" or "PRN" or "AUX" or "NUL")
                {
                    return true;
                }
                if (u.Length == 4 && char.IsDigit(u[3]) && u[3] != '0')
                {
                    if (u.StartsWith("COM", StringComparison.Ordinal) || u.StartsWith("LPT", StringComparison.Ordinal))
                    {
                        return true;
                    }
                }
                return false;
            }
            if (IsDevice(up))
            {
                throw new ArgumentException("Partition key is a reserved device name on Windows", nameof(partitionKey));
            }
        }

        var rootFull = Path.GetFullPath(root);
        if (!rootFull.EndsWith(Path.DirectorySeparatorChar))
        {
            rootFull += Path.DirectorySeparatorChar;
        }
        var combined = Path.GetFullPath(Path.Combine(rootFull, partitionKey));
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        return !combined.StartsWith(rootFull, comparison)
            ? throw new UnauthorizedAccessException("Partition key resolves outside the snapshot root")
            : combined;
    }

    private static string NewTempPath(string dir, string finalName)
    {
        // finalName itself already validated via partition key and construction.
        var ticks = DateTime.UtcNow.Ticks;
        var ctr = Interlocked.Increment(ref _tempCounter);
        var pid = Environment.ProcessId;
        var guid = Guid.NewGuid().ToString("N");
        // Pattern keeps characters filesystem-friendly and unique: <final>.wip.<pid>.<ctr>.<ticks>.<guid>.tmp
        return Path.Combine(dir, $"{finalName}.wip.{pid}.{ctr}.{ticks}.{guid}.tmp");
    }

    /// <inheritdoc />
    public async ValueTask SaveAsync(SnapshotMetadata meta, Stream data, CancellationToken ct = default)
    {
        var dir = PartitionDir(_root, meta.PartitionKey);
        _ = Directory.CreateDirectory(dir);
        var finalName = $"{meta.PartitionKey}_{meta.Version}_{meta.TakenAt.UtcTicks}.snap";
        var finalPath = Path.Combine(dir, finalName);
        var tmpPath = NewTempPath(dir, finalName); // unique per attempt

        FileStream? fs = null;
        var moved = false;
        try
        {
            fs = new FileStream(tmpPath, new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                Share = FileShare.None,
                BufferSize = 64 * 1024,
                Options = FileOptions.WriteThrough | FileOptions.Asynchronous
            });
            try
            {
                await data.CopyToAsync(fs, ct).ConfigureAwait(false);
                await fs.FlushAsync(ct).ConfigureAwait(false);
                fs.Flush(flushToDisk: true);
            }
            finally
            {
                await fs.DisposeAsync().ConfigureAwait(false);

                fs = null;
            }

            // Verify temp file still exists (defensive, should always be true)
            if (!File.Exists(tmpPath))
            {
                throw new IOException($"Temporary snapshot file '{tmpPath}' disappeared before move.");
            }

            // Attempt atomic move into place (no overwrite to preserve first writer and surface races)
            File.Move(tmpPath, finalPath, overwrite: false);
            moved = true;

            if (_fsyncDir)
            {
                TryFsyncDirectory(dir);
            }
        }
        finally
        {
            // Best-effort cleanup on any failure path (including exceptions & cancellation)
            if (!moved)
            {
                try
                {
                    if (fs != null)
                    {
                        await fs.DisposeAsync().ConfigureAwait(false);
                    }
                }
                catch { /* ignore */ }
                try
                {
                    if (File.Exists(tmpPath))
                    {
                        File.Delete(tmpPath);
                    }
                }
                catch { /* ignore cleanup errors */ }
            }
        }
    }

    /// <inheritdoc />
    public ValueTask<(SnapshotMetadata Meta, Stream Data)?> TryLoadLatestAsync(string partitionKey, CancellationToken ct = default)
    {
        var dir = PartitionDir(_root, partitionKey);
        if (!Directory.Exists(dir))
        {
            return new ValueTask<(SnapshotMetadata Meta, Stream Data)?>(result: null);
        }
        var files = Directory.EnumerateFiles(dir, "*.snap", SearchOption.TopDirectoryOnly);
        SnapshotMetadata? bestMeta = null;
        string? bestPath = null;
        foreach (var f in files)
        {
            ct.ThrowIfCancellationRequested();
            var name = Path.GetFileNameWithoutExtension(f);
            var parts = name.Split('_');
            if (parts.Length < 3)
            {
                continue;
            }
            if (!long.TryParse(parts[^2], out var version))
            {
                continue;
            }
            if (!long.TryParse(parts[^1], out var ticks))
            {
                continue;
            }
            var meta = new SnapshotMetadata(partitionKey, version, new DateTimeOffset(ticks, TimeSpan.Zero));
            if (bestMeta is null || meta.Version > bestMeta.Value.Version || (meta.Version == bestMeta.Value.Version && meta.TakenAt > bestMeta.Value.TakenAt))
            {
                bestMeta = meta;
                bestPath = f;
            }
        }
        if (bestMeta is null || bestPath is null)
        {
            return new ValueTask<(SnapshotMetadata Meta, Stream Data)?>(result: null);
        }
        // Stream the snapshot file directly to avoid large in-memory allocation; caller is responsible for disposal.
        var fs = new FileStream(bestPath, new FileStreamOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.ReadWrite | FileShare.Delete,
            Options = FileOptions.Asynchronous | FileOptions.SequentialScan
        });
        return new ValueTask<(SnapshotMetadata Meta, Stream Data)?>((bestMeta.Value, fs));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> ListPartitionKeysAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var dir in Directory.EnumerateDirectories(_root))
        {
            ct.ThrowIfCancellationRequested();
            var name = Path.GetFileName(dir);
            if (string.IsNullOrWhiteSpace(name))
            {
                continue;
            }
            // Filter out any directory names that do not satisfy the allowed partition key regex
            if (!AllowedPartitionKeyRegex().IsMatch(name))
            {
                continue;
            }
            yield return name;
            await Task.Yield();
        }
    }

    /// <inheritdoc />
    public async ValueTask PruneAsync(string partitionKey, int snapshotsToKeep, CancellationToken ct = default)
    {
        if (snapshotsToKeep < 1)
        {
            return;
        }
        var dir = PartitionDir(_root, partitionKey);
        if (!Directory.Exists(dir))
        {
            return;
        }

        var entries = new List<(FileInfo File, long Version, long Ticks)>();
        foreach (var path in Directory.EnumerateFiles(dir, "*.snap", SearchOption.TopDirectoryOnly))
        {
            ct.ThrowIfCancellationRequested();
            if (TryParseSnapshotEntry(path, out var entry))
            {
                entries.Add(entry);
            }
        }

        if (entries.Count <= snapshotsToKeep)
        {
            return;
        }

        var ordered = entries
            .OrderByDescending(e => e.Version)
            .ThenByDescending(e => e.Ticks)
            .ToList();
        var toDelete = ordered.Skip(snapshotsToKeep).ToList();
        var deleted = 0;
        foreach (var e in toDelete)
        {
            try
            {
                e.File.Delete();
                deleted++;
            }
            catch
            {
                // ignore
            }
        }
        var act = System.Diagnostics.Activity.Current;
        if (act != null && act.OperationName == "snapshot.save")
        {
            _ = act.AddEvent(new System.Diagnostics.ActivityEvent("snapshot.prune"));
            _ = act.AddTag("prune.partition", partitionKey);
            _ = act.AddTag("prune.deleted", deleted);
            _ = act.AddTag("prune.kept", snapshotsToKeep);
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }

    private static bool TryParseSnapshotEntry(string path, out (FileInfo File, long Version, long Ticks) entry)
    {
        entry = default;
        // Exclude temp files quickly.
        if (path.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }
        if (!path.EndsWith(".snap", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }
        var name = Path.GetFileNameWithoutExtension(path);
        var parts = name.Split('_');
        if (parts.Length < 3)
        {
            return false;
        }
        if (!long.TryParse(parts[^2], out var version))
        {
            return false;
        }
        if (!long.TryParse(parts[^1], out var ticks))
        {
            return false;
        }
        entry = (new FileInfo(path), version, ticks);
        return true;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void TryFsyncDirectory(string dir)
    {
#if NET8_0_OR_GREATER
        try
        {
            using var handle = File.OpenHandle(dir, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.None);
            RandomAccess.FlushToDisk(handle);
        }
        catch
        {
            // ignore
        }
#endif
    }
}
