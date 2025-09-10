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
            fs = new FileStream(tmpPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 64 * 1024, FileOptions.WriteThrough);
            try
            {
                await data.CopyToAsync(fs, ct).ConfigureAwait(false);
                await fs.FlushAsync(ct).ConfigureAwait(false);
                fs.Flush(flushToDisk: true);
            }
            finally
            {
                // Ensure the handle is closed before attempting the atomic move.
                if (fs != null)
                {
                    await fs.DisposeAsync().ConfigureAwait(false);
                }
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
    public async ValueTask<(SnapshotMetadata Meta, Stream Data)?> TryLoadLatestAsync(string partitionKey, CancellationToken ct = default)
    {
        var dir = PartitionDir(_root, partitionKey);
        if (!Directory.Exists(dir))
        {
            return null;
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
            return null;
        }
        // Open, copy into memory, then dispose the file stream so callers don't need to manage file handle lifetime.
        await using var fs = new FileStream(bestPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        var ms = new MemoryStream(capacity: (int)Math.Min(fs.Length, int.MaxValue));
        await fs.CopyToAsync(ms, ct).ConfigureAwait(false);
        ms.Position = 0;
        return (bestMeta.Value, ms);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> ListPartitionKeysAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var dir in Directory.EnumerateDirectories(_root))
        {
            ct.ThrowIfCancellationRequested();
            yield return Path.GetFileName(dir);
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
        foreach (var path in Directory.EnumerateFiles(dir, "*", SearchOption.TopDirectoryOnly))
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
