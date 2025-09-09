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

    private static string TempFile(string dir, string fileName)
    {
        return Path.Combine(dir, fileName + ".tmp");
    }

    /// <inheritdoc />
    public async ValueTask SaveAsync(SnapshotMetadata meta, Stream data, CancellationToken ct = default)
    {
        var dir = PartitionDir(_root, meta.PartitionKey);
        _ = Directory.CreateDirectory(dir);
        var finalName = $"{meta.PartitionKey}_{meta.Version}_{meta.TakenAt.UtcTicks}.snap";
        var finalPath = Path.Combine(dir, finalName);
        var tmpPath = TempFile(dir, finalName);

        using (var fs = new FileStream(tmpPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 64 * 1024, FileOptions.WriteThrough))
        {
            await data.CopyToAsync(fs, ct).ConfigureAwait(false);
            await fs.FlushAsync(ct).ConfigureAwait(false);
            fs.Flush(flushToDisk: true);
        }
        // Attempt atomic move; on failure, ensure temp file is removed so future writes can succeed.
        try
        {
            File.Move(tmpPath, finalPath, overwrite: false);
        }
        catch
        {
            try
            {
                if (File.Exists(tmpPath))
                {
                    File.Delete(tmpPath);
                }
            }
            catch
            {
                // ignore cleanup failure
            }
            throw; // propagate original failure
        }
        if (_fsyncDir)
        {
            TryFsyncDirectory(dir);
        }
    }

    /// <inheritdoc />
    public ValueTask<(SnapshotMetadata Meta, Stream Data)?> TryLoadLatestAsync(string partitionKey, CancellationToken ct = default)
    {
        var dir = PartitionDir(_root, partitionKey);
        if (!Directory.Exists(dir))
        {
            return ValueTask.FromResult<(SnapshotMetadata Meta, Stream Data)?>(null);
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
            return ValueTask.FromResult<(SnapshotMetadata Meta, Stream Data)?>(null);
        }
        var stream = new FileStream(bestPath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var result = (bestMeta.Value, (Stream)stream);
        return ValueTask.FromResult<(SnapshotMetadata Meta, Stream Data)?>(result);
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
            if (path.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase))
            {
                continue; // ignore temp
            }
            if (!path.EndsWith(".snap", StringComparison.OrdinalIgnoreCase))
            {
                continue; // ignore other files
            }
            var name = Path.GetFileNameWithoutExtension(path);
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
            entries.Add((new FileInfo(path), version, ticks));
        }
        if (entries.Count <= snapshotsToKeep)
        {
            return;
        }
        var ordered = entries.OrderByDescending(e => e.Version).ThenByDescending(e => e.Ticks).ToList();
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
        // Trace pruning if there is an active snapshot Activity
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
