using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using LockFree.EventStore;
using LockFree.EventStore.Snapshots;
using Xunit;
using LockFree.EventStore.Tests.TestDoubles; // added
using System.Threading; // for retry sleep

namespace LockFree.EventStore.Tests;

public class SnapshotsMoreEdgeTests
{
    // Custom stream that throws an IOException after a threshold of bytes read (to cover BinarySnapshotSerializer event loop IOException catch path)
    private sealed class ThrowingAfterReadStream : Stream
    {
        private readonly Stream _inner;
        private readonly long _threshold;
        private long _read;
        public ThrowingAfterReadStream(Stream inner, long threshold)
        {
            _inner = inner; _threshold = threshold;
        }
        public override bool CanRead => true;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => false;
        public override long Length => _inner.Length;
        public override long Position { get => _inner.Position; set => _inner.Position = value; }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_read >= _threshold) throw new IOException("Injected I/O failure");
            var toRead = (int)Math.Min(count, _threshold - _read);
            var n = _inner.Read(buffer, offset, toRead);
            _read += n;
            if (n == 0 && _read < _threshold) return 0;
            return n;
        }
        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    // Helper to robustly delete directories on Windows when recently closed handles may still block deletion briefly.
    private static void DeleteDirectoryRobust(string path)
    {
        for (int i = 0; i < 5; i++)
        {
            try
            {
                if (!Directory.Exists(path)) return;
                Directory.Delete(path, true);
                return;
            }
            catch (IOException) when (i < 4)
            {
                Thread.Sleep(50);
            }
            catch (UnauthorizedAccessException) when (i < 4)
            {
                Thread.Sleep(50);
            }
        }
        // Final attempt without swallowing to surface persistent issues
        if (Directory.Exists(path)) Directory.Delete(path, true);
    }

    [Fact]
    public void BinarySnapshotSerializer_EventLoop_IOException_Path()
    {
        // Build a valid header with 1 event but inject IOException when reading event value (double)
        using var header = new MemoryStream();
        using (var bw = new BinaryWriter(header, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            bw.Write(1);          // schema
            bw.Write("p");       // partition key
            bw.Write(123L);       // version
            bw.Write(DateTimeOffset.UtcNow.UtcTicks); // takenAt
            bw.Write(1);          // count
            bw.Write(7);          // event key (int)
            // Do NOT write the rest (value + timestamp) so we can trigger IOException via throwing stream instead of EndOfStream
        }
        header.Position = 0;
        // Compute threshold so that reading stops right after the key (schema + key + etc.). Binary format: we counted bytes until just after key.
        var threshold = header.Length; // allow reading all existing bytes, then any further read triggers IOException
        using var throwing = new ThrowingAfterReadStream(header, threshold);
        var ser = new BinarySnapshotSerializer();
        Assert.Throws<SnapshotDeserializationException>(() => ser.DeserializeAsync(throwing).AsTask().GetAwaiter().GetResult());
    }

    [Fact]
    public void ExponentialBackoffPolicy_Monotonic_Bump_On_Repeated_Attempt()
    {
        var policy = new ExponentialBackoffPolicy(TimeSpan.FromMilliseconds(5), 2);
        var prev = TimeSpan.Zero;
        for (int i = 0; i < 10; i++)
        {
            var d = policy.NextDelay(1); // same attempt triggers jitter + monotonic bump logic
            Assert.True(d > prev, $"Delay not strictly increasing: {prev} -> {d}");
            prev = d;
        }
    }

    [Fact]
    public async Task FileSystemSnapshotStore_TieBreak_SameVersion_By_Timestamp()
    {
        var root = Path.Combine(Path.GetTempPath(), "snap_tiebreak_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var fs = new FileSystemSnapshotStore(root);
            var p = "t";
            var baseTicks = DateTimeOffset.UtcNow.UtcTicks;
            var older = new SnapshotMetadata(p, 10, new DateTimeOffset(baseTicks, TimeSpan.Zero), 1);
            var newer = new SnapshotMetadata(p, 10, new DateTimeOffset(baseTicks + 500, TimeSpan.Zero), 1);
            using (var ms = new MemoryStream(new byte[]{1})) await fs.SaveAsync(older, ms);
            using (var ms2 = new MemoryStream(new byte[]{2})) await fs.SaveAsync(newer, ms2);
            var latest = await fs.TryLoadLatestAsync(p);
            if (latest.HasValue)
            {
                using (latest.Value.Data) // ensure deterministic disposal
                {
                    Assert.Equal(newer.TakenAt.UtcTicks, latest.Value.Meta.TakenAt.UtcTicks);
                }
            }
            else
            {
                Assert.True(false, "No latest snapshot returned");
            }
        }
        finally
        {
            try { if (Directory.Exists(root)) Directory.Delete(root, true); } catch { /* ignore cleanup issues */ }
        }
    }

    [Fact]
    public async Task FileSystemSnapshotStore_ListPartitionKeys_Enumerates_All()
    {
        var root = Path.Combine(Path.GetTempPath(), "snap_listparts_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var fs = new FileSystemSnapshotStore(root);
            foreach (var part in new[]{"a","b"})
            {
                var meta = new SnapshotMetadata(part, 1, DateTimeOffset.UtcNow, 1);
                using var ms = new MemoryStream(new byte[]{1});
                await fs.SaveAsync(meta, ms);
            }
            var parts = new List<string>();
            await foreach (var k in fs.ListPartitionKeysAsync()) parts.Add(k);
            Assert.Contains("a", parts);
            Assert.Contains("b", parts);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [Fact]
    public async Task FileSystemSnapshotStore_FsyncDirectory_Path()
    {
        var root = Path.Combine(Path.GetTempPath(), "snap_fsync_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var fs = new FileSystemSnapshotStore(root, fsyncDirectory: true);
            var meta = new SnapshotMetadata("p", 1, DateTimeOffset.UtcNow, 1);
            using var ms = new MemoryStream(new byte[]{1,2,3});
            await fs.SaveAsync(meta, ms); // should not throw
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [Fact]
    public void SnapshotValidation_NonPositiveExpectedSchemaVersion_Throws()
    {
        var opts = new SnapshotOptions { Enabled = true, Interval = TimeSpan.FromMilliseconds(10), MinEventsBetweenSnapshots = 0, ExpectedSchemaVersion = 0 };
        Assert.Throws<ArgumentOutOfRangeException>(() => SnapshotValidation.ValidateSnapshotOptions(opts, new InMemoryJsonSnapshotSerializer(), new InMemorySnapshotStore()));
    }
}
