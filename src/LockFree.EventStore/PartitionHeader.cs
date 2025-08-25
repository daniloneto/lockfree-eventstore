using System.Runtime.InteropServices;

namespace LockFree.EventStore;

/// <summary>
/// Padded partition header to prevent false sharing between partition metadata.
/// Aligned to 64-byte cache line boundaries for optimal MPMC performance.
/// </summary>
/// <remarks>
/// Initializes a new partition header with the specified capacity.
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
internal struct PartitionHeader(int capacity)
{
    /// <summary>
    /// Head position in the ring buffer (read position)
    /// </summary>
    public long Head = 0;

    /// <summary>
    /// Tail position in the ring buffer (write position)
    /// </summary>
    public long Tail = 0;

    /// <summary>
    /// Approximate count of items in the partition
    /// </summary>
    public long Count = 0;
    /// <summary>
    /// Epoch counter for consistency checks
    /// </summary>
    public int Epoch = 0;

    /// <summary>
    /// Capacity of this partition
    /// </summary>
    public readonly int Capacity = capacity;

    /// <summary>
    /// Reserved for future use
    /// </summary>
    private readonly int _reserved1 = 0;

    // Padding to fill 64-byte cache line (64 bytes total)
    // Current used: 8 + 8 + 8 + 4 + 4 + 4 = 36 bytes
    // Padding needed: 64 - 36 = 28 bytes = 3.5 * 8-byte longs
    private readonly long _pad1 = 0;
    private readonly long _pad2 = 0;
    private readonly long _pad3 = 0;
    private readonly int _pad4 = 0;

    /// <summary>
    /// Atomically increments the epoch counter.
    /// </summary>
    public void IncrementEpoch()
    {
        _ = Interlocked.Increment(ref Epoch);
    }

    /// <summary>
    /// Gets the current approximate count of items.
    /// </summary>
    public long GetApproximateCount()
    {
        var head = Volatile.Read(ref Head);
        var tail = Volatile.Read(ref Tail);
        return Math.Max(0, Math.Min(Capacity, tail - head));
    }

    /// <summary>
    /// Updates the count based on current head and tail positions.
    /// </summary>
    public void UpdateCount()
    {
        var newCount = GetApproximateCount();
        Volatile.Write(ref Count, newCount);
    }
}
