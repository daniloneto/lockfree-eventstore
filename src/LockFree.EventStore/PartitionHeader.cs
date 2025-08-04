using System.Runtime.InteropServices;

namespace LockFree.EventStore;

/// <summary>
/// Padded partition header to prevent false sharing between partition metadata.
/// Aligned to 64-byte cache line boundaries for optimal MPMC performance.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct PartitionHeader
{
    /// <summary>
    /// Head position in the ring buffer (read position)
    /// </summary>
    public long Head;
    
    /// <summary>
    /// Tail position in the ring buffer (write position)
    /// </summary>
    public long Tail;
    
    /// <summary>
    /// Approximate count of items in the partition
    /// </summary>
    public long Count;
      /// <summary>
    /// Epoch counter for consistency checks
    /// </summary>
    public int Epoch;
    
    /// <summary>
    /// Capacity of this partition
    /// </summary>
    public readonly int Capacity;
    
    /// <summary>
    /// Reserved for future use
    /// </summary>
    private readonly int _reserved1;
    
    // Padding to fill 64-byte cache line (64 bytes total)
    // Current used: 8 + 8 + 8 + 4 + 4 + 4 = 36 bytes
    // Padding needed: 64 - 36 = 28 bytes = 3.5 * 8-byte longs
    private readonly long _pad1;
    private readonly long _pad2;
    private readonly long _pad3;
    private readonly int _pad4;
    
    /// <summary>
    /// Initializes a new partition header with the specified capacity.
    /// </summary>
    public PartitionHeader(int capacity)
    {
        Head = 0;
        Tail = 0;
        Count = 0;
        Epoch = 0;
        Capacity = capacity;
        _reserved1 = 0;
        _pad1 = 0;
        _pad2 = 0;
        _pad3 = 0;
        _pad4 = 0;
    }
    
    /// <summary>
    /// Atomically increments the epoch counter.
    /// </summary>
    public void IncrementEpoch()
    {
        Interlocked.Increment(ref Epoch);
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
