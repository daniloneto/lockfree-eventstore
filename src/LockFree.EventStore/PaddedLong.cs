using System.Runtime.InteropServices;

namespace LockFree.EventStore;

/// <summary>
/// A long value padded to a full cache line to reduce false sharing under contention.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 128)]
public struct PaddedLong
{
    /// <summary>
    /// The underlying 64-bit integer value.
    /// </summary>
    [FieldOffset(0)]
    public long Value;
}
