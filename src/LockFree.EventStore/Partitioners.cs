using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Helpers for stable partition mapping.
/// </summary>
public static class Partitioners
{
    /// <summary>
    /// Maps a key to a partition index using a simple stable hash.
    /// </summary>
    public static int ForKey<TKey>(TKey key, int partitions)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitions);
        return (int)((uint)HashCode.Combine(key) % partitions);
    }

    /// <summary>
    /// Maps a KeyId to a partition index using optimized integer arithmetic.
    /// This is the hot path version that avoids string hashing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ForKeyId(KeyId keyId, int partitions)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitions);

        // Use fast bitmask when partitions is a power of two; otherwise fallback to modulo
        return PerformanceHelpers.IsPowerOfTwo(partitions) ? PerformanceHelpers.FastMod(keyId.Value, partitions) : keyId.Value % partitions;
    }

    /// <summary>
    /// Maps a KeyId to a partition index using simple modulo.
    /// Fallback when FastMod is not available or partitions is not power of 2.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ForKeyIdSimple(KeyId keyId, int partitions)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitions);
        return keyId.Value % partitions;
    }
}
