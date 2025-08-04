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
        if (partitions <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitions));
        return (int)((uint)HashCode.Combine(key) % partitions);
    }    /// <summary>
    /// Maps a KeyId to a partition index using optimized integer arithmetic.
    /// This is the hot path version that avoids string hashing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ForKeyId(KeyId keyId, int partitions)
    {
        if (partitions <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitions));
        
        // Use the KeyId value directly with fast modulo
        // Since KeyId values are sequential, this provides good distribution
        return (int)PerformanceHelpers.FastMod((uint)keyId.Value, (uint)partitions);
    }

    /// <summary>
    /// Maps a KeyId to a partition index using simple modulo.
    /// Fallback when FastMod is not available or partitions is not power of 2.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ForKeyIdSimple(KeyId keyId, int partitions)
    {
        if (partitions <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitions));
        return keyId.Value % partitions;
    }
}
