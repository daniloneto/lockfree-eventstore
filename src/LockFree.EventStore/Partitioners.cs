using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Helpers for stable partition mapping.
/// </summary>
public static class Partitioners
{
    /// <summary>
    /// Maps a key to a partition index using a simple stable hash.
    /// <summary>
    /// Maps a key to a stable partition index in range [0, partitions - 1].
    /// </summary>
    /// <param name="key">The key to map to a partition.</param>
    /// <param name="partitions">The total number of partitions; must be greater than zero.</param>
    /// <returns>An integer partition index between 0 and <paramref name="partitions"/> - 1.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="partitions"/> is less than or equal to zero.</exception>
    public static int ForKey<TKey>(TKey key, int partitions)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitions);
        return (int)((uint)HashCode.Combine(key) % partitions);
    }

    /// <summary>
    /// Specialized overload for Event to avoid generic hashing. Uses KeyId directly.
    /// <summary>
    /// Maps the event's numeric key to a partition index in the range [0, partitions - 1].
    /// </summary>
    /// <param name="e">The event whose <c>Key.Value</c> is used to compute the partition.</param>
    /// <param name="partitions">The number of partitions; must be greater than zero.</param>
    /// <returns>The partition index for the event's key.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="partitions"/> is less than or equal to zero.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ForKey(Event e, int partitions)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitions);
        return PerformanceHelpers.IsPowerOfTwo(partitions)
            ? PerformanceHelpers.FastMod(e.Key.Value, partitions)
            : e.Key.Value % partitions;
    }

    /// <summary>
    /// Maps a KeyId to a partition index using optimized integer arithmetic.
    /// This is the hot path version that avoids string hashing.
    /// <summary>
    /// Maps a KeyId to a partition index in the range [0, partitions - 1].
    /// </summary>
    /// <param name="keyId">The KeyId whose numeric value will be used to determine the partition.</param>
    /// <param name="partitions">The number of partitions; must be greater than zero.</param>
    /// <returns>An integer partition index between 0 and <paramref name="partitions"/> - 1.</returns>
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
