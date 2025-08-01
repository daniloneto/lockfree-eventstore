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
    }
}
