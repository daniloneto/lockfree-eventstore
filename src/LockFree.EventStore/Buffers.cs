using System.Buffers;
using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Centralizes buffer pooling to reduce allocations in hot paths.
/// Uses ArrayPool&lt;T&gt; for reusable temporary arrays with chunking support.
/// </summary>
internal static class Buffers
{
    /// <summary>
    /// Pool for Event arrays - used primarily in SpecializedEventStore operations
    /// </summary>
    public static readonly ArrayPool<Event> EventPool = ArrayPool<Event>.Shared;
    
    /// <summary>
    /// Pool for int arrays - used for indexes, partition mappings, etc.
    /// </summary>
    public static readonly ArrayPool<int> IntPool = ArrayPool<int>.Shared;
    
    /// <summary>
    /// Pool for double arrays - used for numeric aggregations
    /// </summary>
    public static readonly ArrayPool<double> DoublePool = ArrayPool<double>.Shared;
    
    /// <summary>
    /// Pool for long arrays - used for timestamp operations
    /// </summary>
    public static readonly ArrayPool<long> LongPool = ArrayPool<long>.Shared;
    
    /// <summary>
    /// Standard chunk size for processing operations - balances memory usage and performance
    /// </summary>
    public const int DefaultChunkSize = 4096;
    
    /// <summary>
    /// Rents an Event array from the pool with proper size handling.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Event[] RentEvents(int minimumSize)
    {
        return EventPool.Rent(minimumSize);
    }
    
    /// <summary>
    /// Returns an Event array to the pool with optional clearing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReturnEvents(Event[] array, bool clearArray = false)
    {
        if (array != null)
            EventPool.Return(array, clearArray);
    }
    
    /// <summary>
    /// Rents an int array from the pool with proper size handling.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int[] RentInts(int minimumSize)
    {
        return IntPool.Rent(minimumSize);
    }
    
    /// <summary>
    /// Returns an int array to the pool with optional clearing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReturnInts(int[] array, bool clearArray = false)
    {
        if (array != null)
            IntPool.Return(array, clearArray);
    }
    
    /// <summary>
    /// Rents a double array from the pool with proper size handling.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double[] RentDoubles(int minimumSize)
    {
        return DoublePool.Rent(minimumSize);
    }
    
    /// <summary>
    /// Returns a double array to the pool with optional clearing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReturnDoubles(double[] array, bool clearArray = false)
    {
        if (array != null)
            DoublePool.Return(array, clearArray);
    }
    
    /// <summary>
    /// Rents a long array from the pool with proper size handling.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long[] RentLongs(int minimumSize)
    {
        return LongPool.Rent(minimumSize);
    }
    
    /// <summary>
    /// Returns a long array to the pool with optional clearing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReturnLongs(long[] array, bool clearArray = false)
    {
        if (array != null)
            LongPool.Return(array, clearArray);
    }
    
    /// <summary>
    /// Helper to safely rent and process data in chunks, automatically handling return to pool.
    /// </summary>
    public static void ProcessInChunks<T>(ReadOnlySpan<T> data, Action<ReadOnlySpan<T>> processor, int chunkSize = DefaultChunkSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);
        for (int i = 0; i < data.Length; i += chunkSize)
        {
            var chunk = data.Slice(i, Math.Min(chunkSize, data.Length - i));
            processor(chunk);
        }
    }
    
    /// <summary>
    /// Helper to safely rent a buffer, process data, and return to pool with automatic disposal.
    /// </summary>
    public static TResult WithRentedBuffer<T, TResult>(int minimumSize, Func<T[], TResult> processor, ArrayPool<T> pool)
    {
        var buffer = pool.Rent(minimumSize);
        try
        {
            return processor(buffer);
        }
        finally
        {
            pool.Return(buffer, clearArray: false);
        }
    }
    
    /// <summary>
    /// Helper to safely rent a buffer, process data, and return to pool with automatic disposal (void version).
    /// </summary>
    public static void WithRentedBuffer<T>(int minimumSize, Action<T[]> processor, ArrayPool<T> pool)
    {
        var buffer = pool.Rent(minimumSize);
        try
        {
            processor(buffer);
        }
        finally
        {
            pool.Return(buffer, clearArray: false);
        }
    }
}
