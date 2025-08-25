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
    /// <summary>
    /// Rents an <see cref="Event"/>[] from the shared event array pool with at least the requested length.
    /// </summary>
    /// <param name="minimumSize">The minimum required length of the rented array; the returned array may be larger.</param>
    /// <returns>A rented <see cref="Event"/>[] from the shared pool. The caller must return it to the pool when finished.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Event[] RentEvents(int minimumSize)
    {
        return EventPool.Rent(minimumSize);
    }

    /// <summary>
    /// Returns an Event array to the pool with optional clearing.
    /// <summary>
    /// Returns an Event array to the shared EventPool if it is not null.
    /// </summary>
    /// <param name="array">The rented Event array to return; if null the method is a no-op.</param>
    /// <param name="clearArray">If true, the array is cleared before being returned to the pool.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReturnEvents(Event[] array, bool clearArray = false)
    {
        if (array != null)
        {
            EventPool.Return(array, clearArray);
        }
    }

    /// <summary>
    /// Rents an int array from the pool with proper size handling.
    /// <summary>
    /// Rents an int array from the shared integer pool with at least the requested length.
    /// </summary>
    /// <param name="minimumSize">Minimum required length of the rented array.</param>
    /// <returns>
    /// An int[] with length >= <paramref name="minimumSize"/>. The caller is responsible for returning the array to the pool when finished.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int[] RentInts(int minimumSize)
    {
        return IntPool.Rent(minimumSize);
    }

    /// <summary>
    /// Returns an int array to the pool with optional clearing.
    /// <summary>
    /// Returns an rented int array to the shared IntPool.
    /// </summary>
    /// <param name="array">The int array previously rented from the pool. If null, the call is a no-op.</param>
    /// <param name="clearArray">If true, the pool will clear the array's contents before storing it.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReturnInts(int[] array, bool clearArray = false)
    {
        if (array != null)
        {
            IntPool.Return(array, clearArray);
        }
    }

    /// <summary>
    /// Rents a double array from the pool with proper size handling.
    /// <summary>
    /// Rents a double[] from the shared double array pool with at least the specified minimum length.
    /// </summary>
    /// <param name="minimumSize">The minimum required length of the rented array. The returned array may be larger.</param>
    /// <returns>A double[] whose length is >= <paramref name="minimumSize"/>. Must be returned to the pool (e.g., via <see cref="ReturnDoubles(double[], bool)"/>) when no longer needed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double[] RentDoubles(int minimumSize)
    {
        return DoublePool.Rent(minimumSize);
    }

    /// <summary>
    /// Returns a double array to the pool with optional clearing.
    /// <summary>
    /// Returns a rented <see cref="double"/>[] to the shared DoublePool. If <paramref name="array"/> is null this is a no-op.
    /// </summary>
    /// <param name="array">The array previously rented from <see cref="DoublePool"/> to return; may be null.</param>
    /// <param name="clearArray">If true, the array will be cleared before being returned to the pool.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReturnDoubles(double[] array, bool clearArray = false)
    {
        if (array != null)
        {
            DoublePool.Return(array, clearArray);
        }
    }

    /// <summary>
    /// Rents a long array from the pool with proper size handling.
    /// <summary>
    /// Rents a <see cref="long"/>[] from the shared long array pool with at least the specified length.
    /// </summary>
    /// <param name="minimumSize">The minimum required length of the returned array.</param>
    /// <returns>An array of <see cref="long"/> with length &gt;= <paramref name="minimumSize"/>. The caller should return the array to the pool (e.g., via <see cref="ReturnLongs(long[], bool)"/>) when finished.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long[] RentLongs(int minimumSize)
    {
        return LongPool.Rent(minimumSize);
    }

    /// <summary>
    /// Returns a long array to the pool with optional clearing.
    /// <summary>
    /// Returns a rented long[] to the shared LongPool if it is non-null.
    /// </summary>
    /// <param name="array">The buffer to return; null values are ignored.</param>
    /// <param name="clearArray">If true, the array is cleared before being returned to the pool.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReturnLongs(long[] array, bool clearArray = false)
    {
        if (array != null)
        {
            LongPool.Return(array, clearArray);
        }
    }

    /// <summary>
    /// Helper to safely rent and process data in chunks, automatically handling return to pool.
    /// <summary>
    /// Splits <paramref name="data"/> into sequential slices of up to <paramref name="chunkSize"/>
    /// elements and invokes <paramref name="processor"/> for each slice.
    /// </summary>
    /// <param name="data">The input span to process in chunks.</param>
    /// <param name="processor">Action invoked for each chunk slice.</param>
    /// <param name="chunkSize">Maximum number of elements per chunk. Must be greater than zero.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="chunkSize"/> is zero or negative.</exception>
    public static void ProcessInChunks<T>(ReadOnlySpan<T> data, Action<ReadOnlySpan<T>> processor, int chunkSize = DefaultChunkSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);
        for (var i = 0; i < data.Length; i += chunkSize)
        {
            var chunk = data.Slice(i, Math.Min(chunkSize, data.Length - i));
            processor(chunk);
        }
    }

    /// <summary>
    /// Helper to safely rent a buffer, process data, and return to pool with automatic disposal.
    /// <summary>
    /// Rent a buffer from the given pool, invoke <paramref name="processor"/> with it, and return the buffer to the pool.
    /// </summary>
    /// <typeparam name="T">Element type of the rented buffer.</typeparam>
    /// <typeparam name="TResult">Return type of the processor function.</typeparam>
    /// <param name="minimumSize">Minimum required length of the rented buffer.</param>
    /// <param name="processor">Function that receives the rented buffer and produces a result.</param>
    /// <returns>The value returned by <paramref name="processor"/>.</returns>
    /// <remarks>
    /// The rented buffer's contents are unspecified (may contain data from previous renters) and the pool may supply an array larger than <paramref name="minimumSize"/>.
    /// The buffer is always returned to <paramref name="pool"/>, even if <paramref name="processor"/> throws.
    /// </remarks>
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
