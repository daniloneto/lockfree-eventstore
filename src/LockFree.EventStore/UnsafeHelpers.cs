using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Internal helpers for high-performance operations.
/// These methods are optimized for performance in critical paths.
/// </summary>
internal static class PerformanceHelpers
{
    /// <summary>
    /// Fast memory copy for spans of types.
    /// Uses the built-in optimized copy operations.
    /// <summary>
    /// Copies elements from a source <see cref="ReadOnlySpan{T}"/> into a destination <see cref="Span{T}"/> of the same length.
    /// </summary>
    /// <param name="source">The source span.</param>
    /// <param name="destination">The destination span; must have the same length as <paramref name="source"/>.</param>
    /// <exception cref="System.ArgumentException">Thrown when <paramref name="source"/> and <paramref name="destination"/> have different lengths.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void FastCopySpan<T>(ReadOnlySpan<T> source, Span<T> destination)
    {
        if (source.Length != destination.Length)
        {
            throw new ArgumentException("Source and destination spans must have the same length");
        }

        if (source.IsEmpty)
        {
            return;
        }

        // Use built-in optimized copy
        source.CopyTo(destination);
    }

    /// <summary>
    /// Performs a volatile read of a reference type field.
    /// Ensures proper memory ordering.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static T VolatileRead<T>(ref T location) where T : class?
    {
        return Volatile.Read(ref location);
    }

    /// <summary>
    /// Performs a volatile write of a reference type field.
    /// Ensures proper memory ordering.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void VolatileWrite<T>(ref T location, T value) where T : class?
    {
        Volatile.Write(ref location, value);
    }

    /// <summary>
    /// Fast bounds check that the JIT can better optimize.
    /// <summary>
    /// Ensures a zero-based index is within the valid range [0, length).
    /// </summary>
    /// <param name="index">The zero-based index to validate.</param>
    /// <param name="length">The exclusive upper bound of the valid range.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="index"/> is greater than or equal to <paramref name="length"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void BoundsCheck(uint index, uint length)
    {
        if (index >= length)
        {
            ThrowArgumentOutOfRange(index, length);
        }
    }

    /// <summary>
    /// Throws an <see cref="ArgumentOutOfRangeException"/> for an index that is outside the valid range [0, length).
    /// </summary>
    /// <param name="index">The index that was out of range.</param>
    /// <param name="length">The upper bound (exclusive) of the valid range.</param>
    /// <exception cref="ArgumentOutOfRangeException">Always thrown with a message indicating the invalid <paramref name="index"/> and the valid range.</exception>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowArgumentOutOfRange(uint index, uint length)
    {
        throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} out of range [0, {length}).");
    }

    /// <summary>
    /// Fast modulo operation for power-of-2 values.
    /// SAFETY: Only works correctly when divisor is a power of 2.
    /// <summary>
    /// Computes value modulo <paramref name="powerOfTwoDivisor"/> using a fast bitmask, where the divisor must be a positive power of two.
    /// </summary>
    /// <param name="value">The dividend.</param>
    /// <param name="powerOfTwoDivisor">A positive power-of-two divisor.</param>
    /// <returns>The remainder of <paramref name="value"/> modulo <paramref name="powerOfTwoDivisor"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="powerOfTwoDivisor"/> is not a positive power of two.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int FastMod(int value, int powerOfTwoDivisor)
    {
        return !IsPowerOfTwo(powerOfTwoDivisor)
            ? throw new ArgumentOutOfRangeException(nameof(powerOfTwoDivisor), "Divisor must be a positive power of two.")
            : value & (powerOfTwoDivisor - 1);
    }

    /// <summary>
    /// Fast modulo operation for power-of-2 values (long version).
    /// SAFETY: Only works correctly when divisor is a power of 2.
    /// <summary>
    /// Computes the remainder of <paramref name="value"/> divided by <paramref name="powerOfTwoDivisor"/> using a fast bitwise mask.
    /// </summary>
    /// <param name="value">The dividend.</param>
    /// <param name="powerOfTwoDivisor">
    /// The divisor, which must be a positive power of two. For example: 1, 2, 4, 8, ...
    /// </param>
    /// <returns>
    /// The remainder equal to <c>value & (powerOfTwoDivisor - 1)</c>; when <paramref name="powerOfTwoDivisor"/> is a power of two this is equivalent to <c>value % powerOfTwoDivisor</c>.
    /// </returns>
    /// <exception cref="System.ArgumentOutOfRangeException">Thrown when <paramref name="powerOfTwoDivisor"/> is not a positive power of two.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long FastMod(long value, long powerOfTwoDivisor)
    {
        return !IsPowerOfTwo(powerOfTwoDivisor)
            ? throw new ArgumentOutOfRangeException(nameof(powerOfTwoDivisor), "Divisor must be a positive power of two.")
            : value & (powerOfTwoDivisor - 1);
    }

    /// <summary>
    /// Checks if a number is a power of 2 (int).
    /// <summary>
    /// Determines whether the specified integer is a positive power of two.
    /// </summary>
    /// <param name="value">The integer to test.</param>
    /// <returns><c>true</c> if <paramref name="value"/> is greater than zero and a power of two; otherwise <c>false</c>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsPowerOfTwo(int value)
    {
        return value > 0 && (value & (value - 1)) == 0;
    }

    /// <summary>
    /// Checks if a number is a power of 2 (long).
    /// <summary>
    /// Determines whether the specified 64-bit integer is a positive power of two.
    /// </summary>
    /// <param name="value">The value to test.</param>
    /// <returns>
    /// true if <paramref name="value"/> is greater than zero and exactly a power of two; otherwise, false.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsPowerOfTwo(long value)
    {
        return value > 0 && (value & (value - 1)) == 0;
    }

    /// <summary>
    /// Rounds up to the next power of 2.
    /// <summary>
    /// Rounds the given positive integer up to the nearest power of two.
    /// </summary>
    /// <param name="value">The value to round. If &lt;= 1, the method returns 1.</param>
    /// <returns>The smallest power of two greater than or equal to <paramref name="value"/>.</returns>
    /// <remarks>
    /// For inputs greater than 2^30 (1_073_741_824) the result may overflow the signed 32-bit range and produce a negative value; callers should ensure <paramref name="value"/> is within an appropriate range when overflow is not acceptable.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int RoundUpToPowerOfTwo(int value)
    {
        if (value <= 1)
        {
            return 1;
        }

        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        value++;

        return value;
    }
}
