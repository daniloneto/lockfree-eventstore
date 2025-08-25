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
    /// </summary>
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
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void BoundsCheck(uint index, uint length)
    {
        if (index >= length)
        {
            ThrowArgumentOutOfRange(index, length);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowArgumentOutOfRange(uint index, uint length)
    {
        throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} out of range [0, {length}).");
    }

    /// <summary>
    /// Fast modulo operation for power-of-2 values.
    /// SAFETY: Only works correctly when divisor is a power of 2.
    /// </summary>
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
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long FastMod(long value, long powerOfTwoDivisor)
    {
        return !IsPowerOfTwo(powerOfTwoDivisor)
            ? throw new ArgumentOutOfRangeException(nameof(powerOfTwoDivisor), "Divisor must be a positive power of two.")
            : value & (powerOfTwoDivisor - 1);
    }

    /// <summary>
    /// Checks if a number is a power of 2 (int).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsPowerOfTwo(int value)
    {
        return value > 0 && (value & (value - 1)) == 0;
    }

    /// <summary>
    /// Checks if a number is a power of 2 (long).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsPowerOfTwo(long value)
    {
        return value > 0 && (value & (value - 1)) == 0;
    }

    /// <summary>
    /// Rounds up to the next power of 2.
    /// </summary>
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
