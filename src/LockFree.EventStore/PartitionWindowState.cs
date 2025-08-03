using System.Runtime.CompilerServices;
using System.Numerics;

namespace LockFree.EventStore;

/// <summary>
/// Represents the result of a window aggregation operation.
/// </summary>
public readonly record struct WindowAggregateResult(
    long Count, 
    double Sum, 
    double Min, 
    double Max, 
    double Avg);

/// <summary>
/// Internal state for window aggregation operations.
/// </summary>
internal struct WindowAggregateState
{
    public long Count;
    public double Sum; 
    public double Min;
    public double Max;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Merge(WindowAggregateState other)
    {
        Count += other.Count;
        Sum += other.Sum;
        if (other.Count > 0)
        {
            if (Count == other.Count) // First partition
            {
                Min = other.Min;
                Max = other.Max;
            }
            else
            {
                if (other.Min < Min) Min = other.Min;
                if (other.Max > Max) Max = other.Max;
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly WindowAggregateResult ToResult()
    {
        return new WindowAggregateResult(
            Count,
            Sum,
            Count > 0 ? Min : 0.0,
            Count > 0 ? Max : 0.0,
            Count > 0 ? Sum / Count : 0.0);
    }
}

/// <summary>
/// Internal state for sum aggregation operations.
/// </summary>
internal struct SumAggregateState<TResult>
    where TResult : struct, INumber<TResult>
{
    public TResult Sum;
}

/// <summary>
/// Internal state for tracking removed items during window advancement.
/// </summary>
internal struct WindowRemoveState
{
    public long RemovedCount;
    
    // Constructor to suppress warning about never assigned field
    public WindowRemoveState(long removedCount = 0)
    {
        RemovedCount = removedCount;
    }
}

/// <summary>
/// Internal state for incremental window aggregation per partition.
/// Maintains sliding window boundaries and aggregate values.
/// </summary>
internal struct PartitionWindowState
{
    /// <summary>
    /// Lower bound (inclusive) of the current window in ticks.
    /// </summary>
    public long WindowStartTicks;
    
    /// <summary>
    /// Upper bound of the current window in ticks.
    /// </summary>
    public long WindowEndTicks;
    
    /// <summary>
    /// Current count of events in the window.
    /// </summary>
    public long Count;
    
    /// <summary>
    /// Sum of values in the current window.
    /// </summary>
    public double Sum;
    
    /// <summary>
    /// Minimum value in the current window.
    /// </summary>
    public double Min;
    
    /// <summary>
    /// Maximum value in the current window.
    /// </summary>
    public double Max;
    
    /// <summary>
    /// Logical pointer to the start position of the current window in the ring buffer.
    /// </summary>
    public int WindowHeadIndex;

    /// <summary>
    /// Resets the window state to initial values.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Reset()
    {
        WindowStartTicks = 0;
        WindowEndTicks = 0;
        Count = 0;
        Sum = 0.0;
        Min = double.MaxValue;
        Max = double.MinValue;
        WindowHeadIndex = 0;
    }

    /// <summary>
    /// Updates the aggregate with a new value.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddValue(double value)
    {
        Count++;
        Sum += value;
        if (value < Min) Min = value;
        if (value > Max) Max = value;
    }

    /// <summary>
    /// Removes a value from the aggregate.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RemoveValue(double value)
    {
        Count--;
        Sum -= value;
        // Note: Min/Max recalculation will be handled during window advance
    }

    /// <summary>
    /// Gets the average value in the window.
    /// </summary>
    public readonly double Average => Count > 0 ? Sum / Count : 0.0;
}
