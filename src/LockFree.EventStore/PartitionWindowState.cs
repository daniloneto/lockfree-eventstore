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
                if (other.Min < Min)
                {
                    Min = other.Min;
                }
                if (other.Max > Max)
                {
                    Max = other.Max;
                }
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public WindowRemoveState(long removedCount)
    {
        RemovedCount = removedCount;
    }
}

/// <summary>
/// Per-partition bucket used for O(1) evict/apply window aggregations.
/// </summary>
internal struct AggregateBucket
{
    public long StartTicks;
    public int Count;
    public double Sum;
    public double Min;
    public double Max;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Reset(long bucketStart)
    {
        StartTicks = bucketStart;
        Count = 0;
        Sum = 0.0;
        Min = double.MaxValue;
        Max = double.MinValue;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(double value)
    {
        Count++;
        Sum += value;
        if (value < Min)
        {
            Min = value;
        }
        if (value > Max)
        {
            Max = value;
        }
    }
}

/// <summary>
/// Internal state for incremental window aggregation per partition, with bucket ring.
/// </summary>
internal struct PartitionWindowState
{
    public long WindowStartTicks;
    public long WindowEndTicks;

    // Aggregated state for current window (sum of buckets within window)
    public long Count;
    public double Sum;
    public double Min;
    public double Max;

    // Logical pointer to the start position of the current window in the ring buffer (events). Kept for compatibility.
    public int WindowHeadIndex;

    // Bucket ring for O(1) evict/apply
    public AggregateBucket[]? Buckets;
    public int BucketHead; // index of the bucket that starts at WindowStartTicks
    public long BucketWidthTicks;

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
        Buckets = null;
        BucketHead = 0;
        BucketWidthTicks = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddValue(double value)
    {
        Count++;
        Sum += value;
        if (value < Min)
        {
            Min = value;
        }
        if (value > Max)
        {
            Max = value;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RemoveValue(double value)
    {
        Count--;
        Sum -= value;
        // Min/Max recomputation is handled when buckets roll
    }

    public readonly double Average => Count > 0 ? Sum / Count : 0.0;
}
