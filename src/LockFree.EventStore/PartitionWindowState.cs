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

    /// <summary>
    /// Merges another WindowAggregateState into this one, combining counts, sums, and min/max bounds.
    /// </summary>
    /// <param name="other">The other partition's aggregate state to incorporate.</param>
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
[method: MethodImpl(MethodImplOptions.AggressiveInlining)]
internal struct WindowRemoveState(long removedCount)
{
    public long RemovedCount = removedCount;
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

    /// <summary>
    /// Initializes the bucket to represent a new time window starting at the given tick and clears its aggregates.
    /// After calling this, <see cref="Count"/> is 0, <see cref="Sum"/> is 0.0, and <see cref="Min"/>/<see cref="Max"/>
    /// are set to extreme values to indicate an empty bucket.
    /// </summary>
    /// <param name="bucketStart">The starting tick (inclusive) for the bucket.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Reset(long bucketStart)
    {
        StartTicks = bucketStart;
        Count = 0;
        Sum = 0.0;
        Min = double.MaxValue;
        Max = double.MinValue;
    }

    /// <summary>
    /// Incorporates a sample into the bucket's running aggregates.
    /// </summary>
    /// <param name="value">The numeric sample to add; increments Count, adds to Sum, and updates Min/Max as needed.</param>
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
    public int BucketHead; // index of the bucket that contains WindowStartTicks (floor-aligned to BucketWidthTicks)
    public long BucketWidthTicks;

    /// <summary>
    /// Reset all partition window state to its initial defaults.
    /// </summary>
    /// <remarks>
    /// Clears the logical window (start/end ticks, head index) and aggregation counters (count, sum).
    /// Sets the running minimum to <see cref="double.MaxValue"/> and maximum to <see cref="double.MinValue"/> as sentinels,
    /// and clears any bucket ring by setting <see cref="Buckets"/> to <c>null</c> and bucket-related fields to zero.
    /// </remarks>
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

    /// <summary>
    /// Adds a value to the running window aggregate, incrementing Count, adding to Sum,
    /// and updating Min and Max as needed.
    /// </summary>
    /// <param name="value">The sample value to include in the aggregate.</param>
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

    /// <summary>
    /// Removes a single observation from the running window aggregate.
    /// </summary>
    /// <param name="value">The value to remove; this decrements <see cref="Count"/> and subtracts from <see cref="Sum"/>.</param>
    /// <remarks>
    /// This method does not recompute Min/Max immediately — min/max recalculation is deferred and handled when buckets roll.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RemoveValue(double value)
    {
        Count--;
        Sum -= value;
        // Min/Max recomputation is handled when buckets roll
    }

    public readonly double Average => Count > 0 ? Sum / Count : 0.0;
}
