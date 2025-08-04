using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Represents a view of a partition's data without materializing/copying events.
/// Provides zero-allocation access to partition contents through ReadOnlyMemory segments.
/// </summary>
/// <typeparam name="T">The event type.</typeparam>
public readonly record struct PartitionView<T>(
    ReadOnlyMemory<T> Segment1,
    ReadOnlyMemory<T> Segment2, // for wrap-around case; may be empty
    int Count, 
    long FromTicks, 
    long ToTicks)
{
    /// <summary>
    /// Whether this view has no events.
    /// </summary>
    public bool IsEmpty => Count == 0;

    /// <summary>
    /// Whether this view spans multiple segments (wrap-around case).
    /// </summary>
    public bool HasWrapAround => !Segment2.IsEmpty;

    /// <summary>
    /// Enumerates all events in this partition view efficiently without allocation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PartitionViewEnumerator<T> GetEnumerator()
    {
        return new PartitionViewEnumerator<T>(Segment1.Span, Segment2.Span);
    }

    /// <summary>
    /// Gets the total memory span of all segments combined.
    /// Note: This only works if there's no wrap-around (single segment).
    /// </summary>
    public ReadOnlySpan<T> AsSpan()
    {
        if (HasWrapAround)
            throw new InvalidOperationException("Cannot create a single span from a wrapped partition view. Use GetEnumerator() instead.");
        
        return Segment1.Span;
    }
}

/// <summary>
/// High-performance enumerator for PartitionView that avoids allocations.
/// </summary>
public ref struct PartitionViewEnumerator<T>
{
    private ReadOnlySpan<T> _segment1;
    private ReadOnlySpan<T> _segment2;
    private int _currentIndex;
    private bool _inSecondSegment;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal PartitionViewEnumerator(ReadOnlySpan<T> segment1, ReadOnlySpan<T> segment2)
    {
        _segment1 = segment1;
        _segment2 = segment2;
        _currentIndex = -1;
        _inSecondSegment = false;
    }

    /// <summary>
    /// Gets the current event.
    /// </summary>
    public readonly T Current
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            if (_inSecondSegment)
                return _segment2[_currentIndex];
            return _segment1[_currentIndex];
        }
    }

    /// <summary>
    /// Advances to the next event.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MoveNext()
    {
        if (!_inSecondSegment)
        {
            _currentIndex++;
            if (_currentIndex < _segment1.Length)
                return true;

            // Switch to second segment if available
            if (_segment2.Length > 0)
            {
                _inSecondSegment = true;
                _currentIndex = 0;
                return true;
            }
            return false;
        }

        _currentIndex++;
        return _currentIndex < _segment2.Length;
    }
}
