using System.Threading;
using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Fixed-size MPMC ring buffer with lock-free overwrite when full.
/// </summary>
public sealed class LockFreeRingBuffer<T>
{
    private readonly T[] _buffer;
    private readonly int _capacity;
    private long _head;
    private long _tail;
    private readonly Action<T>? _onItemDiscarded;

    /// <summary>
    /// Initializes a new instance with the specified capacity.
    /// </summary>
    public LockFreeRingBuffer(int capacity, Action<T>? onItemDiscarded = null)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity));
        _capacity = capacity;
        _buffer = new T[capacity];
        _onItemDiscarded = onItemDiscarded;
    }    /// <summary>
    /// Total capacity.
    /// </summary>
    public int Capacity => _capacity;

    /// <summary>
    /// Approximate count of items currently in the buffer.
    /// </summary>
    public long CountApprox => Math.Max(0, Math.Min(_capacity, Volatile.Read(ref _tail) - Volatile.Read(ref _head)));

    /// <summary>
    /// Whether the buffer is empty (approximate).
    /// </summary>
    public bool IsEmpty => CountApprox == 0;

    /// <summary>
    /// Whether the buffer is at full capacity (approximate).
    /// </summary>
    public bool IsFull => CountApprox >= _capacity;

    /// <summary>
    /// Enqueues a single item, overwriting the oldest if necessary.
    /// </summary>
    public bool TryEnqueue(T item)
    {
        var tail = Interlocked.Increment(ref _tail);
        _buffer[(tail - 1) % _capacity] = item;
        AdvanceHeadIfNeeded(tail);
        return true;
    }

    /// <summary>
    /// Enqueues a batch of items.
    /// </summary>
    public int TryEnqueue(ReadOnlySpan<T> batch)
    {
        int written = 0;
        foreach (var item in batch)
        {
            TryEnqueue(item);
            written++;
        }
        return written;
    }    private void AdvanceHeadIfNeeded(long tail)
    {
        while (true)
        {
            var head = Volatile.Read(ref _head);
            if (tail - head <= _capacity)
                break;
            
            // Notify about discarded item if callback is provided
            var discardedIndex = head % _capacity;
            var discardedItem = _buffer[discardedIndex];
            _onItemDiscarded?.Invoke(discardedItem);
            
            if (Interlocked.CompareExchange(ref _head, head + 1, head) == head)
                break;
        }
    }

    /// <summary>
    /// Clears all items from the buffer.
    /// </summary>
    public void Clear()
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        // Clear the array to help GC
        if (head < tail)
        {
            for (long i = head; i < tail; i++)
            {
                _buffer[i % _capacity] = default(T)!;
            }
        }
        
        // Reset pointers
        Volatile.Write(ref _head, 0);
        Volatile.Write(ref _tail, 0);
    }

    /// <summary>
    /// Copies a snapshot of the buffer into <paramref name="destination"/>.
    /// </summary>
    /// <returns>Number of items copied.</returns>
    public int Snapshot(Span<T> destination)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var length = (int)Math.Min(Math.Min(tail - head, _capacity), destination.Length);
        for (int i = 0; i < length; i++)
        {
            destination[i] = _buffer[(head + i) % _capacity];
        }
        return length;
    }

    /// <summary>
    /// Convenience method that allocates an array and returns an enumerable.
    /// </summary>
    public IEnumerable<T> EnumerateSnapshot()
    {
        var tmp = new T[_capacity];
        var len = Snapshot(tmp);
        for (int i = 0; i < len; i++)
            yield return tmp[i];
    }

    /// <summary>
    /// Internal method for window aggregation. Enumerates items within a timestamp range
    /// without allocating collections, using a callback for each valid item.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void EnumerateWindow<TState>(
        long fromTicks, 
        long toTicks, 
        IEventTimestampSelector<T>? timestampSelector,
        ref TState state,
        WindowItemCallback<T, TState> callback)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(tail - head, _capacity);
        
        for (long i = 0; i < count; i++)
        {
            var index = (head + i) % _capacity;
            var item = _buffer[index];
            
            if (timestampSelector != null)
            {
                var timestamp = timestampSelector.GetTimestamp(item);
                var ticks = timestamp.Ticks;
                
                if (ticks >= fromTicks && ticks <= toTicks)
                {
                    callback(ref state, item, ticks);
                }
            }
            else
            {
                // No timestamp filtering - include all items
                callback(ref state, item, 0);
            }
        }
    }

    /// <summary>
    /// Advances the window head to start from the first event with timestamp >= windowStartTicks.
    /// Updates aggregate state by removing events that fall outside the window.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void AdvanceWindowTo<TState>(
        long windowStartTicks,
        IEventTimestampSelector<T>? timestampSelector,
        ref TState state,
        WindowAdvanceCallback<T, TState> removeCallback,
        ref int windowHeadIndex)
    {
        if (timestampSelector == null) return;
        
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(tail - head, _capacity);
        
        // Find new window head position
        var newWindowHead = windowHeadIndex;
        
        for (long i = windowHeadIndex; i < count; i++)
        {
            var index = (head + i) % _capacity;
            var item = _buffer[index];
            var timestamp = timestampSelector.GetTimestamp(item);
            
            if (timestamp.Ticks >= windowStartTicks)
            {
                break; // Found the new window start
            }
            
            // Remove this item from the window state
            removeCallback(ref state, item, timestamp.Ticks);
            newWindowHead = (int)(i + 1);
        }
        
        windowHeadIndex = newWindowHead;
    }
}

/// <summary>
/// Callback delegate for processing items during window enumeration.
/// </summary>
internal delegate void WindowItemCallback<in T, TState>(ref TState state, T item, long ticks);

/// <summary>
/// Callback delegate for removing items during window advancement.
/// </summary>
internal delegate void WindowAdvanceCallback<in T, TState>(ref TState state, T item, long ticks);
