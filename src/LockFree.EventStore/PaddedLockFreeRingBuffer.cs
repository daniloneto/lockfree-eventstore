using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace LockFree.EventStore;

/// <summary>
/// High-performance MPMC ring buffer with anti-false sharing padding.
/// Uses padded partition headers to prevent false sharing in multi-core scenarios.
/// </summary>
/// <typeparam name="T">The type of items stored in the buffer</typeparam>
public sealed class PaddedLockFreeRingBuffer<T>
{
    private readonly T[] _buffer;
    private readonly Action<T>? _onItemDiscarded;
    private PartitionHeader _header;
    
    /// <summary>
    /// Initializes a new instance with the specified capacity and anti-false sharing padding.
    /// </summary>
    public PaddedLockFreeRingBuffer(int capacity, Action<T>? onItemDiscarded = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        
        _buffer = new T[capacity];
        _onItemDiscarded = onItemDiscarded;
        _header = new PartitionHeader(capacity);
    }
    
    /// <summary>
    /// Total capacity.
    /// </summary>
    public int Capacity => _header.Capacity;

    /// <summary>
    /// Approximate count of items currently in the buffer.
    /// </summary>
    public long CountApprox => _header.GetApproximateCount();

    /// <summary>
    /// Whether the buffer is empty (approximate).
    /// </summary>
    public bool IsEmpty => CountApprox == 0;

    /// <summary>
    /// Whether the buffer is at full capacity (approximate).
    /// </summary>
    public bool IsFull => CountApprox >= Capacity;

    /// <summary>
    /// Enqueues a single item, overwriting the oldest if necessary.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(T item)
    {
        var newTail = Interlocked.Increment(ref _header.Tail);
        var index = (newTail - 1) % Capacity;
        
        // Store the item
        _buffer[index] = item;
        
        // Update count and handle potential overwrite
        _header.UpdateCount();
        
        // Check if we need to advance head (buffer is full)
        var head = Volatile.Read(ref _header.Head);
        if (newTail - head > Capacity)
        {
            var oldItem = _buffer[head % Capacity];
            if (Interlocked.CompareExchange(ref _header.Head, head + 1, head) == head)
            {
                _onItemDiscarded?.Invoke(oldItem);
                _header.IncrementEpoch();
            }
        }
        
        return true;
    }

    /// <summary>
    /// Attempts to enqueue multiple items in a batch operation.
    /// </summary>
    public int TryEnqueueBatch(ReadOnlySpan<T> items)
    {
        if (items.IsEmpty)
        {
            return 0;
        }

        var batchSize = items.Length;
        var newTail = Interlocked.Add(ref _header.Tail, batchSize);
        var startIndex = newTail - batchSize;
        
        // Store all items
        for (int i = 0; i < batchSize; i++)
        {
            var index = (startIndex + i) % Capacity;
            _buffer[index] = items[i];
        }
        
        // Update count
        _header.UpdateCount();
        
        // Handle potential overwrites
        var head = Volatile.Read(ref _header.Head);
        var overflow = newTail - head - Capacity;
        
        if (overflow > 0)
        {
            // We've overwritten some items
            var itemsToDiscard = Math.Min((int)overflow, batchSize);
            
            if (_onItemDiscarded != null)
            {
                for (int i = 0; i < itemsToDiscard; i++)
                {
                    var discardIndex = (head + i) % Capacity;
                    _onItemDiscarded(_buffer[discardIndex]);
                }
            }
            
            // Advance head past discarded items
            var newHead = head + itemsToDiscard;
            if (Interlocked.CompareExchange(ref _header.Head, newHead, head) == head)
            {
                _header.IncrementEpoch();
            }
        }
        
        return batchSize;
    }

    /// <summary>
    /// Takes a snapshot of current items into the provided buffer.
    /// </summary>
    public int Snapshot(Span<T> destination)
    {
        var head = Volatile.Read(ref _header.Head);
        var tail = Volatile.Read(ref _header.Tail);
        var count = (int)Math.Min(destination.Length, Math.Min(Capacity, tail - head));
        
        if (count <= 0)
        {
            return 0;
        }

        for (int i = 0; i < count; i++)
        {
            var index = (head + i) % Capacity;
            destination[i] = _buffer[index];
        }
        
        return count;
    }

    /// <summary>
    /// Zero-allocation snapshot using chunked processing with pooled buffers.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotZeroAlloc<TBuffer>(Action<ReadOnlySpan<T>> processor, ArrayPool<T> pool, int chunkSize = Buffers.DefaultChunkSize)
    {
        var buffer = pool.Rent(Math.Min(chunkSize, Capacity));
        try
        {
            var len = Snapshot(buffer);
            if (len > 0)
            {
                // Process in chunks
                for (int i = 0; i < len; i += chunkSize)
                {
                    var chunkLen = Math.Min(chunkSize, len - i);
                    processor(buffer.AsSpan(i, chunkLen));
                }
            }
        }
        finally
        {
            pool.Return(buffer, clearArray: false);
        }
    }

    /// <summary>
    /// Zero-allocation enumeration using callback processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ProcessItemsZeroAlloc<TState>(TState initialState, Func<TState, T, (TState State, bool Continue)> processor, out TState finalState)
    {
        var head = Volatile.Read(ref _header.Head);
        var tail = Volatile.Read(ref _header.Tail);
        var count = Math.Min(tail - head, Capacity);
        
        var state = initialState;
        for (long i = 0; i < count; i++)
        {
            var index = (head + i) % Capacity;
            var item = _buffer[index];
            
            var result = processor(state, item);
            state = result.State;
            
            if (!result.Continue)
            {
                break; // Early termination
            }
        }
        
        finalState = state;
    }

    /// <summary>
    /// Creates a view of the buffer contents without copying data.
    /// Uses epoch-based consistency to ensure a stable snapshot.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PartitionView<T> CreateView(IEventTimestampSelector<T>? timestampSelector = null)
    {
        // Try multiple times to get a consistent snapshot
        for (int attempt = 0; attempt < 3; attempt++)
        {
            var epoch1 = Volatile.Read(ref _header.Epoch);
            var head = Volatile.Read(ref _header.Head);
            var tail = Volatile.Read(ref _header.Tail);
            var epoch2 = Volatile.Read(ref _header.Epoch);
            
            if (epoch1 == epoch2)
            {
                return CreateViewFromRange(head, tail, timestampSelector);
            }
        }
        
        // Fallback: take current snapshot even if not perfectly consistent
        var currentHead = Volatile.Read(ref _header.Head);
        var currentTail = Volatile.Read(ref _header.Tail);
        return CreateViewFromRange(currentHead, currentTail, timestampSelector);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PartitionView<T> CreateViewFromRange(long head, long tail, IEventTimestampSelector<T>? timestampSelector)
    {
        var count = (int)Math.Min(Capacity, tail - head);
        if (count <= 0)
        {
            return new PartitionView<T>(
                ReadOnlyMemory<T>.Empty,
                ReadOnlyMemory<T>.Empty,
                0, 0, 0);
        }

        var headIndex = (int)(head % Capacity);
        var tailIndex = (int)(tail % Capacity);
        
        // Calculate timestamp range if selector is provided
        long fromTicks = 0, toTicks = 0;
        if (timestampSelector != null && count > 0)
        {
            var firstItem = _buffer[headIndex];
            var lastItem = _buffer[(tailIndex - 1 + Capacity) % Capacity];
            fromTicks = timestampSelector.GetTimestamp(firstItem).Ticks;
            toTicks = timestampSelector.GetTimestamp(lastItem).Ticks;
        }

        if (tailIndex > headIndex || (tailIndex == headIndex && count == Capacity))
        {
            // No wrap-around case
            var segment = new ReadOnlyMemory<T>(_buffer, headIndex, count);
            return new PartitionView<T>(segment, ReadOnlyMemory<T>.Empty, count, fromTicks, toTicks);
        }
        else
        {
            // Wrap-around case
            var segment1 = new ReadOnlyMemory<T>(_buffer, headIndex, Capacity - headIndex);
            var segment2 = new ReadOnlyMemory<T>(_buffer, 0, tailIndex);
            return new PartitionView<T>(segment1, segment2, count, fromTicks, toTicks);
        }
    }

    /// <summary>
    /// Creates a filtered view of buffer contents based on timestamp range.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PartitionView<T> CreateViewFiltered(
        long fromTicks, 
        long toTicks, 
        IEventTimestampSelector<T> timestampSelector)
    {
        ArgumentNullException.ThrowIfNull(timestampSelector);
            
        const int maxRetries = 10;
        
        for (int retry = 0; retry < maxRetries; retry++)
        {
            var startEpoch = Volatile.Read(ref _header.Epoch);
            var head = Volatile.Read(ref _header.Head);
            var tail = Volatile.Read(ref _header.Tail);
            var endEpoch = Volatile.Read(ref _header.Epoch);
            
            if (startEpoch != endEpoch)
            {
                continue;
            }

            return CreateFilteredViewFromRange(head, tail, fromTicks, toTicks, timestampSelector);
        }
        
        // Fallback
        var currentHead = Volatile.Read(ref _header.Head);
        var currentTail = Volatile.Read(ref _header.Tail);
        return CreateFilteredViewFromRange(currentHead, currentTail, fromTicks, toTicks, timestampSelector);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PartitionView<T> CreateFilteredViewFromRange(
        long head, 
        long tail, 
        long fromTicks, 
        long toTicks, 
        IEventTimestampSelector<T> timestampSelector)
    {
        var totalCount = (int)Math.Min(Math.Min(tail - head, Capacity), int.MaxValue);
        if (totalCount <= 0)
        {
            return new PartitionView<T>(
                ReadOnlyMemory<T>.Empty,
                ReadOnlyMemory<T>.Empty,
                0, fromTicks, toTicks);
        }
        
        // Find the range of valid events
        int validStart = -1, validEnd = -1;
        int validCount = 0;
        
        for (int i = 0; i < totalCount; i++)
        {
            var index = (int)((head + i) % Capacity);
            var item = _buffer[index];
            var itemTicks = timestampSelector.GetTimestamp(item).Ticks;
            
            if (itemTicks >= fromTicks && itemTicks <= toTicks)
            {
                if (validStart == -1)
                {
                    validStart = i;
                }

                validEnd = i;
                validCount++;
            }
        }
        
        if (validCount == 0)
        {
            return new PartitionView<T>(
                ReadOnlyMemory<T>.Empty,
                ReadOnlyMemory<T>.Empty,
                0, fromTicks, toTicks);
        }
        
        // Create view for the valid range
        var actualStartIndex = (int)((head + validStart) % Capacity);
        var actualEndIndex = (int)((head + validEnd) % Capacity);
        
        if (actualStartIndex <= actualEndIndex)
        {
            // Single segment
            var length = actualEndIndex - actualStartIndex + 1;
            var segment = new ReadOnlyMemory<T>(_buffer, actualStartIndex, length);
            return new PartitionView<T>(segment, ReadOnlyMemory<T>.Empty, validCount, fromTicks, toTicks);
        }
        else
        {
            // Wrap-around case
            var firstSegmentLength = Capacity - actualStartIndex;
            var secondSegmentLength = actualEndIndex + 1;
            
            var segment1 = new ReadOnlyMemory<T>(_buffer, actualStartIndex, firstSegmentLength);
            var segment2 = new ReadOnlyMemory<T>(_buffer, 0, secondSegmentLength);
            
            return new PartitionView<T>(segment1, segment2, validCount, fromTicks, toTicks);
        }
    }

    /// <summary>
    /// Clears all items from the buffer.
    /// </summary>
    public void Clear()
    {
        var tail = Volatile.Read(ref _header.Tail);
        Volatile.Write(ref _header.Head, tail);
        _header.UpdateCount();
        _header.IncrementEpoch();
    }

    /// <summary>
    /// Gets statistics for monitoring and debugging.
    /// </summary>
    public (long Head, long Tail, int Epoch, long Count) GetStatistics()
    {
        var head = Volatile.Read(ref _header.Head);
        var tail = Volatile.Read(ref _header.Tail);
        var epoch = Volatile.Read(ref _header.Epoch);
        var count = _header.GetApproximateCount();
        
        return (head, tail, epoch, count);
    }

    /// <summary>
    /// Convenience method that allocates an array and returns an enumerable.
    /// </summary>
    public IEnumerable<T> EnumerateSnapshot()
    {
        var tmp = new T[Capacity];
        var len = Snapshot(tmp);
        var results = new List<T>(len);
        for (int i = 0; i < len; i++)
        {
            results.Add(tmp[i]);
        }

        return results;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryCopyStable(out T[] items, out long version)
    {
        const int maxAttempts = 8;
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            var head1 = Volatile.Read(ref _header.Head);
            var tail = Volatile.Read(ref _header.Tail);
            var head2 = Volatile.Read(ref _header.Head);
            if (head1 != head2)
            {
                continue;
            }
            var capacity = Capacity;
            var available = tail - head1;
            if (available <= 0)
            {
                items = Array.Empty<T>();
                version = 0;
                return true;
            }
            var count = (int)Math.Min(available, capacity);
            var start = head1;
            var end = start + count;
            var headIndex = (int)(start % capacity);
            var buffer = new T[count];
            if (headIndex + count <= capacity)
            {
                Array.Copy(_buffer, headIndex, buffer, 0, count);
            }
            else
            {
                var first = capacity - headIndex;
                var second = count - first;
                Array.Copy(_buffer, headIndex, buffer, 0, first);
                Array.Copy(_buffer, 0, buffer, first, second);
            }
            items = buffer;
            version = end;
            return true;
        }
        items = Array.Empty<T>();
        version = 0;
        return false;
    }
}
