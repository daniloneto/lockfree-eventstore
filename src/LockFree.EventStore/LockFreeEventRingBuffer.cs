using System.Threading;
using System.Runtime.CompilerServices;

namespace LockFree.EventStore;

/// <summary>
/// Specialized high-performance ring buffer for Event struct with contiguous memory layout.
/// Optimized for cache locality and zero-allocation operations.
/// </summary>
public sealed class LockFreeEventRingBuffer
{
    private readonly Event[] _buffer; // Contiguous Event[] for optimal cache locality
    private readonly int _capacity;
    private long _head;
    private long _tail;
    private readonly Action<Event>? _onItemDiscarded;
    
    // Epoch-based consistency for snapshot operations
    private int _epoch;

    /// <summary>
    /// Initializes a new instance with the specified capacity.
    /// </summary>
    public LockFreeEventRingBuffer(int capacity, Action<Event>? onItemDiscarded = null)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity));
        _capacity = capacity;
        _buffer = new Event[capacity]; // Contiguous memory allocation
        _onItemDiscarded = onItemDiscarded;
        _epoch = 0;
    }

    /// <summary>
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
    /// Enqueues a single event, overwriting the oldest if necessary.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(Event item)
    {
        var tail = Interlocked.Increment(ref _tail);
        var index = (int)((tail - 1) % _capacity);
        
        // Check if we're overwriting an existing event
        var currentHead = Volatile.Read(ref _head);
        if (tail - currentHead > _capacity)
        {
            var overwrittenIndex = (int)(currentHead % _capacity);
            _onItemDiscarded?.Invoke(_buffer[overwrittenIndex]);
        }
        
        _buffer[index] = item; // Direct struct assignment to contiguous array
        AdvanceHeadIfNeeded(tail);
        
        // Update epoch sparingly - only when tail advances significantly
        if ((tail & 0xFF) == 0) // Every 256 items
        {
            Interlocked.Increment(ref _epoch);
        }
        
        return true;
    }

    /// <summary>
    /// Enqueues a batch of events with optimized epoch updating.
    /// Only updates epoch once for the entire batch instead of per item.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryEnqueueBatch(ReadOnlySpan<Event> batch)
    {
        if (batch.IsEmpty) return 0;
        
        // Reserve space for the entire batch atomically
        var startTail = Interlocked.Add(ref _tail, batch.Length);
        var endTail = startTail;
        
        // Check for overwrites before writing
        var currentHead = Volatile.Read(ref _head);
        var overwriteStart = Math.Max(0, endTail - _capacity);
        
        if (_onItemDiscarded != null && overwriteStart > currentHead)
        {
            for (long i = currentHead; i < overwriteStart; i++)
            {
                var overwrittenIndex = (int)(i % _capacity);
                _onItemDiscarded(_buffer[overwrittenIndex]);
            }
        }
        
        // Write all events to their reserved positions
        for (int i = 0; i < batch.Length; i++)
        {
            var position = startTail - batch.Length + i;
            _buffer[(int)(position % _capacity)] = batch[i];
        }
        
        // Advance head if necessary to maintain ring buffer invariant
        var newHead = Math.Max(currentHead, endTail - _capacity);
        if (newHead > currentHead)
        {
            Volatile.Write(ref _head, newHead);
        }
        
        // Update epoch once for the entire batch
        Interlocked.Increment(ref _epoch);
        
        return batch.Length;
    }

    /// <summary>
    /// Gets a snapshot of current events without enumeration allocation.
    /// </summary>
    public IEnumerable<Event> EnumerateSnapshot()
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(_capacity, tail - head);
        
        var results = new List<Event>((int)count);
        for (long i = 0; i < count; i++)
        {
            var index = (int)((head + i) % _capacity);
            results.Add(_buffer[index]);
        }
        return results;
    }

    /// <summary>
    /// Enumerates events with a filter predicate.
    /// </summary>
    public IEnumerable<Event> EnumerateSnapshot(Func<Event, bool> predicate)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(_capacity, tail - head);
        
        var results = new List<Event>();
        for (long i = 0; i < count; i++)
        {
            var index = (int)((head + i) % _capacity);
            var item = _buffer[index];
            if (predicate(item))
            {
                results.Add(item);
            }
        }
        return results;
    }

    /// <summary>
    /// Enumerates events within a time range.
    /// </summary>
    public IEnumerable<Event> EnumerateSnapshot(long fromTicks, long toTicks)
    {
        return EnumerateSnapshot(e => e.TimestampTicks >= fromTicks && e.TimestampTicks <= toTicks);
    }

    /// <summary>
    /// Enumerates events for a specific key.
    /// </summary>
    public IEnumerable<Event> EnumerateSnapshot(KeyId key)
    {
        return EnumerateSnapshot(e => e.Key.Equals(key));
    }

    /// <summary>
    /// Enumerates events for a specific key within a time range.
    /// </summary>
    public IEnumerable<Event> EnumerateSnapshot(KeyId key, long fromTicks, long toTicks)
    {
        return EnumerateSnapshot(e => e.Key.Equals(key) && e.TimestampTicks >= fromTicks && e.TimestampTicks <= toTicks);
    }

    /// <summary>
    /// Purges events older than the specified timestamp.
    /// Returns the number of events purged.
    /// </summary>
    public long Purge(long beforeTimestamp)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(_capacity, tail - head);
        
        long purged = 0;
        long newHead = head;
        
        // Find the first event that should be kept
        for (long i = 0; i < count; i++)
        {
            var index = (int)((head + i) % _capacity);
            var item = _buffer[index];
            
            if (item.TimestampTicks < beforeTimestamp)
            {
                purged++;
                newHead = head + i + 1;
            }
            else
            {
                break; // Assuming events are roughly ordered by time
            }
        }
        
        if (purged > 0)
        {
            Volatile.Write(ref _head, newHead);
            Interlocked.Increment(ref _epoch);
        }
        
        return purged;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AdvanceHeadIfNeeded(long newTail)
    {
        var currentHead = Volatile.Read(ref _head);
        var targetHead = newTail - _capacity;
        
        if (targetHead > currentHead)
        {
            // Try to advance head, but only if another thread hasn't already done it
            Interlocked.CompareExchange(ref _head, targetHead, currentHead);
        }
    }

    /// <summary>
    /// Gets buffer statistics for debugging and monitoring.
    /// </summary>
    public (long Head, long Tail, int Epoch, long Count) GetStatistics()
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var epoch = Volatile.Read(ref _epoch);
        var count = Math.Max(0, Math.Min(_capacity, tail - head));
        
        return (head, tail, epoch, count);
    }

    /// <summary>
    /// Zero-allocation snapshot using chunked processing with pooled buffers.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotZeroAlloc(Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(_capacity, tail - head);
        
        if (count == 0) return;
        
        Buffers.WithRentedBuffer<Event>(Math.Min(chunkSize, (int)count), buffer =>
        {
            var bufferCount = 0;
            // Respect requested chunk size even if pool returns larger arrays
            var effectiveChunk = Math.Min(chunkSize, buffer.Length);
            
            for (long i = 0; i < count; i++)
            {
                var index = (int)((head + i) % _capacity);
                buffer[bufferCount++] = _buffer[index];
                
                if (bufferCount >= effectiveChunk)
                {
                    processor(buffer.AsSpan(0, bufferCount));
                    bufferCount = 0;
                }
            }
            
            // Process remaining events
            if (bufferCount > 0)
            {
                processor(buffer.AsSpan(0, bufferCount));
            }
        }, Buffers.EventPool);
    }

    /// <summary>
    /// Zero-allocation filtered snapshot using chunked processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotFilteredZeroAlloc(Func<Event, bool> predicate, Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        var count = Math.Min(_capacity, tail - head);
        
        if (count == 0) return;
        
        Buffers.WithRentedBuffer<Event>(chunkSize, buffer =>
        {
            var bufferCount = 0;
            var effectiveChunk = Math.Min(chunkSize, buffer.Length);
            
            for (long i = 0; i < count; i++)
            {
                var index = (int)((head + i) % _capacity);
                var item = _buffer[index];
                
                if (predicate(item))
                {
                    buffer[bufferCount++] = item;
                    
                    if (bufferCount >= effectiveChunk)
                    {
                        processor(buffer.AsSpan(0, bufferCount));
                        bufferCount = 0;
                    }
                }
            }
            
            // Process remaining events
            if (bufferCount > 0)
            {
                processor(buffer.AsSpan(0, bufferCount));
            }
        }, Buffers.EventPool);
    }

    /// <summary>
    /// Zero-allocation time range snapshot using chunked processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotTimeRangeZeroAlloc(long fromTicks, long toTicks, Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        SnapshotFilteredZeroAlloc(e => e.TimestampTicks >= fromTicks && e.TimestampTicks <= toTicks, processor, chunkSize);
    }

    /// <summary>
    /// Zero-allocation key-specific snapshot using chunked processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotByKeyZeroAlloc(KeyId key, Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        SnapshotFilteredZeroAlloc(e => e.Key.Equals(key), processor, chunkSize);
    }

    /// <summary>
    /// Zero-allocation key and time range snapshot using chunked processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SnapshotByKeyAndTimeZeroAlloc(KeyId key, long fromTicks, long toTicks, Action<ReadOnlySpan<Event>> processor, int chunkSize = Buffers.DefaultChunkSize)
    {
        SnapshotFilteredZeroAlloc(e => e.Key.Equals(key) && e.TimestampTicks >= fromTicks && e.TimestampTicks <= toTicks, processor, chunkSize);
    }
}
