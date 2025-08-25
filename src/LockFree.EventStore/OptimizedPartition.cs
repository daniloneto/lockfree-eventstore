using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace LockFree.EventStore;

/// <summary>
/// Storage layout optimization strategies for different query patterns
/// </summary>
public enum StorageLayout
{
    /// <summary>
    /// Standard Array of Structures layout (contiguous Event[] array)
    /// </summary>
    AoS,
    
    /// <summary>
    /// Structure of Arrays layout (separate arrays for keys, values, timestamps)
    /// Optimized for queries that scan a single property across many events
    /// </summary>
    SoA
}

/// <summary>
/// Specialized partition that can switch between AoS and SoA layouts
/// </summary>
public sealed class OptimizedPartition
{    // AoS storage
    private Event[]? _events;
    
    // SoA storage
    private KeyId[]? _keys;
    private double[]? _values;
    private long[]? _timestamps;
    
    private long _head;
    private long _tail;
    private int _epoch;
    private readonly Action<Event>? _onItemDiscarded;
    
    /// <summary>
    /// Initializes a new instance with the specified capacity and layout.
    /// <summary>
    /// Initializes a fixed-capacity OptimizedPartition using the specified storage layout.
    /// </summary>
    /// <param name="capacity">Total number of slots for the partition; must be greater than zero.</param>
    /// <param name="layout">Storage layout to use: AoS (array of Event) or SoA (separate arrays for keys, values, timestamps).</param>
    /// <param name="onItemDiscarded">Optional callback invoked for each item that is overwritten when the partition overflows.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="capacity"/> is zero or negative.</exception>
    public OptimizedPartition(int capacity, StorageLayout layout = StorageLayout.AoS, Action<Event>? onItemDiscarded = null)
    {
        // CA1512: prefer guard method
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        
        Capacity = capacity;
        Layout = layout;
        _onItemDiscarded = onItemDiscarded;
        
        if (layout == StorageLayout.AoS)
        {
            _events = new Event[capacity];
        }
        else
        {
            _keys = new KeyId[capacity];
            _values = new double[capacity];
            _timestamps = new long[capacity];
        }
        
        _head = 0;
        _tail = 0;
        _epoch = 0;
    }

    /// <summary>
    /// Gets the current storage layout.
    /// </summary>
    public StorageLayout Layout { get; }

    /// <summary>
    /// Total capacity.
    /// </summary>
    public int Capacity { get; }

    /// <summary>
    /// Approximate count of items currently in the buffer.
    /// </summary>
    public long CountApprox => Math.Max(0, Math.Min(Capacity, Volatile.Read(ref _tail) - Volatile.Read(ref _head)));
    
    /// <summary>
    /// Whether the buffer is empty (approximate).
    /// </summary>
    public bool IsEmpty => CountApprox == 0;
    
    /// <summary>
    /// Whether the buffer is at full capacity (approximate).
    /// </summary>
    public bool IsFull => CountApprox >= Capacity;
    
    /// <summary>
    /// Enqueues a single event.
    /// <summary>
    /// Atomically appends an event to the partition's ring buffer.
    /// </summary>
    /// <remarks>
    /// This method always reserves a slot and returns true. If the partition is full,
    /// the head may be advanced (discarding the oldest items) and the optional
    /// discard callback will be invoked for each overwritten item.
    /// The operation is thread-safe (uses atomic increments) and updates an internal
    /// epoch counter periodically for versioning.
    /// </remarks>
    /// <param name="item">The event to enqueue.</param>
    /// <returns>Always returns <c>true</c>.</returns>
    public bool TryEnqueue(Event item)
    {
        var tail = Interlocked.Increment(ref _tail);
        var index = (int)((tail - 1) % Capacity);
        
        if (Layout == StorageLayout.AoS)
        {
            _events![index] = item;
        }
        else
        {
            _keys![index] = item.Key;
            _values![index] = item.Value;
            _timestamps![index] = item.TimestampTicks;
        }
        
        AdvanceHeadIfNeeded(tail);
        
        // Update epoch sparingly
        if ((tail & 0xFF) == 0)
        {
            _ = Interlocked.Increment(ref _epoch);
        }
        
        return true;
    }
      /// <summary>
    /// Enqueues a batch of events with optimized epoch updating.
    /// <summary>
    /// Atomically enqueues a batch of events into the partition and returns the number enqueued.
    /// </summary>
    /// <param name="batch">A span of events to append; if empty the method returns 0 immediately.</param>
    /// <returns>The number of events enqueued (equal to <paramref name="batch"/>.Length when non-empty).</returns>
    /// <remarks>
    /// Reservation for the entire batch is performed atomically, making the append safe for concurrent writers.
    /// If the partition lacks capacity for the new items, the head is advanced and the oldest entries are overwritten;
    /// when overwrites occur the optional discard callback will be invoked for each discarded item. The partition's
    /// epoch is incremented once for the whole batch to indicate activity.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryEnqueueBatch(ReadOnlySpan<Event> batch)
    {
        if (batch.IsEmpty)
        {
            return 0;
        }

        // Reserve space for the entire batch atomically
        var startTail = Interlocked.Add(ref _tail, batch.Length);
        
        // Write all items to their reserved positions
        if (Layout == StorageLayout.AoS)
        {
            for (int i = 0; i < batch.Length; i++)
            {
                var position = startTail - batch.Length + i;
                _events![position % Capacity] = batch[i];
            }
        }
        else
        {
            for (int i = 0; i < batch.Length; i++)
            {
                var position = startTail - batch.Length + i;
                var index = (int)(position % Capacity);
                
                _keys![index] = batch[i].Key;
                _values![index] = batch[i].Value;
                _timestamps![index] = batch[i].TimestampTicks;
            }
        }
        
        // Advance head if needed (may overwrite oldest entries)
        AdvanceHeadIfNeeded(startTail);

        // Update epoch once for the entire batch
        _ = Interlocked.Increment(ref _epoch);
        
        return batch.Length;
    }
    
    /// <summary>
    /// Advances the head pointer if needed to avoid buffer overflow.
    /// <summary>
    /// Advances the partition head when writers have reserved more slots than the capacity allows,
    /// freeing space by moving the head forward to (tail - Capacity).
    /// </summary>
    /// <remarks>
    /// This method performs a compare-and-swap loop to update the volatile head pointer atomically.
    /// If the head is advanced and an item-discard callback was supplied, the callback is invoked
    /// once for each overwritten item in oldest-to-newest order (items at indices [oldHead, targetHead)).
    /// The method is thread-safe and may be retried internally if the CAS fails due to concurrent updates.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]    private void AdvanceHeadIfNeeded(long tail)
    {
        while (true)
        {
            var head = Volatile.Read(ref _head);
            var targetHead = tail - Capacity;
            if (targetHead <= head)
            {
                return; // No advance needed
            }

            // Attempt to move head in one step
            if (Interlocked.CompareExchange(ref _head, targetHead, head) == head)
            {
                // Invoke discard callback for all overwritten items
                if (_onItemDiscarded != null)
                {
                    for (long i = head; i < targetHead; i++)
                    {
                        var index = (int)(i % Capacity);
                        var eventItem = GetEventAt(index);
                        _onItemDiscarded(eventItem);
                    }
                }
                return;
            }
            // CAS failed, retry with updated head
        }
    }
    
    /// <summary>
    /// Gets an event from a specific index.
    /// <summary>
    /// Returns the Event stored at the given internal buffer index.
    /// </summary>
    /// <param name="index">Zero-based index into the partition's internal storage (0 through Capacity-1).</param>
    /// <returns>The Event at the specified storage index. In SoA layout this constructs a new Event from the parallel arrays; in AoS layout it returns the stored Event reference.</returns>
    private Event GetEventAt(int index)
    {
        return Layout == StorageLayout.AoS ? _events![index] : new Event(_keys![index], _values![index], _timestamps![index]);
    }
      /// <summary>
    /// Gets a read-only view of the partition.
    /// <summary>
    /// Creates a read-only view of the partition's current contents.
    /// </summary>
    /// <remarks>
    /// The view is a point-in-time snapshot based on volatile reads of the partition's head and tail.
    /// For StorageLayout.SoA, events are reconstructed into a temporary array (allocated) and returned
    /// as a single contiguous segment. For StorageLayout.AoS, the view will reference the internal
    /// backing array and is returned as either a single contiguous segment or two segments when the
    /// logical contents wrap around the ring buffer. If the partition is empty, an empty view is
    /// returned. The returned view includes the item count and the first/last timestamp ticks.
    /// </remarks>
    /// <returns>A <see cref="PartitionView{Event}"/> describing one or two read-only segments, the item count, and first/last timestamps.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PartitionView<Event> GetView()
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(Capacity, tail - head);
        if (count <= 0)
        {
            return new PartitionView<Event>(
                ReadOnlyMemory<Event>.Empty, 
                ReadOnlyMemory<Event>.Empty, 
                0, 
                0, 
                0);
        }
        // If using SoA layout, we need to create a temporary Event array
        if (Layout == StorageLayout.SoA)
        {
            var tempEvents = new Event[count];
            var headPos = (int)(head % Capacity);
            
            for (int i = 0; i < count; i++)
            {
                var index = (headPos + i) % Capacity;
                tempEvents[i] = new Event(_keys![index], _values![index], _timestamps![index]);
            }
            
            return new PartitionView<Event>(
                new ReadOnlyMemory<Event>(tempEvents),
                ReadOnlyMemory<Event>.Empty,
                count,
                tempEvents[0].TimestampTicks,
                tempEvents[count - 1].TimestampTicks);
        }
        
        // For AoS layout, we can use the existing buffer directly
        var headIndex = (int)(head % Capacity);
        var tailIndex = (int)(tail % Capacity);
        var isFull = count == Capacity;
        
        if (tailIndex > headIndex || (isFull && headIndex == 0))
        {
            // No wrap-around case (or full buffer starting at index 0)
            var segment = new ReadOnlyMemory<Event>(_events!, headIndex, count);
            
            return new PartitionView<Event>(
                segment, 
                ReadOnlyMemory<Event>.Empty, 
                count,
                _events![headIndex].TimestampTicks,
                _events![(headIndex + count - 1) % Capacity].TimestampTicks);
        }
        else
        {
            // Wrap-around case (including full buffer with headIndex > 0)
            var segment1 = new ReadOnlyMemory<Event>(_events!, headIndex, Capacity - headIndex);
            var segment2 = new ReadOnlyMemory<Event>(_events!, 0, tailIndex);
            
            return new PartitionView<Event>(
                segment1, 
                segment2, 
                count,
                _events![headIndex].TimestampTicks,
                _events![(tailIndex - 1 + Capacity) % Capacity].TimestampTicks);
        }
    }
      /// <summary>
    /// For SoA layout, provides direct access to the keys array.
    /// <summary>
    /// Returns a snapshot span of keys in the partition when using the SoA layout.
    /// </summary>
    /// <returns>
    /// A ReadOnlySpan&lt;KeyId&gt; containing the keys in logical order from head to tail.
    /// The returned span either references the internal keys array (no copy) when the range is contiguous,
    /// or references a newly allocated array containing a contiguous copy when the partition wraps.
    /// </returns>
    /// <exception cref="InvalidOperationException">Thrown if the partition is not using the SoA layout.</exception>
    public ReadOnlySpan<KeyId> GetKeysSpan()
    {
        if (Layout != StorageLayout.SoA)
        {
            throw new InvalidOperationException("Direct key access is only available for SoA layout");
        }

        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(Capacity, tail - head);
        if (count <= 0)
        {
            return ReadOnlySpan<KeyId>.Empty;
        }

        var headIndex = (int)(head % Capacity);
        
        if (headIndex + count <= Capacity)
        {
            // No wrap-around
            return new ReadOnlySpan<KeyId>(_keys!, headIndex, count);
        }
        else
        {
            // Wrap-around - need to copy to a contiguous array
            var result = new KeyId[count];
            for (int i = 0; i < count; i++)
            {
                result[i] = _keys![(headIndex + i) % Capacity];
            }
            return result;
        }
    }
      /// <summary>
    /// For SoA layout, provides direct access to the values array.
    /// <summary>
    /// Returns a read-only span of the partition's value elements (double) in logical order.
    /// </summary>
    /// <returns>
    /// A <see cref="ReadOnlySpan{Double}"/> containing up to Capacity most-recent values between the current head and tail.
    /// If the requested range is contiguous in the underlying storage, the span is a view over the internal values array; if the range wraps, a new contiguous array is allocated and the span refers to that array.
    /// </returns>
    /// <exception cref="InvalidOperationException">Thrown when the partition is not using the SoA storage layout.</exception>
    /// <remarks>
    /// The span represents a snapshot taken from volatile head/tail counters; concurrent writers may modify the underlying storage after this call, so callers should consume the span immediately and must not rely on it remaining stable across unrelated concurrent operations.
    /// The returned span's length is min(Capacity, tail - head). If the partition is empty, an empty span is returned.
    /// </remarks>
    public ReadOnlySpan<double> GetValuesSpan()
    {
        if (Layout != StorageLayout.SoA)
        {
            throw new InvalidOperationException("Direct value access is only available for SoA layout");
        }

        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(Capacity, tail - head);
        if (count <= 0)
        {
            return ReadOnlySpan<double>.Empty;
        }

        var headIndex = (int)(head % Capacity);
        
        if (headIndex + count <= Capacity)
        {
            // No wrap-around
            return new ReadOnlySpan<double>(_values!, headIndex, count);
        }
        else
        {
            // Wrap-around - need to copy to a contiguous array
            var result = new double[count];
            for (int i = 0; i < count; i++)
            {
                result[i] = _values![(headIndex + i) % Capacity];
            }
            return result;
        }
    }
      /// <summary>
    /// For SoA layout, provides direct access to the timestamps array.
    /// <summary>
    /// Returns a contiguous read-only span of stored timestamps for the current partition contents.
    /// </summary>
    /// <remarks>
    /// Requires the partition to be using the SoA layout. If the logical range is already contiguous in the underlying timestamps array, the returned span is a direct view into the internal storage (no allocation). If the logical range wraps around the end of the buffer, a new array is allocated and populated with the timestamps in chronological order and a span over that array is returned.
    /// An empty span is returned when the partition contains no items.
    /// </remarks>
    /// <returns>A ReadOnlySpan&lt;long&gt; containing the timestamps in chronological order.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the partition is not using the SoA layout.</exception>
    public ReadOnlySpan<long> GetTimestampsSpan()
    {
        if (Layout != StorageLayout.SoA)
        {
            throw new InvalidOperationException("Direct timestamp access is only available for SoA layout");
        }

        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(Capacity, tail - head);
        if (count <= 0)
        {
            return ReadOnlySpan<long>.Empty;
        }

        var headIndex = (int)(head % Capacity);
        
        if (headIndex + count <= Capacity)
        {
            // No wrap-around
            return new ReadOnlySpan<long>(_timestamps!, headIndex, count);
        }
        else
        {
            // Wrap-around - need to copy to a contiguous array
            var result = new long[count];
            for (int i = 0; i < count; i++)
            {
                result[i] = _timestamps![(headIndex + i) % Capacity];
            }
            return result;
        }
    }
    
    /// <summary>
    /// Purges all events from the partition.
    /// </summary>
    public void Clear()
    {
        Volatile.Write(ref _head, Volatile.Read(ref _tail));
    }
      /// <summary>
    /// For SoA layout, provides direct access to keys with zero allocation using pooled buffers for wrap-around case.
    /// <summary>
    /// Invokes the given <paramref name="processor"/> with a read-only span of the partition's keys in chronological order (oldest to newest).
    /// </summary>
    /// <remarks>
    /// Requires the partition to be using SoA (structure-of-arrays) storage; calling this on AoS will fail.
    /// If the requested range is contiguous in the underlying array this method supplies a direct span (no heap allocation). If the range wraps the circular buffer boundary a temporary buffer is rented from the shared pool and returned to the pool after <paramref name="processor"/> completes.
    /// </remarks>
    /// <param name="processor">Action that will receive a ReadOnlySpan of KeyId containing the current keys.</param>
    /// <exception cref="InvalidOperationException">Thrown when the partition is not using SoA layout.</exception>
    public void GetKeysZeroAlloc(Action<ReadOnlySpan<KeyId>> processor)
    {
        if (Layout != StorageLayout.SoA)
        {
            throw new InvalidOperationException("Direct key access is only available for SoA layout");
        }

        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(Capacity, tail - head);
        if (count <= 0)
        {
            return;
        }

        var headIndex = (int)(head % Capacity);
        
        if (headIndex + count <= Capacity)
        {
            // No wrap-around - direct access
            processor(new ReadOnlySpan<KeyId>(_keys!, headIndex, count));
        }
        else
        {
            // Wrap-around - use pooled buffer
            Buffers.WithRentedBuffer<KeyId>(count, buffer =>
            {
                for (int i = 0; i < count; i++)
                {
                    buffer[i] = _keys![(headIndex + i) % Capacity];
                }
                processor(buffer.AsSpan(0, count));
            }, ArrayPool<KeyId>.Shared);
        }
    }

    /// <summary>
    /// For SoA layout, provides direct access to values with zero allocation using pooled buffers for wrap-around case.
    /// <summary>
    /// Provides zero-allocation read access to the stored values (double) in SoA layout by passing a contiguous <see cref="ReadOnlySpan{Double}"/> to the supplied processor.
    /// </summary>
    /// <param name="processor">Synchronous callback that receives a span containing the current values in insertion order. The span is only valid for the duration of the callback and must not be stored.</param>
    /// <exception cref="InvalidOperationException">Thrown if the partition is not using SoA layout.</exception>
    /// <remarks>
    /// If the partition is empty the method returns without invoking <paramref name="processor"/>. The callback is invoked on the calling thread. In wrap-around cases a pooled buffer may be used internally to present a contiguous span, but callers should rely only on the span passed into the callback.
    /// </remarks>
    public void GetValuesZeroAlloc(Action<ReadOnlySpan<double>> processor)
    {
        if (Layout != StorageLayout.SoA)
        {
            throw new InvalidOperationException("Direct value access is only available for SoA layout");
        }

        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(Capacity, tail - head);
        if (count <= 0)
        {
            return;
        }

        var headIndex = (int)(head % Capacity);
        
        if (headIndex + count <= Capacity)
        {
            // No wrap-around - direct access
            processor(new ReadOnlySpan<double>(_values!, headIndex, count));
        }
        else
        {
            // Wrap-around - use pooled buffer
            Buffers.WithRentedBuffer<double>(count, buffer =>
            {
                for (int i = 0; i < count; i++)
                {
                    buffer[i] = _values![(headIndex + i) % Capacity];
                }
                processor(buffer.AsSpan(0, count));
            }, Buffers.DoublePool);
        }
    }

    /// <summary>
    /// For SoA layout, provides direct access to timestamps with zero allocation using pooled buffers for wrap-around case.
    /// <summary>
    /// Provides a zero-allocation read-only view of the partition's timestamps and passes it to <paramref name="processor"/>.
    /// </summary>
    /// <remarks>
    /// Requires the partition to use SoA layout. The method snapshots head and tail and, if non-empty, invokes
    /// <paramref name="processor"/> synchronously with a <see cref="ReadOnlySpan{T}"/> of the timestamps in logical order.
    /// The span may reference the internal timestamps array (no wrap-around) or a pooled temporary buffer (wrap-around).
    /// If the caller needs the data after this method returns, it must copy the span's contents.
    /// </remarks>
    /// <param name="processor">Callback that receives a read-only span of timestamps for the current contents.</param>
    /// <exception cref="InvalidOperationException">Thrown when the partition is not using SoA layout.</exception>
    public void GetTimestampsZeroAlloc(Action<ReadOnlySpan<long>> processor)
    {
        if (Layout != StorageLayout.SoA)
        {
            throw new InvalidOperationException("Direct timestamp access is only available for SoA layout");
        }

        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(Capacity, tail - head);
        if (count <= 0)
        {
            return;
        }

        var headIndex = (int)(head % Capacity);
        
        if (headIndex + count <= Capacity)
        {
            // No wrap-around - direct access
            processor(new ReadOnlySpan<long>(_timestamps!, headIndex, count));
        }
        else
        {
            // Wrap-around - use pooled buffer
            Buffers.WithRentedBuffer<long>(count, buffer =>
            {
                for (int i = 0; i < count; i++)
                {
                    buffer[i] = _timestamps![(headIndex + i) % Capacity];
                }
                processor(buffer.AsSpan(0, count));
            }, Buffers.LongPool);
        }
    }

    /// <summary>
    /// Zero-allocation view creation for SoA layout using pooled buffers.
    /// <summary>
    /// Invokes the provided <paramref name="processor"/> with a read-only view of the partition's current contents without allocating new long-lived buffers.
    /// </summary>
    /// <param name="processor">
    /// Action to receive the partition view. The call is synchronous; the view (and any memory it references) is only valid for the duration of the call.
    /// </param>
    /// <remarks>
    /// - If the partition is empty, an empty <see cref="PartitionView{Event}"/> is passed.
    /// - In AoS layout the returned <see cref="ReadOnlyMemory{Event}"/> segments reference the partition's internal buffer directly and must not be modified or retained after the callback returns.
    /// - In SoA layout events are reconstructed into a rented, pooled temporary array; that array is only valid during the callback and will be returned to the pool afterwards.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void GetViewZeroAlloc(Action<PartitionView<Event>> processor)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(Capacity, tail - head);
        if (count <= 0) 
        {
            processor(new PartitionView<Event>(
                ReadOnlyMemory<Event>.Empty, 
                ReadOnlyMemory<Event>.Empty, 
                0, 
                0, 
                0));
            return;
        }
          // If using SoA layout, use pooled buffer for temporary Event array
        if (Layout == StorageLayout.SoA)
        {
            Buffers.WithRentedBuffer<Event>(count, tempEvents =>
            {
                var headPos = (int)(head % Capacity);
                
                for (int i = 0; i < count; i++)
                {
                    var index = (headPos + i) % Capacity;
                    tempEvents[i] = new Event(_keys![index], _values![index], _timestamps![index]);
                }
                
                processor(new PartitionView<Event>(
                    new ReadOnlyMemory<Event>(tempEvents, 0, count),
                    ReadOnlyMemory<Event>.Empty,
                    count,
                    tempEvents[0].TimestampTicks,
                    tempEvents[count - 1].TimestampTicks));
            }, Buffers.EventPool);
        }
        else
        {
            // For AoS layout, we can use the existing buffer directly
            var headIndex = (int)(head % Capacity);
            var tailIndex = (int)(tail % Capacity);
            var isFull = count == Capacity;
            
            if (tailIndex > headIndex || (isFull && headIndex == 0))
            {
                // No wrap-around case (or full buffer starting at index 0)
                var segment = new ReadOnlyMemory<Event>(_events!, headIndex, count);
                
                processor(new PartitionView<Event>(
                    segment, 
                    ReadOnlyMemory<Event>.Empty, 
                    count,
                    _events![headIndex].TimestampTicks,
                    _events![(headIndex + count - 1) % Capacity].TimestampTicks));
            }
            else
            {
                // Wrap-around case (including full buffer with headIndex > 0)
                var segment1 = new ReadOnlyMemory<Event>(_events!, headIndex, Capacity - headIndex);
                var segment2 = new ReadOnlyMemory<Event>(_events!, 0, tailIndex);
                
                processor(new PartitionView<Event>(
                    segment1, 
                    segment2, 
                    count,
                    _events![headIndex].TimestampTicks,
                    _events![(tailIndex - 1 + Capacity) % Capacity].TimestampTicks));
            }
        }
    }
}
