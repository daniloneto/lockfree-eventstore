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
    private readonly int _capacity;
    private readonly StorageLayout _layout;
    private readonly Action<Event>? _onItemDiscarded;
    
    /// <summary>
    /// Initializes a new instance with the specified capacity and layout.
    /// </summary>
    public OptimizedPartition(int capacity, StorageLayout layout = StorageLayout.AoS, Action<Event>? onItemDiscarded = null)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity));
        
        _capacity = capacity;
        _layout = layout;
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
    public StorageLayout Layout => _layout;
    
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
    /// Enqueues a single event.
    /// </summary>    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(Event item)
    {
        var tail = Interlocked.Increment(ref _tail);
        var index = (int)((tail - 1) % _capacity);
        
        if (_layout == StorageLayout.AoS)
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
            Interlocked.Increment(ref _epoch);
        }
        
        return true;
    }
      /// <summary>
    /// Enqueues a batch of events with optimized epoch updating.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int TryEnqueueBatch(ReadOnlySpan<Event> batch)
    {
        if (batch.IsEmpty) return 0;
        
        // Reserve space for the entire batch atomically
        var startTail = Interlocked.Add(ref _tail, batch.Length);
        
        // Write all items to their reserved positions
        if (_layout == StorageLayout.AoS)
        {
            for (int i = 0; i < batch.Length; i++)
            {
                var position = startTail - batch.Length + i;
                _events![position % _capacity] = batch[i];
            }
        }
        else
        {
            for (int i = 0; i < batch.Length; i++)
            {
                var position = startTail - batch.Length + i;
                var index = (int)(position % _capacity);
                
                _keys![index] = batch[i].Key;
                _values![index] = batch[i].Value;
                _timestamps![index] = batch[i].TimestampTicks;
            }
        }
        
        // Advance head if needed (may overwrite oldest entries)
        AdvanceHeadIfNeeded(startTail);
        
        // Update epoch once for the entire batch
        Interlocked.Increment(ref _epoch);
        
        return batch.Length;
    }
    
    /// <summary>
    /// Advances the head pointer if needed to avoid buffer overflow.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]    private void AdvanceHeadIfNeeded(long tail)
    {
        var head = Volatile.Read(ref _head);
        
        while (tail - head > _capacity)
        {
            // Discard oldest events if necessary
            var expectedHead = head;
            var newHead = head + 1;
            
            if (Interlocked.CompareExchange(ref _head, newHead, expectedHead) == expectedHead)
            {
                if (_onItemDiscarded != null)
                {
                    var index = (int)(head % _capacity);
                    var eventItem = GetEventAt(index);
                    _onItemDiscarded(eventItem);
                }
                
                break;
            }
            
            head = Volatile.Read(ref _head);
        }
    }
    
    /// <summary>
    /// Gets an event from a specific index.
    /// </summary>    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Event GetEventAt(int index)
    {
        if (_layout == StorageLayout.AoS)
        {
            return _events![index];
        }
        else
        {
            return new Event(_keys![index], _values![index], _timestamps![index]);
        }
    }
      /// <summary>
    /// Gets a read-only view of the partition.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PartitionView<Event> GetView()
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(_capacity, tail - head);
        if (count <= 0) 
            return new PartitionView<Event>(
                ReadOnlyMemory<Event>.Empty, 
                ReadOnlyMemory<Event>.Empty, 
                0, 
                0, 
                0);
          // If using SoA layout, we need to create a temporary Event array
        if (_layout == StorageLayout.SoA)
        {
            var tempEvents = new Event[count];
            var headPos = (int)(head % _capacity);
            
            for (int i = 0; i < count; i++)
            {
                var index = (headPos + i) % _capacity;
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
        var headIndex = (int)(head % _capacity);
        var tailIndex = (int)(tail % _capacity);
        
        if (tailIndex > headIndex || (tailIndex == headIndex && count == _capacity))
        {
            // No wrap-around case
            var segment = new ReadOnlyMemory<Event>(_events!, headIndex, count);
            
            return new PartitionView<Event>(
                segment, 
                ReadOnlyMemory<Event>.Empty, 
                count,
                _events![headIndex].TimestampTicks,
                _events![(headIndex + count - 1) % _capacity].TimestampTicks);
        }
        else
        {
            // Wrap-around case
            var segment1 = new ReadOnlyMemory<Event>(_events!, headIndex, _capacity - headIndex);
            var segment2 = new ReadOnlyMemory<Event>(_events!, 0, tailIndex);
            
            return new PartitionView<Event>(
                segment1, 
                segment2, 
                count,
                _events![headIndex].TimestampTicks,
                _events![(tailIndex - 1 + _capacity) % _capacity].TimestampTicks);
        }
    }
      /// <summary>
    /// For SoA layout, provides direct access to the keys array.
    /// </summary>
    public ReadOnlySpan<KeyId> GetKeysSpan()
    {
        if (_layout != StorageLayout.SoA)
            throw new InvalidOperationException("Direct key access is only available for SoA layout");
        
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(_capacity, tail - head);
        if (count <= 0) return ReadOnlySpan<KeyId>.Empty;
        
        var headIndex = (int)(head % _capacity);
        
        if (headIndex + count <= _capacity)
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
                result[i] = _keys![(headIndex + i) % _capacity];
            }
            return result;
        }
    }
      /// <summary>
    /// For SoA layout, provides direct access to the values array.
    /// </summary>
    public ReadOnlySpan<double> GetValuesSpan()
    {
        if (_layout != StorageLayout.SoA)
            throw new InvalidOperationException("Direct value access is only available for SoA layout");
        
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(_capacity, tail - head);
        if (count <= 0) return ReadOnlySpan<double>.Empty;
        
        var headIndex = (int)(head % _capacity);
        
        if (headIndex + count <= _capacity)
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
                result[i] = _values![(headIndex + i) % _capacity];
            }
            return result;
        }
    }
      /// <summary>
    /// For SoA layout, provides direct access to the timestamps array.
    /// </summary>
    public ReadOnlySpan<long> GetTimestampsSpan()
    {
        if (_layout != StorageLayout.SoA)
            throw new InvalidOperationException("Direct timestamp access is only available for SoA layout");
        
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        
        int count = (int)Math.Min(_capacity, tail - head);
        if (count <= 0) return ReadOnlySpan<long>.Empty;
        
        var headIndex = (int)(head % _capacity);
        
        if (headIndex + count <= _capacity)
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
                result[i] = _timestamps![(headIndex + i) % _capacity];
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
}
