using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace LockFree.EventStore;

/// <summary>
/// Specialized EventStore implementation optimized for Event value type with contiguous memory layout.
/// Uses SoA (Structure of Arrays) pattern internally for better cache locality.
/// </summary>
public sealed class EventStoreV2
{
    private readonly Event[][] _partitionBuffers;
    private readonly long[] _partitionHeads;
    private readonly long[] _partitionTails;
    private readonly int[] _partitionEpochs;
    private readonly int _capacity;
    private readonly int _partitionCount;
    private readonly EventStoreOptions _options;
    private readonly EventStoreStatistics _statistics;
    private readonly KeyMap _keyMap;
    
    // Window state per partition for incremental aggregation
    private readonly PartitionWindowState[] _windowStates;    /// <summary>
    /// Initializes a new instance with default options.
    /// </summary>
    public EventStoreV2() : this(null) { }

    /// <summary>
    /// Initializes a new instance with specified capacity.
    /// </summary>
    public EventStoreV2(int capacity) : this(new EventStoreOptions { Capacity = capacity }) { }

    /// <summary>
    /// Initializes a new instance with specified capacity and partitions.
    /// </summary>
    public EventStoreV2(int capacity, int partitions) : this(new EventStoreOptions 
    { 
        Capacity = capacity, 
        Partitions = partitions 
    }) { }

    /// <summary>
    /// Initializes a new instance with the provided options.
    /// </summary>
    public EventStoreV2(EventStoreOptions? options = null)
    {
        _options = options ?? new EventStoreOptions();
        if (_options.Partitions <= 0)
            throw new ArgumentOutOfRangeException(nameof(_options.Partitions));
        
        _statistics = new EventStoreStatistics();
        _keyMap = new KeyMap();
        _partitionCount = _options.Partitions;
        
        var capacityPerPartition = _options.Capacity.HasValue 
            ? Math.Max(1, _options.Capacity.Value / _options.Partitions)
            : _options.CapacityPerPartition;
        
        _capacity = capacityPerPartition * _partitionCount;
        
        // Allocate contiguous arrays for partitions
        _partitionBuffers = new Event[_partitionCount][];
        _partitionHeads = new long[_partitionCount];
        _partitionTails = new long[_partitionCount];
        _partitionEpochs = new int[_partitionCount];
        
        for (int i = 0; i < _partitionCount; i++)
        {
            _partitionBuffers[i] = new Event[capacityPerPartition];
            _partitionHeads[i] = 0;
            _partitionTails[i] = 0;
            _partitionEpochs[i] = 0;
        }
        
        _windowStates = new PartitionWindowState[_partitionCount];
        for (int i = 0; i < _windowStates.Length; i++)
        {
            _windowStates[i].Reset();
        }
    }

    /// <summary>
    /// Number of partitions.
    /// </summary>
    public int Partitions => _partitionCount;

    /// <summary>
    /// Total configured capacity across all partitions.
    /// </summary>
    public int Capacity => _capacity;

    /// <summary>
    /// Approximate total number of events across partitions.
    /// </summary>
    public long CountApprox
    {
        get
        {
            long total = 0;
            for (int i = 0; i < _partitionCount; i++)
            {
                total += Math.Max(0, Math.Min(_partitionBuffers[i].Length, 
                    Volatile.Read(ref _partitionTails[i]) - Volatile.Read(ref _partitionHeads[i])));
            }
            return total;
        }
    }

    /// <summary>
    /// Approximate total number of events across partitions (alias for CountApprox).
    /// </summary>
    public long Count => CountApprox;

    /// <summary>
    /// Whether the store is empty (approximate).
    /// </summary>
    public bool IsEmpty => CountApprox == 0;

    /// <summary>
    /// Event store statistics.
    /// </summary>
    public EventStoreStatistics Statistics => _statistics;
    
    /// <summary>
    /// Adds an event to the store.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(string key, double value)
    {
        var keyId = _keyMap.GetOrAdd(key);
        Add(keyId, value, DateTime.UtcNow.Ticks);
    }
    
    /// <summary>
    /// Adds an event to the store with a specific timestamp.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(string key, double value, DateTime timestamp)
    {
        var keyId = _keyMap.GetOrAdd(key);
        Add(keyId, value, timestamp.Ticks);
    }
    
    /// <summary>
    /// Adds an event to the store using KeyId for optimized hot path.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(KeyId key, double value)
    {
        Add(key, value, DateTime.UtcNow.Ticks);
    }
    
    /// <summary>
    /// Adds an event to the store using KeyId and specific timestamp.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(KeyId key, double value, long timestampTicks)
    {
        var ev = new Event(key, value, timestampTicks);
        
        // Partition based on key hash
        int partitionIndex = Math.Abs(key.GetHashCode()) % _partitionCount;
        
        var buffer = _partitionBuffers[partitionIndex];
        var tail = Interlocked.Increment(ref _partitionTails[partitionIndex]);
        var index = (int)((tail - 1) % buffer.Length);
        
        buffer[index] = ev;
        
        // Advance head if needed (overwrites oldest when full)
        AdvanceHeadIfNeeded(partitionIndex, tail);
        
        // Update epoch sparingly
        if ((tail & 0xFF) == 0)
        {
            Interlocked.Increment(ref _partitionEpochs[partitionIndex]);
        }
        
        _statistics.IncrementTotalAdded();
    }
    
    /// <summary>
    /// Adds a batch of events efficiently.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddBatch(string key, ReadOnlySpan<double> values)
    {
        var keyId = _keyMap.GetOrAdd(key);
        AddBatch(keyId, values, DateTime.UtcNow.Ticks);
    }
    
    /// <summary>
    /// Adds a batch of events efficiently with timestamp.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddBatch(string key, ReadOnlySpan<double> values, DateTime timestamp)
    {
        var keyId = _keyMap.GetOrAdd(key);
        AddBatch(keyId, values, timestamp.Ticks);
    }
    
    /// <summary>
    /// Adds a batch of events efficiently using KeyId for optimized hot path.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddBatch(KeyId key, ReadOnlySpan<double> values, long timestampTicks)
    {
        if (values.IsEmpty) return;
        
        // Partition based on key hash
        int partitionIndex = Math.Abs(key.GetHashCode()) % _partitionCount;
        
        // Reserve space for the entire batch atomically
        var buffer = _partitionBuffers[partitionIndex];
        var startTail = Interlocked.Add(ref _partitionTails[partitionIndex], values.Length);
        
        // Write all items to their reserved positions
        for (int i = 0; i < values.Length; i++)
        {
            var position = startTail - values.Length + i;
            var index = (int)(position % buffer.Length);
            buffer[index] = new Event(key, values[i], timestampTicks);
        }
        
        // Advance head if needed (may overwrite oldest entries)
        AdvanceHeadIfNeeded(partitionIndex, startTail);
        
        // Update epoch once for the entire batch
        Interlocked.Increment(ref _partitionEpochs[partitionIndex]);
        
        _statistics.IncrementTotalAdded(values.Length);
    }
    
    /// <summary>
    /// Advances the head pointer if needed to avoid buffer overflow.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AdvanceHeadIfNeeded(int partitionIndex, long tail)
    {
        var buffer = _partitionBuffers[partitionIndex];
        var capacity = buffer.Length;
        var head = Volatile.Read(ref _partitionHeads[partitionIndex]);
        
        while (tail - head > capacity)
        {
            // Discard oldest events if necessary
            var expectedHead = head;
            var newHead = head + 1;
            
            if (Interlocked.CompareExchange(ref _partitionHeads[partitionIndex], newHead, expectedHead) == expectedHead)
            {
                _statistics.IncrementOverwritten();
                
                if (_options.OnEventDiscarded != null)
                {
                    var index = (int)(head % capacity);
                    _options.OnEventDiscarded(buffer[index]);
                }
                
                break;
            }
            
            head = Volatile.Read(ref _partitionHeads[partitionIndex]);
        }
    }
    
    /// <summary>
    /// Gets a read-only view of a partition.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PartitionView<Event> GetPartitionView(int partitionIndex)
    {
        if (partitionIndex < 0 || partitionIndex >= _partitionCount)
            throw new ArgumentOutOfRangeException(nameof(partitionIndex));
        
        var buffer = _partitionBuffers[partitionIndex];
        var head = Volatile.Read(ref _partitionHeads[partitionIndex]);
        var tail = Volatile.Read(ref _partitionTails[partitionIndex]);
        var capacity = buffer.Length;
        
        int count = (int)Math.Min(capacity, tail - head);
        if (count <= 0) 
            return new PartitionView<Event>(
                ReadOnlyMemory<Event>.Empty, 
                ReadOnlyMemory<Event>.Empty, 
                0, 
                0, 
                0);
        
        var headIndex = (int)(head % capacity);
        var tailIndex = (int)(tail % capacity);
        
        if (tailIndex > headIndex || (tailIndex == headIndex && count == capacity))
        {
            // No wrap-around case
            var segment = new ReadOnlyMemory<Event>(buffer, headIndex, count);
            
            var firstEvent = buffer[headIndex];
            var lastEvent = buffer[(headIndex + count - 1) % capacity];
            
            return new PartitionView<Event>(
                segment, 
                ReadOnlyMemory<Event>.Empty, 
                count,
                firstEvent.TimestampTicks,
                lastEvent.TimestampTicks);
        }
        else
        {
            // Wrap-around case
            var segment1 = new ReadOnlyMemory<Event>(buffer, headIndex, capacity - headIndex);
            var segment2 = new ReadOnlyMemory<Event>(buffer, 0, tailIndex);
            
            var firstEvent = buffer[headIndex];
            var lastEvent = buffer[(tailIndex - 1 + capacity) % capacity];
            
            return new PartitionView<Event>(
                segment1, 
                segment2, 
                count,
                firstEvent.TimestampTicks,
                lastEvent.TimestampTicks);
        }
    }
    
    /// <summary>
    /// Purges all events from the store.
    /// </summary>
    public void Clear()
    {
        for (int i = 0; i < _partitionCount; i++)
        {
            Volatile.Write(ref _partitionHeads[i], Volatile.Read(ref _partitionTails[i]));
            _windowStates[i].Reset();
        }
    }
}
