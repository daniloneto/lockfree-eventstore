using System.Collections.Concurrent;
using System.Threading.Channels;
using LockFree.EventStore;

#if DISRUPTOR
using Disruptor;
using Disruptor.Dsl;
#endif

namespace LockfreeEventStore.Benchmarks.Competitors;

/// <summary>
/// Base interface for all benchmark competitors.
/// </summary>
public interface IEventStoreCompetitor : IDisposable
{
    string Name { get; }
    bool TryAppend(BenchmarkEvent evt);
    int Capacity { get; }
    long Count { get; }
    void Clear();
    
    // Query operations
    long CountEvents(DateTime? from = null, DateTime? to = null);
    long CountEvents(Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null);
    double Sum(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null);
    double Sum(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null);
    double Average(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null);
    double Average(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null);
    double? Min(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null);
    double? Min(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null);
    double? Max(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null);
    double? Max(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null);
    IReadOnlyList<BenchmarkEvent> Snapshot();
}

/// <summary>
/// EventStore competitor using the actual LockFree.EventStore library.
/// </summary>
public sealed class EventStoreCompetitor : IEventStoreCompetitor
{
    private readonly EventStore<BenchmarkEvent> _store;
    
    public string Name => "LockFree.EventStore";
    public int Capacity => _store.Capacity;
    public long Count => _store.Count;
    
    public EventStoreCompetitor(int capacity, int partitions = 8)
    {
        var options = new EventStoreOptions<BenchmarkEvent>
        {
            Capacity = capacity,
            Partitions = partitions,
            TimestampSelector = new BenchmarkEventTimestampSelector()
        };
        _store = new EventStore<BenchmarkEvent>(options);
    }
    
    public bool TryAppend(BenchmarkEvent evt) => _store.TryAppend(evt);
    public void Clear() => _store.Clear();
    public IReadOnlyList<BenchmarkEvent> Snapshot() => _store.Snapshot();
    
    public long CountEvents(DateTime? from = null, DateTime? to = null) => _store.CountEvents(from, to);
    public long CountEvents(Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) => _store.CountEvents(filter, from, to);
    
    public double Sum(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) => _store.Sum(selector, from, to);
    public double Sum(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) => _store.Sum(selector, filter, from, to);
    
    public double Average(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) => _store.Average(selector, from, to);
    public double Average(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) => _store.Average(selector, filter, from, to);
    
    public double? Min(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) => _store.Min(selector, from, to);
    public double? Min(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) => _store.Min(selector, filter, from, to);
    
    public double? Max(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) => _store.Max(selector, from, to);
    public double? Max(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) => _store.Max(selector, filter, from, to);
    
    public void Dispose() { /* EventStore doesn't need disposal */ }
}

/// <summary>
/// Competitor using System.Threading.Channels with bounded capacity.
/// </summary>
public sealed class ChannelBoundedCompetitor : IEventStoreCompetitor
{
    private readonly Channel<BenchmarkEvent> _channel;
    private readonly ChannelWriter<BenchmarkEvent> _writer;
    private readonly List<BenchmarkEvent> _buffer;
    private readonly Timer _aggregatorTimer;
    private readonly object _bufferLock = new();
    private volatile bool _disposed;
    
    public string Name => "Channel.Bounded";
    public int Capacity { get; }
    public long Count { get; private set; }
    
    public ChannelBoundedCompetitor(int capacity)
    {
        Capacity = capacity;
        _buffer = new List<BenchmarkEvent>(capacity);
        
        var options = new BoundedChannelOptions(capacity)
        {
            AllowSynchronousContinuations = false,
            SingleReader = false,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest
        };
        
        _channel = Channel.CreateBounded<BenchmarkEvent>(options);
        _writer = _channel.Writer;
        
        // Background consumer to maintain window
        _aggregatorTimer = new Timer(ConsumeEvents, null, TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(10));
    }
    
    public bool TryAppend(BenchmarkEvent evt)
    {
        if (_disposed) return false;
        return _writer.TryWrite(evt);
    }
    
    private void ConsumeEvents(object? state)
    {
        if (_disposed) return;
        
        var reader = _channel.Reader;
        var consumed = new List<BenchmarkEvent>();
        
        while (reader.TryRead(out var evt))
        {
            consumed.Add(evt);
            if (consumed.Count >= 1000) break; // Batch processing
        }
        
        if (consumed.Count > 0)
        {
            lock (_bufferLock)
            {
                _buffer.AddRange(consumed);
                Count += consumed.Count;
                
                // Keep only recent events (simulate capacity)
                if (_buffer.Count > Capacity)
                {
                    var toRemove = _buffer.Count - Capacity;
                    _buffer.RemoveRange(0, toRemove);
                    Count = _buffer.Count;
                }
            }
        }
    }
    
    public void Clear()
    {
        lock (_bufferLock)
        {
            _buffer.Clear();
            Count = 0;
        }
    }
    
    public IReadOnlyList<BenchmarkEvent> Snapshot()
    {
        lock (_bufferLock)
        {
            return _buffer.ToArray();
        }
    }
    
    private IEnumerable<BenchmarkEvent> Query(Func<BenchmarkEvent, bool>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        lock (_bufferLock)
        {
            return _buffer.Where(evt =>
            {
                if (from.HasValue && evt.Timestamp < from.Value) return false;
                if (to.HasValue && evt.Timestamp > to.Value) return false;
                return filter?.Invoke(evt) ?? true;
            }).ToList();
        }
    }
    
    public long CountEvents(DateTime? from = null, DateTime? to = null) => Query(from: from, to: to).Count();
    public long CountEvents(Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) => Query(filter, from, to).Count();
    
    public double Sum(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) => Query(from: from, to: to).Sum(selector);
    public double Sum(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) => Query(filter, from, to).Sum(selector);
    
    public double Average(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(from: from, to: to).ToList();
        return items.Any() ? items.Average(selector) : 0.0;
    }
    
    public double Average(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(filter, from, to).ToList();
        return items.Any() ? items.Average(selector) : 0.0;
    }
    
    public double? Min(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(from: from, to: to).ToList();
        return items.Any() ? items.Min(selector) : null;
    }
    
    public double? Min(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(filter, from, to).ToList();
        return items.Any() ? items.Min(selector) : null;
    }
    
    public double? Max(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(from: from, to: to).ToList();
        return items.Any() ? items.Max(selector) : null;
    }
    
    public double? Max(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(filter, from, to).ToList();
        return items.Any() ? items.Max(selector) : null;
    }
    
    public void Dispose()
    {
        _disposed = true;
        _aggregatorTimer?.Dispose();
        _writer.Complete();
    }
}

/// <summary>
/// Competitor using ConcurrentQueue with manual capacity management.
/// </summary>
public sealed class ConcurrentQueueCompetitor : IEventStoreCompetitor
{
    private readonly ConcurrentQueue<BenchmarkEvent> _queue;
    private volatile int _count;
    
    public string Name => "ConcurrentQueue";
    public int Capacity { get; }
    public long Count => _count;
    
    public ConcurrentQueueCompetitor(int capacity)
    {
        Capacity = capacity;
        _queue = new ConcurrentQueue<BenchmarkEvent>();
    }
    
    public bool TryAppend(BenchmarkEvent evt)
    {
        _queue.Enqueue(evt);
        Interlocked.Increment(ref _count);
        
        // Drop oldest if over capacity
        while (_count > Capacity && _queue.TryDequeue(out _))
        {
            Interlocked.Decrement(ref _count);
        }
        
        return true;
    }
    
    public void Clear()
    {
        while (_queue.TryDequeue(out _)) { }
        _count = 0;
    }
    
    public IReadOnlyList<BenchmarkEvent> Snapshot()
    {
        return _queue.ToArray();
    }
    
    private IEnumerable<BenchmarkEvent> Query(Func<BenchmarkEvent, bool>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        return _queue.Where(evt =>
        {
            if (from.HasValue && evt.Timestamp < from.Value) return false;
            if (to.HasValue && evt.Timestamp > to.Value) return false;
            return filter?.Invoke(evt) ?? true;
        });
    }
    
    public long CountEvents(DateTime? from = null, DateTime? to = null) => Query(from: from, to: to).Count();
    public long CountEvents(Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) => Query(filter, from, to).Count();
    
    public double Sum(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) => Query(from: from, to: to).Sum(selector);
    public double Sum(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) => Query(filter, from, to).Sum(selector);
    
    public double Average(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(from: from, to: to).ToList();
        return items.Any() ? items.Average(selector) : 0.0;
    }
    
    public double Average(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(filter, from, to).ToList();
        return items.Any() ? items.Average(selector) : 0.0;
    }
    
    public double? Min(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(from: from, to: to).ToList();
        return items.Any() ? items.Min(selector) : null;
    }
    
    public double? Min(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(filter, from, to).ToList();
        return items.Any() ? items.Min(selector) : null;
    }
    
    public double? Max(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(from: from, to: to).ToList();
        return items.Any() ? items.Max(selector) : null;
    }
    
    public double? Max(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(filter, from, to).ToList();
        return items.Any() ? items.Max(selector) : null;
    }
    
    public void Dispose() { }
}

#if DISRUPTOR
/// <summary>
/// Competitor using Disruptor ring buffer.
/// </summary>
public sealed class DisruptorCompetitor : IEventStoreCompetitor
{
    private readonly RingBuffer<BenchmarkEventWrapper> _ringBuffer;
    private readonly Disruptor<BenchmarkEventWrapper> _disruptor;
    private readonly List<BenchmarkEvent> _buffer;
    private readonly object _bufferLock = new();
    
    public string Name => "Disruptor";
    public int Capacity { get; }
    public long Count => _buffer.Count;
    
    public DisruptorCompetitor(int capacity)
    {
        Capacity = capacity;
        _buffer = new List<BenchmarkEvent>(capacity);
        
        var ringBufferSize = 1;
        while (ringBufferSize < capacity) ringBufferSize <<= 1; // Next power of 2
        
        _disruptor = new Disruptor<BenchmarkEventWrapper>(() => new BenchmarkEventWrapper(), ringBufferSize);
        _disruptor.HandleEventsWith(new BenchmarkEventHandler(_buffer, _bufferLock, capacity));
        _disruptor.Start();
        _ringBuffer = _disruptor.RingBuffer;
    }
    
    public bool TryAppend(BenchmarkEvent evt)
    {
        var sequence = _ringBuffer.Next();
        var entry = _ringBuffer[sequence];
        entry.Event = evt;
        _ringBuffer.Publish(sequence);
        return true;
    }
    
    public void Clear()
    {
        lock (_bufferLock)
        {
            _buffer.Clear();
        }
    }
    
    public IReadOnlyList<BenchmarkEvent> Snapshot()
    {
        lock (_bufferLock)
        {
            return _buffer.ToArray();
        }
    }
    
    private IEnumerable<BenchmarkEvent> Query(Func<BenchmarkEvent, bool>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        lock (_bufferLock)
        {
            return _buffer.Where(evt =>
            {
                if (from.HasValue && evt.Timestamp < from.Value) return false;
                if (to.HasValue && evt.Timestamp > to.Value) return false;
                return filter?.Invoke(evt) ?? true;
            }).ToList();
        }
    }
    
    public long CountEvents(DateTime? from = null, DateTime? to = null) => Query(from: from, to: to).Count();
    public long CountEvents(Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) => Query(filter, from, to).Count();
    
    public double Sum(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) => Query(from: from, to: to).Sum(selector);
    public double Sum(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) => Query(filter, from, to).Sum(selector);
    
    public double Average(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(from: from, to: to).ToList();
        return items.Any() ? items.Average(selector) : 0.0;
    }
    
    public double Average(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(filter, from, to).ToList();
        return items.Any() ? items.Average(selector) : 0.0;
    }
    
    public double? Min(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(from: from, to: to).ToList();
        return items.Any() ? items.Min(selector) : null;
    }
    
    public double? Min(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(filter, from, to).ToList();
        return items.Any() ? items.Min(selector) : null;
    }
    
    public double? Max(Func<BenchmarkEvent, double> selector, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(from: from, to: to).ToList();
        return items.Any() ? items.Max(selector) : null;
    }
    
    public double? Max(Func<BenchmarkEvent, double> selector, Func<BenchmarkEvent, bool> filter, DateTime? from = null, DateTime? to = null) 
    {
        var items = Query(filter, from, to).ToList();
        return items.Any() ? items.Max(selector) : null;
    }
    
    public void Dispose()
    {
        _disruptor?.Shutdown();
    }
}

public class BenchmarkEventWrapper
{
    public BenchmarkEvent Event { get; set; }
}

public class BenchmarkEventHandler : IEventHandler<BenchmarkEventWrapper>
{
    private readonly List<BenchmarkEvent> _buffer;
    private readonly object _bufferLock;
    private readonly int _capacity;
    
    public BenchmarkEventHandler(List<BenchmarkEvent> buffer, object bufferLock, int capacity)
    {
        _buffer = buffer;
        _bufferLock = bufferLock;
        _capacity = capacity;
    }
    
    public void OnEvent(BenchmarkEventWrapper data, long sequence, bool endOfBatch)
    {
        lock (_bufferLock)
        {
            _buffer.Add(data.Event);
            
            // Keep capacity bounded
            if (_buffer.Count > _capacity)
            {
                var toRemove = _buffer.Count - _capacity;
                _buffer.RemoveRange(0, toRemove);
            }
        }
    }
}
#endif
