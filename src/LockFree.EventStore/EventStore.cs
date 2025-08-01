using System.Collections.Concurrent;
using System.Threading;

namespace LockFree.EventStore;

/// <summary>
/// In-memory partitioned event store using lock-free ring buffers.
/// </summary>
public sealed class EventStore<TEvent>
{
    private readonly LockFreeRingBuffer<TEvent>[] _partitions;
    private readonly IEventTimestampSelector<TEvent>? _ts;

    /// <summary>
    /// Initializes a new instance with the provided options.
    /// </summary>
    public EventStore(EventStoreOptions<TEvent>? options = null)
    {
        options ??= new EventStoreOptions<TEvent>();
        if (options.Partitions <= 0)
            throw new ArgumentOutOfRangeException(nameof(options.Partitions));
        _partitions = new LockFreeRingBuffer<TEvent>[options.Partitions];
        for (int i = 0; i < _partitions.Length; i++)
            _partitions[i] = new LockFreeRingBuffer<TEvent>(options.CapacityPerPartition);
        _ts = options.TimestampSelector;
    }

    /// <summary>
    /// Number of partitions.
    /// </summary>
    public int Partitions => _partitions.Length;

    /// <summary>
    /// Approximate total number of events across partitions.
    /// </summary>
    public long CountApprox
    {
        get
        {
            long total = 0;
            foreach (var p in _partitions)
                total += p.CountApprox;
            return total;
        }
    }

    /// <summary>
    /// Appends an event using the default partitioner.
    /// </summary>
    public bool TryAppend(TEvent e)
    {
        var partition = Partitioners.ForKey(e, _partitions.Length);
        return TryAppend(e, partition);
    }

    /// <summary>
    /// Appends a batch of events using the default partitioner.
    /// </summary>
    public int TryAppend(ReadOnlySpan<TEvent> batch)
    {
        int written = 0;
        foreach (var e in batch)
        {
            TryAppend(e);
            written++;
        }
        return written;
    }

    /// <summary>
    /// Appends an event to the specified partition.
    /// </summary>
    public bool TryAppend(TEvent e, int partition)
    {
        if ((uint)partition >= (uint)_partitions.Length)
            throw new ArgumentOutOfRangeException(nameof(partition));
        return _partitions[partition].TryEnqueue(e);
    }

    /// <summary>
    /// Takes a snapshot of all partitions and returns an immutable list.
    /// </summary>
    public IReadOnlyList<TEvent> Snapshot()
    {
        var arrays = new (TEvent[] Buffer, int Length)[_partitions.Length];
        long total = 0;
        for (int i = 0; i < _partitions.Length; i++)
        {
            var buf = new TEvent[_partitions[i].Capacity];
            var len = _partitions[i].Snapshot(buf);
            arrays[i] = (buf, len);
            total += len;
        }
        var result = new TEvent[total];
        int idx = 0;
        foreach (var (buf, len) in arrays)
        {
            buf.AsSpan(0, len).CopyTo(result.AsSpan(idx));
            idx += len;
        }
        return result;
    }

    /// <summary>
    /// Returns an enumerable snapshot of all events.
    /// </summary>
    public IEnumerable<TEvent> EnumerateSnapshot()
    {
        foreach (var partition in _partitions)
        {
            foreach (var e in partition.EnumerateSnapshot())
                yield return e;
        }
    }

    private bool WithinWindow(TEvent e, DateTime? from, DateTime? to)
    {
        if (_ts is null)
            return true;
        var ts = _ts.GetTimestamp(e);
        if (from.HasValue && ts < from.Value)
            return false;
        if (to.HasValue && ts > to.Value)
            return false;
        return true;
    }

    /// <summary>
    /// Queries events by optional filter and time window.
    /// </summary>
    public IEnumerable<TEvent> Query(Predicate<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        foreach (var partition in _partitions)
        {
            foreach (var e in partition.EnumerateSnapshot())
            {
                if (!WithinWindow(e, from, to))
                    continue;
                if (filter is null || filter(e))
                    yield return e;
            }
        }
    }

    /// <summary>
    /// Aggregates all events using the specified fold function.
    /// </summary>
    public TAcc Aggregate<TAcc>(Func<TAcc> seed, Func<TAcc, TEvent, TAcc> fold, Predicate<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
    {
        var acc = seed();
        foreach (var e in Query(filter, from, to))
        {
            acc = fold(acc, e);
        }
        return acc;
    }

    /// <summary>
    /// Aggregates events grouped by a key.
    /// </summary>
    public Dictionary<TKey, TAcc> AggregateBy<TKey, TAcc>(Func<TEvent, TKey> groupBy, Func<TAcc> seed, Func<TAcc, TEvent, TAcc> fold, Predicate<TEvent>? filter = null, DateTime? from = null, DateTime? to = null)
        where TKey : notnull
    {
        var dict = new Dictionary<TKey, TAcc>();
        foreach (var e in Query(filter, from, to))
        {
            var key = groupBy(e);
            if (!dict.TryGetValue(key, out var acc))
            {
                acc = seed();
                dict[key] = acc;
            }
            dict[key] = fold(acc, e);
        }
        return dict;
    }
}
