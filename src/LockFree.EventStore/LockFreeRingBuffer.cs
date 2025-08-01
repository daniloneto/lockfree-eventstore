using System.Threading;

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

    /// <summary>
    /// Initializes a new instance with the specified capacity.
    /// </summary>
    public LockFreeRingBuffer(int capacity)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity));
        _capacity = capacity;
        _buffer = new T[capacity];
    }

    /// <summary>
    /// Total capacity.
    /// </summary>
    public int Capacity => _capacity;    /// <summary>
    /// Approximate count of items currently in the buffer.
    /// </summary>
    public long CountApprox => Math.Max(0, Math.Min(_capacity, Volatile.Read(ref _tail) - Volatile.Read(ref _head)));

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
    }

    private void AdvanceHeadIfNeeded(long tail)
    {
        while (true)
        {
            var head = Volatile.Read(ref _head);
            if (tail - head <= _capacity)
                break;
            if (Interlocked.CompareExchange(ref _head, head + 1, head) == head)
                break;
        }
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
}
