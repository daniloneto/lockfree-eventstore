using BenchmarkDotNet.Attributes;
using LockfreeEventStore.Benchmarks.Competitors;
using LockfreeEventStore.Benchmarks.Utils;

namespace LockfreeEventStore.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for window aggregation scenarios with producers and query operations.
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class WindowAggregateBenchmarks
{    [Params(2, 4)]
    public int ProducerCount { get; set; }

    [Params(10_000)]
    public int Capacity { get; set; }

    [Params(100)]
    public int WindowSizeMs { get; set; }

    [Params(0.0, 0.8)]
    public double Skew { get; set; }

    [Params(1.0, 0.1)]
    public double FilterSelectivity { get; set; }

    [Params("EventStore", "ChannelBounded", "ConcurrentQueue"
#if DISRUPTOR
        , "Disruptor"
#endif
    )]
    public string Competitor { get; set; } = "EventStore";

    private IEventStoreCompetitor _store = null!;
    private BenchmarkEvent[] _events = null!;
    private CancellationTokenSource _cts = null!;
    private Func<BenchmarkEvent, bool> _filter = null!;
      private const int EventsPerProducer = 1_000;
    private const int KeyCount = 100;
    private const int QueryIntervalMs = 10;

    [GlobalSetup]
    public void Setup()
    {
        _store = CreateCompetitor();
        _events = WorkloadGenerator.GenerateEvents(
            EventsPerProducer * ProducerCount, 
            KeyCount, 
            Skew);
        _filter = WorkloadGenerator.CreateSelectivityFilter(FilterSelectivity);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _store?.Dispose();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _store.Clear();
        _cts = new CancellationTokenSource();
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _cts?.Cancel();
        _cts?.Dispose();
    }    [Benchmark]
    public long ProducersWithWindowQueries()    {
        var eventsPerThread = EventsPerProducer;
        var totalEvents = eventsPerThread * ProducerCount;
        var windowTimeSpan = TimeSpan.FromMilliseconds(WindowSizeMs);
        long totalQueries = 0;

        // Start query task
        var queryTask = Task.Run(() =>
        {
            long queryCount = 0;
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var now = DateTime.UtcNow;
                    var windowStart = now - windowTimeSpan;

                    // Perform various aggregations
                    if (FilterSelectivity >= 1.0)
                    {
                        // No filter
                        _ = _store.CountEvents(windowStart, now);
                        _ = _store.Sum(e => e.Value, windowStart, now);
                        _ = _store.Average(e => e.Value, windowStart, now);
                        _ = _store.Min(e => e.Value, windowStart, now);
                        _ = _store.Max(e => e.Value, windowStart, now);
                        queryCount += 5;
                    }
                    else
                    {
                        // With filter
                        _ = _store.CountEvents(_filter, windowStart, now);
                        _ = _store.Sum(e => e.Value, _filter, windowStart, now);
                        _ = _store.Average(e => e.Value, _filter, windowStart, now);
                        _ = _store.Min(e => e.Value, _filter, windowStart, now);
                        _ = _store.Max(e => e.Value, _filter, windowStart, now);
                        queryCount += 5;
                    }

                    Thread.Sleep(QueryIntervalMs);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
            return queryCount;
        });

        // Start producers
        var producerTasks = new Task[ProducerCount];
        for (int p = 0; p < ProducerCount; p++)
        {
            int producerId = p;
            producerTasks[p] = Task.Run(() =>
            {
                var startIndex = producerId * eventsPerThread;
                var endIndex = startIndex + eventsPerThread;

                for (int i = startIndex; i < endIndex && i < totalEvents; i++)
                {
                    _store.TryAppend(_events[i]);
                    
                    if (_cts.Token.IsCancellationRequested)
                        break;
                        
                    // Add small delay to spread events over time
                    if (i % 100 == 0)
                    {
                        Thread.Sleep(1);
                    }
                }
            });
        }

        // Wait for all producers to complete
        Task.WaitAll(producerTasks);
        
        // Let queries run a bit more
        Thread.Sleep(100);
        
        // Stop query task
        _cts.Cancel();
        
        try
        {
            totalQueries = queryTask.Result;
        }
        catch (OperationCanceledException)
        {
            totalQueries = 0;
        }
        
        return totalQueries;
    }

    [Benchmark]
    public long SimpleProducersWithQueries()    {
        var eventsPerThread = EventsPerProducer;
        var totalEvents = eventsPerThread * ProducerCount;
        var windowTimeSpan = TimeSpan.FromMilliseconds(WindowSizeMs);
        long totalQueries = 0;

        // Fill store with events first
        for (int p = 0; p < ProducerCount; p++)
        {
            var startIndex = p * eventsPerThread;
            var endIndex = startIndex + eventsPerThread;

            for (int i = startIndex; i < endIndex && i < totalEvents; i++)
            {
                _store.TryAppend(_events[i]);
            }
        }

        // Now perform queries
        var now = DateTime.UtcNow;
        var windowStart = now - windowTimeSpan;

        // Perform aggregations without filter
        _ = _store.CountEvents(windowStart, now);
        _ = _store.Sum(e => e.Value, windowStart, now);
        _ = _store.Average(e => e.Value, windowStart, now);
        _ = _store.Min(e => e.Value, windowStart, now);
        _ = _store.Max(e => e.Value, windowStart, now);
        totalQueries = 5;

        return totalQueries;
    }

    private IEventStoreCompetitor CreateCompetitor() => Competitor switch
    {
        "EventStore" => new EventStoreCompetitor(Capacity),
        "ChannelBounded" => new ChannelBoundedCompetitor(Capacity),
        "ConcurrentQueue" => new ConcurrentQueueCompetitor(Capacity),
#if DISRUPTOR
        "Disruptor" => new DisruptorCompetitor(Capacity),
#endif
        _ => throw new ArgumentException($"Unknown competitor: {Competitor}")
    };
}
