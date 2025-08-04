using BenchmarkDotNet.Attributes;
using LockfreeEventStore.Benchmarks.Competitors;
using LockfreeEventStore.Benchmarks.Utils;

namespace LockfreeEventStore.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks comparing different key distributions (hot vs cold keys).
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class MixedHotColdKeysBenchmarks
{    [Params(4)]
    public int ProducerCount { get; set; }

    [Params(50_000, 200_000)]
    public int Capacity { get; set; }

    [Params(1000)]
    public int WindowMs { get; set; }

    [Params(0.5, 0.9)] // 0.5 = moderate skew, 0.9 = heavy skew
    public double Skew { get; set; }

    [Params(0.2)]
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
    
    private const int EventsPerProducer = 4_000;
    private const int KeyCount = 2000;
    private const int QueryIntervalMs = 75;

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
    }

    [Benchmark]
    public async Task MixedWorkload()
    {
        var eventsPerThread = EventsPerProducer;
        var totalEvents = eventsPerThread * ProducerCount;
        var windowTimeSpan = TimeSpan.FromMilliseconds(WindowMs);

        // Start mixed query task (aggregations + snapshots)
        var queryTask = Task.Run(async () =>
        {
            int queryCount = 0;
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var now = DateTime.UtcNow;
                    var windowStart = now - windowTimeSpan;

                    // Alternate between different types of queries
                    switch (queryCount % 6)
                    {
                        case 0:
                            _ = _store.CountEvents(_filter, windowStart, now);
                            break;
                        case 1:
                            _ = _store.Sum(e => e.Value, _filter, windowStart, now);
                            break;
                        case 2:
                            _ = _store.Average(e => e.Value, _filter, windowStart, now);
                            break;
                        case 3:
                            _ = _store.Min(e => e.Value, _filter, windowStart, now);
                            break;
                        case 4:
                            _ = _store.Max(e => e.Value, _filter, windowStart, now);
                            break;
                        case 5:
                            // Snapshot every 6th query
                            _ = _store.Snapshot();
                            break;
                    }

                    queryCount++;
                    await Task.Delay(QueryIntervalMs, _cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        });

        // Start producers with different patterns
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
                        
                    // Different producers with different append patterns
                    if (producerId % 2 == 0)
                    {
                        // Even producers: steady rate
                        if (i % 50 == 0)
                        {
                            Thread.Sleep(1);
                        }
                    }
                    else
                    {
                        // Odd producers: bursty
                        if (i % 200 == 0)
                        {
                            Thread.Sleep(5);
                        }
                    }
                }
            });
        }

        // Wait for all producers to complete
        await Task.WhenAll(producerTasks);
        
        // Let queries run a bit more to process final events
        await Task.Delay(200);
        
        // Stop query task
        _cts.Cancel();
        
        try
        {
            await queryTask;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
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
