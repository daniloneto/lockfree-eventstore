using BenchmarkDotNet.Attributes;
using LockfreeEventStore.Benchmarks.Competitors;
using LockfreeEventStore.Benchmarks.Utils;

namespace LockfreeEventStore.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for append scenarios with periodic snapshot calls.
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class AppendWithSnapshotBenchmarks
{
    [Params(2, 4, 8)]
    public int ProducerCount { get; set; }

    [Params(10_000, 100_000)]
    public int Capacity { get; set; }

    [Params(0.0, 0.8)]
    public double Skew { get; set; }

    [Params("EventStore", "ChannelBounded", "ConcurrentQueue"
#if DISRUPTOR
        , "Disruptor"
#endif
    )]
    public string Competitor { get; set; } = "EventStore";

    private IEventStoreCompetitor _store = null!;
    private BenchmarkEvent[] _events = null!;
    private CancellationTokenSource _cts = null!;
    
    private const int EventsPerProducer = 5_000;
    private const int KeyCount = 1000;
    private const int SnapshotIntervalMs = 100;

    [GlobalSetup]
    public void Setup()
    {
        _store = CreateCompetitor();
        _events = WorkloadGenerator.GenerateEvents(
            EventsPerProducer * ProducerCount, 
            KeyCount, 
            Skew);
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
    public async Task AppendWithPeriodicSnapshots()
    {
        var eventsPerThread = EventsPerProducer;
        var totalEvents = eventsPerThread * ProducerCount;

        // Start snapshot task
        var snapshotTask = Task.Run(async () =>
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    _store.Snapshot();
                    await Task.Delay(SnapshotIntervalMs, _cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
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
                }
            });
        }

        // Wait for all producers to complete
        await Task.WhenAll(producerTasks);
        
        // Stop snapshot task
        _cts.Cancel();
        
        try
        {
            await snapshotTask;
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
