using BenchmarkDotNet.Attributes;
using LockfreeEventStore.Benchmarks.Competitors;
using LockfreeEventStore.Benchmarks.Utils;

namespace LockfreeEventStore.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for append-only scenarios without consumers.
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class AppendOnlyBenchmarks
{    [Params(1, 2, 4)]
    public int ProducerCount { get; set; }

    [Params(1_000, 10_000)]
    public int Capacity { get; set; }

    [Params(0.0)]
    public double Skew { get; set; }

    [Params("EventStore", "ChannelBounded", "ConcurrentQueue"
#if DISRUPTOR
        , "Disruptor"
#endif
    )]
    public string Competitor { get; set; } = "EventStore";    private IEventStoreCompetitor _store = null!;
    private BenchmarkEvent[] _events = null!;
    private const int EventsPerProducer = 1_000;
    private const int KeyCount = 100;

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
    }    [Benchmark]
    public long AppendConcurrent()
    {
        var eventsPerThread = EventsPerProducer;
        var totalEvents = eventsPerThread * ProducerCount;
        long appendedCount = 0;

        using var barrier = new Barrier(ProducerCount + 1);
        var tasks = new Task<int>[ProducerCount];
        
        for (int p = 0; p < ProducerCount; p++)
        {
            int producerId = p;
            tasks[p] = Task.Run(() =>
            {
                barrier.SignalAndWait(); // Wait for all threads to be ready
                
                var startIndex = producerId * eventsPerThread;
                var endIndex = startIndex + eventsPerThread;
                int localCount = 0;

                for (int i = startIndex; i < endIndex && i < totalEvents; i++)
                {
                    if (_store.TryAppend(_events[i]))
                        localCount++;
                }
                
                return localCount;
            });
        }
        
        barrier.SignalAndWait(); // Release all threads
        Task.WaitAll(tasks);
        
        // Sum up all appended events
        foreach (var task in tasks)
        {
            appendedCount += task.Result;
        }
        
        return appendedCount;
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
