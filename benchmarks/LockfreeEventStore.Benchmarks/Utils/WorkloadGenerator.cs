namespace LockfreeEventStore.Benchmarks.Utils;

/// <summary>
/// Zipf distribution generator for simulating hot keys.
/// </summary>
public sealed class ZipfGenerator
{
    private readonly Random _random;
    private readonly double[] _probabilities;
    private readonly int _size;

    public ZipfGenerator(int size, double skew, int seed = 42)
    {
        _size = size;
        _random = new Random(seed);
        _probabilities = new double[size];

        // Calculate Zipf probabilities
        double sum = 0;
        for (int i = 1; i <= size; i++)
        {
            sum += 1.0 / Math.Pow(i, skew);
        }

        double cumulativeProb = 0;
        for (int i = 0; i < size; i++)
        {
            var prob = (1.0 / Math.Pow(i + 1, skew)) / sum;
            cumulativeProb += prob;
            _probabilities[i] = cumulativeProb;
        }
    }

    public int Next()
    {
        var rand = _random.NextDouble();
        for (int i = 0; i < _size; i++)
        {
            if (rand <= _probabilities[i])
                return i;
        }
        return _size - 1;
    }

    public string NextKey() => $"key-{Next()}";
}

/// <summary>
/// Utility for generating benchmark workload.
/// </summary>
public static class WorkloadGenerator
{
    public static BenchmarkEvent[] GenerateEvents(int count, int keyCount, double skew, int seed = 42)
    {
        var generator = new ZipfGenerator(keyCount, skew, seed);
        var random = new Random(seed);
        var events = new BenchmarkEvent[count];
        var baseTime = DateTime.UtcNow.Ticks;

        for (int i = 0; i < count; i++)
        {
            events[i] = new BenchmarkEvent(
                Key: generator.NextKey(),
                Value: random.NextDouble() * 1000,
                TimestampTicks: baseTime + i * TimeSpan.TicksPerMillisecond
            );
        }

        return events;
    }

    public static Func<BenchmarkEvent, bool> CreateSelectivityFilter(double selectivity, int seed = 42)
    {
        if (selectivity >= 1.0) return _ => true;
        
        var random = new Random(seed);
        var threshold = selectivity;
        
        return evt => 
        {
            // Use hash of key for deterministic filtering
            var hash = evt.Key.GetHashCode();
            var normalized = ((double)(hash & 0x7FFFFFFF)) / int.MaxValue;
            return normalized < threshold;
        };
    }
}
