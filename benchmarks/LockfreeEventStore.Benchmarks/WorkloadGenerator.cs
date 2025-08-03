namespace LockfreeEventStore.Benchmarks;

/// <summary>
/// Workload generator with Zipf distribution and filters.
/// </summary>
public static class WorkloadGenerator
{
    public static BenchmarkEvent[] GenerateEvents(int count, int keyCount, double skew = 0.0)
    {
        var random = new Random(42); // Fixed seed for reproducibility
        var zipf = new ZipfGenerator(keyCount, skew, random);
        var events = new BenchmarkEvent[count];
        
        var baseTime = DateTime.UtcNow;
        
        for (int i = 0; i < count; i++)
        {
            var keyIndex = zipf.Next();
            var key = $"key-{keyIndex}";
            var value = random.NextDouble() * 1000.0;
            var timestamp = baseTime.AddMilliseconds(i * 10); // Events spaced 10ms apart
            
            events[i] = new BenchmarkEvent(key, value, timestamp.Ticks);
        }
        
        return events;
    }
    
    public static Func<BenchmarkEvent, bool> CreateSelectivityFilter(double selectivity)
    {
        if (selectivity >= 1.0) 
            return _ => true; // No filtering
            
        var random = new Random(123); // Fixed seed
        return evt => random.NextDouble() < selectivity;
    }
}

/// <summary>
/// Zipf distribution generator for hot/cold key patterns.
/// </summary>
public class ZipfGenerator
{
    private readonly int _keyCount;
    private readonly double _skew;
    private readonly Random _random;
    private readonly double[] _probabilities;
    
    public ZipfGenerator(int keyCount, double skew, Random? random = null)
    {
        _keyCount = keyCount;
        _skew = skew;
        _random = random ?? new Random();
        _probabilities = CalculateProbabilities();
    }
    
    private double[] CalculateProbabilities()
    {
        var probs = new double[_keyCount];
        double sum = 0.0;
        
        for (int i = 0; i < _keyCount; i++)
        {
            probs[i] = 1.0 / Math.Pow(i + 1, _skew);
            sum += probs[i];
        }
        
        // Normalize to cumulative probabilities
        for (int i = 0; i < _keyCount; i++)
        {
            probs[i] /= sum;
            if (i > 0)
                probs[i] += probs[i - 1];
        }
        
        return probs;
    }
    
    public int Next()
    {
        if (_skew == 0.0)
        {
            // Uniform distribution
            return _random.Next(_keyCount);
        }
        
        var rand = _random.NextDouble();
        for (int i = 0; i < _keyCount; i++)
        {
            if (rand <= _probabilities[i])
                return i;
        }
        
        return _keyCount - 1;
    }
}
