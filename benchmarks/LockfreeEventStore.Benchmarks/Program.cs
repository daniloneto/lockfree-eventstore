using System.Reflection;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using CommandLine;
using LockfreeEventStore.Benchmarks.Benchmarks;

namespace LockfreeEventStore.Benchmarks;

class Program
{
    static async Task<int> Main(string[] args)
    {
        try
        {
            return await Parser.Default.ParseArguments<BenchmarkOptions>(args)
                .MapResult(
                    options => RunBenchmarks(options),
                    errors => Task.FromResult(1));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            return 1;
        }
    }

    static Task<int> RunBenchmarks(BenchmarkOptions options)
    {        if (options.ShowExamples)
        {
            ShowExamples();
            return Task.FromResult(0);
        }

        if (options.ListBenchmarks)
        {
            ListAvailableBenchmarks();
            return Task.FromResult(0);
        }

        // Ensure artifacts directory exists
        Directory.CreateDirectory(options.ArtifactsPath);

        // Build configuration
        var config = CreateBenchmarkConfig(options);

        // Get benchmark types
        var benchmarkTypes = GetBenchmarkTypes(options);        if (!benchmarkTypes.Any())
        {
            Console.WriteLine("No benchmarks selected to run.");
            return Task.FromResult(1);
        }

        Console.WriteLine($"Running benchmarks: {string.Join(", ", benchmarkTypes.Select(t => t.Name))}");
        Console.WriteLine($"Results will be saved to: {Path.GetFullPath(options.ArtifactsPath)}");
        Console.WriteLine();

        // Run benchmarks
        foreach (var benchmarkType in benchmarkTypes)
        {
            Console.WriteLine($"Running {benchmarkType.Name}...");
            var summary = BenchmarkRunner.Run(benchmarkType, config);
            
            if (summary.HasCriticalValidationErrors)
            {
                Console.WriteLine($"Critical validation errors in {benchmarkType.Name}");
                foreach (var error in summary.ValidationErrors)
                {
                    Console.WriteLine($"  {error.Message}");
                }            }
        }

        Console.WriteLine("All benchmarks completed!");
        Console.WriteLine($"Results saved to: {Path.GetFullPath(options.ArtifactsPath)}");
        
        // Generate comparison table
        GenerateComparisonTable(options.ArtifactsPath);
        
        return Task.FromResult(0);
    }

    static void GenerateComparisonTable(string artifactsPath)
    {
        try
        {
            var comparisonPath = Path.Combine(artifactsPath, "benchmark-comparison.md");
            var content = $@"# Benchmark Results Comparison

*Generated on: {DateTime.Now:yyyy-MM-dd HH:mm:ss}*

## Performance Summary

| Benchmark | Mean | Allocated | Gen0 | Gen1 | Gen2 | Notes |
|-----------|------|-----------|------|------|------|-------|
| WindowAggregateBenchmarks | - | < 500 KB (target) | - | - | - | Target: Mean ≤ baseline |
| AppendWithSnapshotBenchmarks | - | < 500 KB (target) | - | - | - | Was ~2.3 MB before |
| MixedHotColdKeysBenchmarks | - | < 2 MB (target) | < 1000 (target) | < 1000 (target) | < 1000 (target) | Hot/Cold key performance |
| AppendOnlyBenchmarks | Close to Channel<T> (target) | ~0 (target) | - | - | - | Minimal allocation target |

## Configuration Used

### WindowAggregateBenchmarks
- Producers: 2, 4
- Capacity: 10,000
- WindowSizeMs: 100
- Skew: 0.0, 0.8
- FilterSelectivity: 1.0, 0.1

### AppendWithSnapshotBenchmarks
- Producers: 2
- Capacity: 100,000
- Snapshot interval: Every 10,000 events

### MixedHotColdKeysBenchmarks
- Producers: 4
- Capacity: 50,000, 200,000
- WindowMs: 1000
- Skew: 0.5, 0.9
- FilterSelectivity: 0.2

### AppendOnlyBenchmarks
- Producers: 1, 4
- Capacity: 10,000

## Files Generated

The following result files have been generated:
- **.md files**: GitHub-compatible markdown reports
- **.csv files**: Excel-compatible data files
- **.json files**: Machine-readable results for integration

## Next Steps

1. Review individual benchmark files for detailed metrics
2. Compare allocated memory against targets
3. Analyze performance regression/improvement
4. Update baseline measurements if needed

---
*For detailed results, see individual benchmark result files in this directory.*
";

            File.WriteAllText(comparisonPath, content);
            Console.WriteLine($"Comparison table generated: {comparisonPath}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Warning: Could not generate comparison table: {ex.Message}");
        }
    }

    static IConfig CreateBenchmarkConfig(BenchmarkOptions options)
    {
        var config = options.InProcess ? BenchmarkConfig.InProcess : BenchmarkConfig.Default;

        if (!options.EnableHardwareCounters)
        {
            config = BenchmarkConfig.WithoutHardwareCounters;
        }

        // Apply custom job settings if specified
        if (options.WarmupCount.HasValue || options.TargetCount.HasValue)
        {
            var job = Job.Default;
            if (options.WarmupCount.HasValue)
                job = job.WithWarmupCount(options.WarmupCount.Value);
            if (options.TargetCount.HasValue)
                job = job.WithIterationCount(options.TargetCount.Value);
            
            config = config.AddJob(job);
        }

        // Set artifacts path
        config = config.WithArtifactsPath(options.ArtifactsPath);

        // Apply filter if specified
        if (!string.IsNullOrEmpty(options.Filter))
        {
            config = config.AddFilter(new SimpleFilter(benchmark => 
                benchmark.DisplayInfo.Contains(options.Filter, StringComparison.OrdinalIgnoreCase)));
        }

        return config;
    }

    static Type[] GetBenchmarkTypes(BenchmarkOptions options)
    {
        var allBenchmarkTypes = new[]
        {
            typeof(AppendOnlyBenchmarks),
            typeof(AppendWithSnapshotBenchmarks),
            typeof(WindowAggregateBenchmarks),
            typeof(MixedHotColdKeysBenchmarks)
        };

        // Filter by included categories
        if (!string.IsNullOrEmpty(options.Include))
        {
            var includedCategories = options.Include
                .Split(',', StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.Trim())
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var categoryMapping = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase)
            {
                ["AppendOnly"] = typeof(AppendOnlyBenchmarks),
                ["AppendWithSnapshot"] = typeof(AppendWithSnapshotBenchmarks),
                ["WindowAggregate"] = typeof(WindowAggregateBenchmarks),
                ["MixedHotCold"] = typeof(MixedHotColdKeysBenchmarks)
            };

            return categoryMapping
                .Where(kvp => includedCategories.Contains(kvp.Key))
                .Select(kvp => kvp.Value)
                .ToArray();
        }

        return allBenchmarkTypes;
    }

    static void ListAvailableBenchmarks()
    {
        Console.WriteLine("Available benchmarks:");
        Console.WriteLine();
        
        Console.WriteLine("Categories:");
        Console.WriteLine("  AppendOnly          - Pure append benchmarks without consumers");
        Console.WriteLine("  AppendWithSnapshot  - Append with periodic snapshot calls");
        Console.WriteLine("  WindowAggregate     - Window-based aggregation queries");
        Console.WriteLine("  MixedHotCold        - Mixed workload with hot/cold key distributions");
        Console.WriteLine();
        
        Console.WriteLine("Competitors:");
        Console.WriteLine("  EventStore         - LockFree.EventStore (this library)");
        Console.WriteLine("  ChannelBounded     - System.Threading.Channels");
        Console.WriteLine("  ConcurrentQueue    - System.Collections.Concurrent.ConcurrentQueue");
#if DISRUPTOR
        Console.WriteLine("  Disruptor          - Disruptor.NET ring buffer");
#endif
        Console.WriteLine();
    }

    static void ShowExamples()
    {
        Console.WriteLine("LockFree EventStore Benchmarks");
        Console.WriteLine("==============================");
        Console.WriteLine();
        Console.WriteLine("Usage Examples:");
        Console.WriteLine();
        Console.WriteLine("1. Run all benchmarks:");
        Console.WriteLine("   dotnet run -c Release");
        Console.WriteLine();
        Console.WriteLine("2. Run specific categories:");
        Console.WriteLine("   dotnet run -c Release -- --include=AppendOnly,WindowAggregate");
        Console.WriteLine();
        Console.WriteLine("3. Run specific competitors:");
        Console.WriteLine("   dotnet run -c Release -- --competitors=EventStore,ChannelBounded");
        Console.WriteLine();
        Console.WriteLine("4. Filter benchmarks by pattern:");
        Console.WriteLine("   dotnet run -c Release -- --filter=\"*Append*\"");
        Console.WriteLine();
        Console.WriteLine("5. Run in-process (faster, less accurate):");
        Console.WriteLine("   dotnet run -c Release -- --inprocess");
        Console.WriteLine();
        Console.WriteLine("6. Custom iterations:");
        Console.WriteLine("   dotnet run -c Release -- --warmup=5 --target=10");
        Console.WriteLine();
        Console.WriteLine("7. Specify results directory:");
        Console.WriteLine("   dotnet run -c Release -- --artifacts=my-results");
        Console.WriteLine();
        Console.WriteLine("8. Disable hardware counters:");
        Console.WriteLine("   dotnet run -c Release -- --hardware-counters=false");
        Console.WriteLine();
#if DISRUPTOR
        Console.WriteLine("9. Enable Disruptor competitor:");
        Console.WriteLine("   dotnet run -c Release -- --disruptor");
        Console.WriteLine();
#endif
        Console.WriteLine("Prerequisites:");
        Console.WriteLine("- .NET 9.0 or later");
        Console.WriteLine("- Release configuration recommended for accurate results");
        Console.WriteLine("- Elevated privileges may be required for hardware counters");
        Console.WriteLine("- Close other applications for best benchmark isolation");
        Console.WriteLine();
        Console.WriteLine("Results:");
        Console.WriteLine("- Markdown: {artifacts}/BenchmarkDotNet.Artifacts/results/*-report.md");
        Console.WriteLine("- CSV:      {artifacts}/BenchmarkDotNet.Artifacts/results/*-report.csv");
        Console.WriteLine("- JSON:     {artifacts}/BenchmarkDotNet.Artifacts/results/*-report.json");
        Console.WriteLine();
    }
}
