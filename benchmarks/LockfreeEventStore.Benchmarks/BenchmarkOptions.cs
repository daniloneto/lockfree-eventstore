using CommandLine;

namespace LockfreeEventStore.Benchmarks;

/// <summary>
/// Command line options for the benchmark runner.
/// </summary>
public class BenchmarkOptions
{
    [Option("competitors", Required = false, HelpText = "Comma-separated list of competitors to run (EventStore,ChannelBounded,ConcurrentQueue,Disruptor)")]
    public string? Competitors { get; set; }

    [Option("include", Required = false, HelpText = "Comma-separated list of benchmark categories to include (AppendOnly,AppendWithSnapshot,WindowAggregate,MixedHotCold)")]
    public string? Include { get; set; }

    [Option("filter", Required = false, HelpText = "BenchmarkDotNet filter pattern")]
    public string? Filter { get; set; }

    [Option("inprocess", Required = false, Default = false, HelpText = "Run benchmarks in-process")]
    public bool InProcess { get; set; }

    [Option("disruptor", Required = false, Default = false, HelpText = "Enable Disruptor competitor (requires compilation with DISRUPTOR=true)")]
    public bool EnableDisruptor { get; set; }

    [Option("warmup", Required = false, HelpText = "Number of warmup iterations")]
    public int? WarmupCount { get; set; }

    [Option("target", Required = false, HelpText = "Number of target iterations")]
    public int? TargetCount { get; set; }

    [Option("artifacts", Required = false, Default = "artifacts/results", HelpText = "Directory to save benchmark results")]
    public string ArtifactsPath { get; set; } = "artifacts/results";

    [Option("hardware-counters", Required = false, Default = true, HelpText = "Enable hardware counters if supported")]
    public bool EnableHardwareCounters { get; set; } = true;

    [Option("list-benchmarks", Required = false, Default = false, HelpText = "List available benchmarks and exit")]
    public bool ListBenchmarks { get; set; }

    [Option("help-examples", Required = false, Default = false, HelpText = "Show usage examples and exit")]
    public bool ShowExamples { get; set; }
}

/// <summary>
/// Available benchmark categories.
/// </summary>
public enum BenchmarkCategory
{
    AppendOnly,
    AppendWithSnapshot,
    WindowAggregate,
    MixedHotCold
}

/// <summary>
/// Available competitors.
/// </summary>
public enum CompetitorType
{
    EventStore,
    ChannelBounded,
    ConcurrentQueue,
#if DISRUPTOR
    Disruptor
#endif
}
