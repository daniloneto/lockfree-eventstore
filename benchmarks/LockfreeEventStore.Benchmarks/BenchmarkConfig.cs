using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Exporters.Json;

namespace LockfreeEventStore.Benchmarks;

public static class BenchmarkConfig
{
    public static IConfig CreateConfig(bool inProcess = false, bool enableHardwareCounters = true)
    {
        var job = Job.Default;
        if (inProcess)
        {
            job = job.WithToolchain(InProcessEmitToolchain.Instance);
        }
          var config = ManualConfig.Create(DefaultConfig.Instance)
            .AddJob(job)
            .AddDiagnoser(MemoryDiagnoser.Default)
            .AddDiagnoser(ThreadingDiagnoser.Default)
            .AddExporter(MarkdownExporter.GitHub)
            .AddExporter(CsvExporter.Default)
            .AddExporter(JsonExporter.Brief)
            .AddExporter(JsonExporter.Full);

        // Hardware counters can be added here if needed in the future
        // Currently simplified for compatibility

        return config;
    }

    public static IConfig Default => CreateConfig();
    public static IConfig InProcess => CreateConfig(inProcess: true);
    public static IConfig WithoutHardwareCounters => CreateConfig(enableHardwareCounters: false);
}
