using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;

// Client sample for the lockfree-eventstore Docker server (MetricsDashboard)
// It will:
// 1. Post a batch of MetricEvent entries concurrently to /metrics
// 2. Query aggregated sum (/metrics/sum)
// 3. Query top labels (/metrics/top)
// 4. Query window aggregation (/metrics/window)
// 5. Display raw JSON for advanced endpoints

public record MetricEvent(string Label, double Value, DateTime Timestamp);

public class Program
{
    private static readonly string[] Labels = ["cpu", "mem", "disk", "latency"];
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    public static async Task Main(string[] args)
    {
        var baseUrl = Environment.GetEnvironmentVariable("EVENTSTORE_URL") ?? "http://localhost:7070";
        using var http = new HttpClient { BaseAddress = new Uri(baseUrl) };

        Console.WriteLine($"== LockFree.EventStore Metrics Client ==\nTarget: {baseUrl}\n");

        int total = 500; // number of events
        int parallelism = Environment.ProcessorCount;
        var rnd = new Random();

        Console.WriteLine($"Posting {total} metric events (parallelism={parallelism})...");
        var sw = Stopwatch.StartNew();

        await Parallel.ForEachAsync(Enumerable.Range(1, total), new ParallelOptions { MaxDegreeOfParallelism = parallelism }, async (i, ct) =>
        {
            var label = Labels[i % Labels.Length];
            var value = label switch
            {
                "cpu" => rnd.NextDouble() * 100,          // percent
                "mem" => rnd.NextDouble() * 32768,        // MB
                "disk" => rnd.NextDouble() * 500,         // MB/s
                _ => rnd.NextDouble() * 1000               // ms latency
            };
            var evt = new MetricEvent(label, value, DateTime.UtcNow);
            var resp = await http.PostAsJsonAsync("/metrics", evt, ct);
            if (!resp.IsSuccessStatusCode)
            {
                Console.WriteLine($"POST /metrics failed: {(int)resp.StatusCode} {resp.ReasonPhrase}");
                ct.ThrowIfCancellationRequested();
            }
        });

        sw.Stop();
        Console.WriteLine($"Posted {total} events in {sw.ElapsedMilliseconds} ms\n");

        // Query sum for a specific label
        string sumLabel = "cpu";
        Console.WriteLine($"Requesting sum for label '{sumLabel}' (last N/A interval) ...");
        var sumResponse = await http.GetAsync($"/metrics/sum?label={sumLabel}");
        if (sumResponse.IsSuccessStatusCode)
        {
            var sum = await sumResponse.Content.ReadFromJsonAsync<double>();
            Console.WriteLine($"Sum[{sumLabel}] = {sum:F2}\n");
        }
        else
        {
            Console.WriteLine($"/metrics/sum failed: {(int)sumResponse.StatusCode} {sumResponse.ReasonPhrase}\n");
        }

        // Query top labels
        Console.WriteLine("Requesting top 3 labels by value ...");
        var topResp = await http.GetAsync("/metrics/top?k=3");
        if (topResp.IsSuccessStatusCode)
        {
            var topJson = await topResp.Content.ReadAsStringAsync();
            Console.WriteLine("Top 3 raw JSON:\n" + topJson + "\n");
        }
        else
        {
            Console.WriteLine($"/metrics/top failed: {(int)topResp.StatusCode} {topResp.ReasonPhrase}\n");
        }

        // Window aggregation
        Console.WriteLine("Requesting window aggregation for 'cpu' ...");
        var winResp = await http.GetAsync("/metrics/window?label=cpu");
        if (winResp.IsSuccessStatusCode)
        {
            var winJson = await winResp.Content.ReadAsStringAsync();
            Console.WriteLine("Window aggregation:\n" + winJson + "\n");
        }
        else
        {
            Console.WriteLine($"/metrics/window failed: {(int)winResp.StatusCode} {winResp.ReasonPhrase}\n");
        }

        // Snapshot views (zero-allocation insight) if available
        Console.WriteLine("Requesting snapshot views ...");
        var snapResp = await http.GetAsync("/metrics/snapshot-views");
        if (snapResp.IsSuccessStatusCode)
        {
            var snapJson = await snapResp.Content.ReadAsStringAsync();
            Console.WriteLine("Snapshot views:\n" + snapJson + "\n");
        }
        else
        {
            Console.WriteLine($"/metrics/snapshot-views failed: {(int)snapResp.StatusCode} {snapResp.ReasonPhrase}\n");
        }

        // Final cleanup (best-effort) – adjust endpoints to match server implementation
        Console.WriteLine("Cleaning up remote store (/admin endpoints)...");
        await TryCall(http, "/admin/purge?olderThanMinutes=999999");
        await TryCall(http, "/admin/reset");
        await TryCall(http, "/admin/clear");
        Console.WriteLine("Cleanup sequence completed.");

        Console.WriteLine("Done.");
    }

    private static async Task TryCall(HttpClient http, string path)
    {
        try
        {
            var resp = await http.PostAsync(path, null);
            Console.WriteLine($" -> {path}: {(int)resp.StatusCode}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($" -> {path}: failed ({ex.Message})");
        }
    }
}
