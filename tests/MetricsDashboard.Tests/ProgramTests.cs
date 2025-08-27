using System;
using System.Collections.Generic;
using System.Net.Http.Json;
using System.Threading.Tasks;
using LockFree.EventStore;
using MetricsDashboard;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit;

namespace MetricsDashboard.Tests;

public class ProgramTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly WebApplicationFactory<Program> _factory;

    public ProgramTests(WebApplicationFactory<Program> factory)
    {
        _factory = factory.WithWebHostBuilder(_ => { });
    }

    [Fact]
    public async Task Health_ReturnsOk()
    {
        var client = _factory.CreateClient();
        var resp = await client.GetAsync("/health");
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadAsStringAsync();
        Assert.Equal("\"OK\"", body);
    }

    [Fact]
    public async Task Metrics_Post_And_Sum_Filtered_By_Label_And_Window()
    {
        var client = _factory.CreateClient();

        // post some metrics
        var now = DateTime.UtcNow;
        await client.PostAsJsonAsync("/metrics", new MetricEvent("cpu", 10, now.AddMinutes(-5)));
        await client.PostAsJsonAsync("/metrics", new MetricEvent("cpu", 20, now.AddMinutes(-3)));
        await client.PostAsJsonAsync("/metrics", new MetricEvent("cpu", 30, now.AddMinutes(-1)));
        await client.PostAsJsonAsync("/metrics", new MetricEvent("mem", 99, now.AddMinutes(-1)));

        // sum with date range and label
        var from = now.AddMinutes(-6).ToString("o");
        var to = now.AddMinutes(-2).ToString("o");
        var sumResp = await client.GetAsync($"/metrics/sum?from={Uri.EscapeDataString(from)}&to={Uri.EscapeDataString(to)}&label=cpu");
        sumResp.EnsureSuccessStatusCode();
        var sumText = await sumResp.Content.ReadAsStringAsync();
        // 10 + 20 = 30
        Assert.Equal("30", sumText);

        // sum-window last 2 minutes -> only 30 for label cpu
        var windowResp = await client.GetAsync("/metrics/sum-window?label=cpu&minutes=2");
        windowResp.EnsureSuccessStatusCode();
        var windowText = await windowResp.Content.ReadAsStringAsync();
        Assert.Equal("30", windowText);
    }

    [Fact]
    public async Task Metrics_Window_With_And_Without_Label()
    {
        var client = _factory.CreateClient();

        var now = DateTime.UtcNow;
        await client.PostAsJsonAsync("/metrics", new MetricEvent("cpu", 1, now.AddSeconds(-30)));
        await client.PostAsJsonAsync("/metrics", new MetricEvent("cpu", 2, now.AddSeconds(-20)));
        await client.PostAsJsonAsync("/metrics", new MetricEvent("mem", 4, now.AddSeconds(-10)));

        var from = now.AddMinutes(-1).ToString("o");
        var to = now.ToString("o");

        // no label
        var resp = await client.GetAsync($"/metrics/window?from={Uri.EscapeDataString(from)}&to={Uri.EscapeDataString(to)}");
        resp.EnsureSuccessStatusCode();
        var payload = await resp.Content.ReadFromJsonAsync<MetricsWindowResponse>();
        Assert.NotNull(payload);
        int count = payload!.Count;
        Assert.True(count >= 3);

        // label=cpu
        var resp2 = await client.GetAsync($"/metrics/window?from={Uri.EscapeDataString(from)}&to={Uri.EscapeDataString(to)}&label=cpu");
        resp2.EnsureSuccessStatusCode();
        var payload2 = await resp2.Content.ReadFromJsonAsync<MetricsWindowResponse>();
        Assert.NotNull(payload2);
        int count2 = payload2!.Count;
        Assert.Equal(2, count2);
    }

    [Fact]
    public async Task Streams_Post_And_Read_And_Aggregate()
    {
        var client = _factory.CreateClient();

        // send events to different gateways within a stream
        await client.PostAsJsonAsync("/streams/orders/eu", new GatewayOrderCreated { Id = "1", GatewayId = "g1", Valor = 10, Timestamp = DateTime.UtcNow.AddSeconds(-5) });
        await client.PostAsJsonAsync("/streams/orders/eu", new GatewayOrderCreated { Id = "2", GatewayId = "g2", Valor = 20, Timestamp = DateTime.UtcNow.AddSeconds(-4) });
        await client.PostAsJsonAsync("/streams/orders/eu", new GatewayOrderCreated { Id = "3", GatewayId = "g1", Valor = 30, Timestamp = DateTime.UtcNow.AddSeconds(-3) });

        // raw list
        var list = await client.GetFromJsonAsync<List<GatewayOrderCreated>>("/streams/orders/eu");
        Assert.NotNull(list);
        Assert.Equal(3, list!.Count);

        // aggregate
        var aggResp = await client.GetFromJsonAsync<StreamAggregate>("/streams/orders/eu?aggregate=true");
        Assert.NotNull(aggResp);
        int totalEvents = aggResp!.TotalEvents;
        long totalValor = aggResp.TotalValor;
        Assert.Equal(3, totalEvents);
        Assert.Equal(60, totalValor);
    }

    [Fact]
    public async Task Metrics_Aggregate_Zero_Alloc_And_Process_Events()
    {
        var client = _factory.CreateClient();

        var now = DateTime.UtcNow;
        for (var i = 0; i < 10; i++)
        {
            await client.PostAsJsonAsync("/metrics", new MetricEvent("cpu", i, now.AddSeconds(-i)));
        }

        var agg = await client.GetFromJsonAsync<AggregateResult>($"/metrics/aggregate-zero-alloc?from={Uri.EscapeDataString(now.AddSeconds(-9).ToString("o"))}&to={Uri.EscapeDataString(now.ToString("o"))}");
        Assert.NotNull(agg);
        int c = agg!.Count;
        Assert.Equal(10, c);

        var high = await client.GetFromJsonAsync<ProcessEventsResult>("/metrics/process-events?label=cpu");
        Assert.NotNull(high);
        int processed = high!.Processed;
        int highCpuCount = high.HighCpuCount;
        Assert.True(processed >= 10);
        Assert.True(highCpuCount >= 0);
    }

    [Fact]
    public async Task Admin_Endpoints_Work()
    {
        var client = _factory.CreateClient();

        // add something
        await client.PostAsJsonAsync("/metrics", new MetricEvent("x", 1, DateTime.UtcNow));

        var clear = await client.PostAsync("/admin/clear", null);
        clear.EnsureSuccessStatusCode();
        var clearPayload = await clear.Content.ReadFromJsonAsync<AdminClearResponse>();
        Assert.NotNull(clearPayload);
        Assert.True(clearPayload!.Cleared);

        var reset = await client.PostAsync("/admin/reset", null);
        reset.EnsureSuccessStatusCode();
        var resetPayload = await reset.Content.ReadFromJsonAsync<AdminResetResponse>();
        Assert.NotNull(resetPayload);
        Assert.True(resetPayload!.Reset);

        var purge = await client.PostAsync($"/admin/purge?olderThanMinutes=1", null);
        purge.EnsureSuccessStatusCode();
        var purgePayload = await purge.Content.ReadFromJsonAsync<AdminPurgeResponse>();
        Assert.NotNull(purgePayload);
        Assert.True(purgePayload!.PurgedBefore <= DateTime.UtcNow);
    }
}

internal sealed class MetricsWindowResponse
{
    public int Count { get; set; }
    public double Sum { get; set; }
    public double Min { get; set; }
    public double Max { get; set; }
    public double Avg { get; set; }
}

internal sealed class AggregateResult
{
    public int Count { get; set; }
    public double Sum { get; set; }
    public double Min { get; set; }
    public double Max { get; set; }
    public double Avg { get; set; }
}

internal sealed class ProcessEventsResult
{
    public int Processed { get; set; }
    public int HighCpuCount { get; set; }
}

internal sealed class StreamAggregate
{
    public string Stream { get; set; } = string.Empty;
    public int TotalEvents { get; set; }
    public long TotalValor { get; set; }
    public Dictionary<string, GatewayAgg> Gateways { get; set; } = new();
    public double DurationMs { get; set; }
}

internal sealed class GatewayAgg
{
    public int Count { get; set; }
    public long Sum { get; set; }
}

internal sealed class AdminClearResponse
{
    public bool Cleared { get; set; }
}

internal sealed class AdminResetResponse
{
    public bool Reset { get; set; }
}

internal sealed class AdminPurgeResponse
{
    public DateTime PurgedBefore { get; set; }
}
