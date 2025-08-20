using System;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class TimestampSelectorsTests
{
    [Fact]
    public void EventTimestampSelector_Works_For_GetTimestamp_And_Ticks()
    {
        var e = new Event(new KeyId(1), 2.5, 123456789);
        var sel = new EventTimestampSelector();
        Assert.Equal(new DateTime(123456789), sel.GetTimestamp(e));
        Assert.Equal(123456789, sel.GetTimestampTicks(e));
    }

    private sealed class SimpleEvt
    {
        public DateTime Ts { get; init; }
        public SimpleEvt(DateTime ts) { Ts = ts; }
    }

    private sealed class SimpleSelector : IEventTimestampSelector<SimpleEvt>
    {
        public DateTime GetTimestamp(SimpleEvt e) => e.Ts;
        // Intentionally do not override GetTimestampTicks to exercise default
    }

    [Fact]
    public void Interface_Default_GetTimestampTicks_Is_Used()
    {
        // Prefer deterministic timestamp over UtcNow to avoid rare flakiness
        var ts = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var evt = new SimpleEvt(ts);
        var sel = new SimpleSelector();
        Assert.Equal(ts, sel.GetTimestamp(evt));
        Assert.Equal(ts.Ticks, ((IEventTimestampSelector<SimpleEvt>)sel).GetTimestampTicks(evt));
    }

    [Fact]
    public void MetricTimestampSelector_Works()
    {
        var ts = new DateTime(987654321);
        var m = new MetricEvent("cpu", 1.23, ts);
        var sel = new MetricTimestampSelector();
        Assert.Equal(ts, sel.GetTimestamp(m));
        Assert.Equal(ts.Ticks, sel.GetTimestampTicks(m));
    }
}
