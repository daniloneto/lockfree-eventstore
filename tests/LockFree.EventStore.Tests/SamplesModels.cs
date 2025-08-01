namespace LockFree.EventStore.Tests;

public sealed record Order(int Id, decimal Amount, DateTime Timestamp);

public sealed class OrderTimestampSelector : IEventTimestampSelector<Order>
{
    public DateTime GetTimestamp(Order e) => e.Timestamp;
}

public sealed record MetricEvent(string Label, double Value, DateTime Timestamp);

public sealed class MetricTimestampSelector : IEventTimestampSelector<MetricEvent>
{
    public DateTime GetTimestamp(MetricEvent e) => e.Timestamp;
}
