namespace LockFree.EventStore;

/// <summary>
/// Represents a metric event with a label, value, and timestamp.
/// </summary>
public sealed record MetricEvent(string Label, double Value, DateTime Timestamp);

/// <summary>
/// Timestamp selector for MetricEvent.
/// </summary>
public sealed class MetricTimestampSelector : IEventTimestampSelector<MetricEvent>
{
    /// <summary>
    /// Gets the timestamp from a MetricEvent.
    /// </summary>
    /// <param name="e">The metric event.</param>
    /// <returns>The timestamp of the event.</returns>
    public DateTime GetTimestamp(MetricEvent e)
    {
        return e.Timestamp;
    }

    /// <summary>
    /// Gets the timestamp ticks from a MetricEvent.
    /// </summary>
    /// <param name="e">The metric event.</param>
    /// <returns>The timestamp ticks of the event.</returns>
    public long GetTimestampTicks(MetricEvent e)
    {
        return e.Timestamp.Ticks;
    }
}
