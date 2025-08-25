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
    /// <summary>
    /// Returns the <see cref="MetricEvent.Timestamp"/> of the provided metric event.
    /// </summary>
    /// <returns>The event's timestamp.</returns>
    public DateTime GetTimestamp(MetricEvent e)
    {
        return e.Timestamp;
    }

    /// <summary>
    /// Gets the timestamp ticks from a MetricEvent.
    /// </summary>
    /// <param name="e">The metric event.</param>
    /// <summary>
    /// Returns the number of ticks from the given metric event's Timestamp.
    /// </summary>
    /// <param name="e">The metric event whose <see cref="MetricEvent.Timestamp"/> ticks to return.</param>
    /// <returns>The value of <see cref="DateTime.Ticks"/> for the event's timestamp.</returns>
    public long GetTimestampTicks(MetricEvent e)
    {
        return e.Timestamp.Ticks;
    }
}
