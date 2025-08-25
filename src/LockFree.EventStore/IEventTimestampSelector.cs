namespace LockFree.EventStore;

/// <summary>
/// Selects a timestamp from an event instance.
/// </summary>
/// <typeparam name="TEvent">Event type.</typeparam>
public interface IEventTimestampSelector<in TEvent>
{
    /// <summary>
    /// Returns the timestamp for <paramref name="e"/>.
    /// </summary>
    DateTime GetTimestamp(TEvent e);

    /// <summary>
    /// Returns the timestamp ticks for <paramref name="e"/>.
    /// Default implementation calls GetTimestamp and gets Ticks.
    /// </summary>
    long GetTimestampTicks(TEvent e)
    {
        return GetTimestamp(e).Ticks;
    }
}
