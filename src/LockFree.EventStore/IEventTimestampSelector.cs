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
}
