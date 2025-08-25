namespace LockFree.EventStore;

/// <summary>
/// Selects a timestamp from an event instance.
/// </summary>
/// <typeparam name="TEvent">Event type.</typeparam>
public interface IEventTimestampSelector<in TEvent>
{
    /// <summary>
    /// Returns the timestamp for <paramref name="e"/>.
    /// <summary>
/// Selects and returns the timestamp for the provided event instance.
/// </summary>
/// <param name="e">The event from which to obtain the timestamp.</param>
/// <returns>The DateTime that represents the event's timestamp.</returns>
    DateTime GetTimestamp(TEvent e);

    /// <summary>
    /// Returns the timestamp ticks for <paramref name="e"/>.
    /// Default implementation calls GetTimestamp and gets Ticks.
    /// <summary>
    /// Returns the number of ticks for the timestamp selected from the given event.
    /// The default implementation calls <see cref="GetTimestamp(TEvent)"/> and returns its <see cref="DateTime.Ticks"/>.
    /// </summary>
    /// <param name="e">The event to extract the timestamp from.</param>
    /// <returns>The timestamp expressed as ticks (DateTime.Ticks).</returns>
    long GetTimestampTicks(TEvent e)
    {
        return GetTimestamp(e).Ticks;
    }
}
