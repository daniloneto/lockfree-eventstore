namespace LockFree.EventStore;

/// <summary>
/// Represents an immutable event with key, value, and timestamp as a value type for maximizing performance.
/// Uses a record struct for zero-allocation semantics and contiguous memory layout.
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1716:Identifiers should not match keywords", Justification = "'Event' is a core domain term and part of the public API.")]
public readonly record struct Event(KeyId Key, double Value, long TimestampTicks);

/// <summary>
/// Timestamp selector for Event struct.
/// </summary>
public readonly struct EventTimestampSelector : IEventTimestampSelector<Event>
{
    /// <summary>
    /// Gets the timestamp from an Event.
    /// </summary>
    /// <param name="e">The event.</param>
    /// <summary>
    /// Returns the event's timestamp as a <see cref="DateTime"/> constructed from its <c>TimestampTicks</c>.
    /// </summary>
    /// <param name="e">The event whose timestamp to retrieve.</param>
    /// <returns>A <see cref="DateTime"/> representing the event's timestamp.</returns>
    public DateTime GetTimestamp(Event e)
    {
        return new(e.TimestampTicks);
    }

    /// <summary>
    /// Gets the timestamp ticks from an Event.
    /// </summary>
    /// <param name="e">The event.</param>
    /// <summary>
    /// Returns the raw timestamp ticks from the specified event.
    /// </summary>
    /// <param name="e">The event to read the timestamp from.</param>
    /// <returns>The event's timestamp expressed as ticks.</returns>
    public long GetTimestampTicks(Event e)
    {
        return e.TimestampTicks;
    }
}
