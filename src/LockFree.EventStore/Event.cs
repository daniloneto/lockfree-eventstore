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
    /// <returns>The timestamp of the event.</returns>
    public DateTime GetTimestamp(Event e)
    {
        // Explicitly specify the kind to avoid defaulting to Unspecified implicitly
        return new DateTime(e.TimestampTicks, DateTimeKind.Unspecified);
    }

    /// <summary>
    /// Gets the timestamp ticks from an Event.
    /// </summary>
    /// <param name="e">The event.</param>
    /// <returns>The timestamp ticks of the event.</returns>
    public long GetTimestampTicks(Event e)
    {
        return e.TimestampTicks;
    }
}
