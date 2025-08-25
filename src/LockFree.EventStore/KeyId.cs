using System.Collections.Concurrent;

namespace LockFree.EventStore;

/// <summary>
/// Represents a unique identifier for a key in the hot path.
/// Using an integer instead of string reduces allocations and improves performance.
/// </summary>
public readonly record struct KeyId(int Value)
{
    /// <summary>
    /// Gets the hash code for partitioning purposes.
    /// <summary>
    /// Returns the underlying integer value as the hash code for this KeyId.
    /// </summary>
    /// <returns>The <see cref="Value"/> of this KeyId.</returns>
    public override int GetHashCode()
    {
        return Value;
    }

    /// <summary>
    /// Returns a string representation of the KeyId.
    /// <summary>
    /// Returns a string representation of the KeyId in the form "KeyId(Value)".
    /// </summary>
    /// <returns>The string "KeyId(Value)" where Value is the underlying integer.</returns>
    public override string ToString()
    {
        return $"KeyId({Value})";
    }

    /// <summary>
    /// Implicit conversion to int for convenience.
    /// </summary>
    public static implicit operator int(KeyId keyId)
    {
        return keyId.Value;
    }
}

/// <summary>
/// Internal mapping between string keys and KeyId values for hot path optimization.
/// Thread-safe and designed for high-performance key resolution.
/// </summary>
public sealed class KeyMap
{
    private readonly ConcurrentDictionary<string, KeyId> _stringToId = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<KeyId, string> _idToString = new();
    private int _nextId;

    /// <summary>
    /// Gets or creates a KeyId for the given string key.
    /// This method is thread-safe and optimized for repeated lookups.
    /// <summary>
    /// Gets the existing KeyId for the specified string key, or atomically creates, registers and returns a new KeyId if the key is not present.
    /// </summary>
    /// <param name="key">The string key to look up (stored using ordinal string comparison).</param>
    /// <returns>The existing or newly created KeyId associated with <paramref name="key"/>.</returns>
    public KeyId GetOrAdd(string key)
    {
        return _stringToId.GetOrAdd(key, k =>
        {
            var keyId = new KeyId(Interlocked.Increment(ref _nextId));
            _ = _idToString.TryAdd(keyId, k);
            return keyId;
        });
    }

    /// <summary>
    /// Attempts to get the KeyId for a given string key.
    /// <summary>
    /// Attempts to retrieve the KeyId associated with the specified string key.
    /// </summary>
    /// <param name="key">The string key to look up (compared using ordinal string comparison).</param>
    /// <param name="id">When this method returns, contains the associated <see cref="KeyId"/> if found; otherwise the default value.</param>
    /// <returns><c>true</c> if the key was found; otherwise <c>false</c>.</returns>
    /// <remarks>
    /// Lookup is thread-safe and does not add new mappings. Throws <see cref="System.ArgumentNullException"/> if <paramref name="key"/> is null.
    /// </remarks>
    public bool TryGet(string key, out KeyId id)
    {
        return _stringToId.TryGetValue(key, out id);
    }

    /// <summary>
    /// Attempts to get the string key for a given KeyId.
    /// <summary>
    /// Attempts to retrieve the string key associated with the given KeyId.
    /// </summary>
    /// <param name="id">The KeyId to look up.</param>
    /// <param name="key">When this method returns, contains the associated string if found; otherwise null.</param>
    /// <returns>true if an association exists for <paramref name="id"/>; otherwise false.</returns>
    public bool TryGet(KeyId id, out string? key)
    {
        return _idToString.TryGetValue(id, out key);
    }

    /// <summary>
    /// Gets all registered key mappings.
    /// <summary>
    /// Returns a read-only view of all registered string-to-KeyId mappings.
    /// </summary>
    /// <returns>An <see cref="IReadOnlyDictionary{TKey, TValue}"/> mapping each string key to its <see cref="KeyId"/>.</returns>
    public IReadOnlyDictionary<string, KeyId> GetAllMappings()
    {
        return _stringToId.AsReadOnly();
    }

    /// <summary>
    /// Gets the current count of registered keys.
    /// </summary>
    public int Count => _stringToId.Count;

    /// <summary>
    /// Clears all key mappings.
    /// <summary>
    /// Removes all key mappings and resets the next KeyId counter to zero.
    /// </summary>
    /// <remarks>
    /// Clears both internal dictionaries and atomically sets the internal _nextId counter to 0 using <see cref="Interlocked.Exchange"/>.
    /// </remarks>
    public void Clear()
    {
        _stringToId.Clear();
        _idToString.Clear();
        _ = Interlocked.Exchange(ref _nextId, 0);
    }
}
