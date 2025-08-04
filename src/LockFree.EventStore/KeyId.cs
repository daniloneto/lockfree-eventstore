using System.Collections.Concurrent;

namespace LockFree.EventStore;

/// <summary>
/// Represents a unique identifier for a key in the hot path.
/// Using an integer instead of string reduces allocations and improves performance.
/// </summary>
public readonly record struct KeyId(int Value)
{    /// <summary>
    /// Gets the hash code for partitioning purposes.
    /// </summary>
    public override int GetHashCode() => Value;
    
    /// <summary>
    /// Returns a string representation of the KeyId.
    /// </summary>
    public override string ToString() => $"KeyId({Value})";
    
    /// <summary>
    /// Implicit conversion to int for convenience.
    /// </summary>
    public static implicit operator int(KeyId keyId) => keyId.Value;
}

/// <summary>
/// Internal mapping between string keys and KeyId values for hot path optimization.
/// Thread-safe and designed for high-performance key resolution.
/// </summary>
public sealed class KeyMap
{
    private readonly ConcurrentDictionary<string, KeyId> _stringToId = 
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<KeyId, string> _idToString = 
        new();
    private int _nextId = 0;

    /// <summary>
    /// Gets or creates a KeyId for the given string key.
    /// This method is thread-safe and optimized for repeated lookups.
    /// </summary>
    public KeyId GetOrAdd(string key)
    {
        return _stringToId.GetOrAdd(key, k =>
        {
            var keyId = new KeyId(Interlocked.Increment(ref _nextId));
            _idToString.TryAdd(keyId, k);
            return keyId;
        });
    }

    /// <summary>
    /// Attempts to get the KeyId for a given string key.
    /// </summary>
    public bool TryGet(string key, out KeyId id) => _stringToId.TryGetValue(key, out id);

    /// <summary>
    /// Attempts to get the string key for a given KeyId.
    /// </summary>
    public bool TryGet(KeyId id, out string? key) => _idToString.TryGetValue(id, out key);

    /// <summary>
    /// Gets all registered key mappings.
    /// </summary>
    public IReadOnlyDictionary<string, KeyId> GetAllMappings() => _stringToId.AsReadOnly();

    /// <summary>
    /// Gets the current count of registered keys.
    /// </summary>
    public int Count => _stringToId.Count;

    /// <summary>
    /// Clears all key mappings.
    /// </summary>
    public void Clear()
    {
        _stringToId.Clear();
        _idToString.Clear();
        Interlocked.Exchange(ref _nextId, 0);
    }
}
