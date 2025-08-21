using System;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class KeyIdAndKeyMapTests
{
    [Fact]
    public void KeyId_Behavior_Equals_HashCode_ToString_Implicit()
    {
        var k1 = new KeyId(42);
        var k2 = new KeyId(42);
        var k3 = new KeyId(7);

        Assert.Equal(k1, k2);
        Assert.NotEqual(k1, k3);
        Assert.Equal(42, k1.Value);
        Assert.Equal(42, k1.GetHashCode());
        Assert.Equal("KeyId(42)", k1.ToString());

        int iv = k1; // implicit
        Assert.Equal(42, iv);
    }

    [Fact]
    public void KeyMap_GetOrAdd_And_TryGet_BothWays()
    {
        var map = new KeyMap();
        var idA1 = map.GetOrAdd("A");
        var idA2 = map.GetOrAdd("A");
        var idB = map.GetOrAdd("B");

        Assert.Equal(idA1, idA2);
        Assert.NotEqual(idA1, idB);
        Assert.Equal(2, map.Count);

        Assert.True(map.TryGet("A", out var idA));
        Assert.Equal(idA1, idA);
        Assert.False(map.TryGet("X", out _));

        Assert.True(map.TryGet(idB, out var keyB));
        Assert.Equal("B", keyB);
        Assert.False(map.TryGet(new KeyId(999), out _));

        var all = map.GetAllMappings();
        Assert.True(all.ContainsKey("A"));
        Assert.True(all.ContainsKey("B"));
        Assert.Equal(idA1, all["A"]);
        Assert.Equal(map.Count, all.Count);
    }

    [Fact]
    public void KeyMap_Clear_Resets_State_And_Sequence()
    {
        var map = new KeyMap();
        var id1 = map.GetOrAdd("one");
        var id2 = map.GetOrAdd("two");
        Assert.Equal(2, map.Count);

        map.Clear();
        Assert.Equal(0, map.Count);
        Assert.False(map.TryGet("one", out _));
        Assert.False(map.TryGet(id1, out _));

        var id1Again = map.GetOrAdd("one");
        Assert.Equal(1, id1Again.Value); // sequence restarted
    }
}
