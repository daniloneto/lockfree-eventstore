using System;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class PartitionersTests
{
    [Fact]
    public void ForKey_Maps_Consistently()
    {
        var p1 = Partitioners.ForKey("alpha", 8);
        var p2 = Partitioners.ForKey("alpha", 8);
        Assert.Equal(p1, p2);
        Assert.InRange(p1, 0, 7);
    }

    [Fact]
    public void ForKey_Throws_When_Partitions_Invalid()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Partitioners.ForKey("x", 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Partitioners.ForKey("x", -1));
    }

    [Fact]
    public void ForKeyId_And_Simple_Map_To_Valid_Range()
    {
        var key = new KeyId(1234);
        var pFast = Partitioners.ForKeyId(key, 16);
        var pSimple = Partitioners.ForKeyIdSimple(key, 16);
        Assert.InRange(pFast, 0, 15);
        Assert.InRange(pSimple, 0, 15);

        // Distribution sanity: same input, same partitions
        Assert.Equal(Partitioners.ForKeyId(key, 16), Partitioners.ForKeyId(key, 16));
        Assert.Equal(Partitioners.ForKeyIdSimple(key, 16), Partitioners.ForKeyIdSimple(key, 16));
    }

    [Fact]
    public void ForKeyId_Throws_When_Partitions_Invalid()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Partitioners.ForKeyId(new KeyId(1), 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Partitioners.ForKeyIdSimple(new KeyId(1), 0));
    }
}
