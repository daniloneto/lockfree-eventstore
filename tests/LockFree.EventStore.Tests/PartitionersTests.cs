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

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ForKey_Throws_When_Partitions_Invalid(int partitions)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Partitioners.ForKey("x", partitions));
    }

    [Fact]
    public void ForKeyId_And_Simple_Map_To_Valid_Range()
    {
        var key = new KeyId(1234);
        var pFast = Partitioners.ForKeyId(key, 16);
        var pSimple = Partitioners.ForKeyIdSimple(key, 16);
        Assert.InRange(pFast, 0, 15);
        Assert.InRange(pSimple, 0, 15);
        Assert.Equal(pFast, pSimple);

        // Also validate a non-power-of-two partition count
        var pFast10 = Partitioners.ForKeyId(key, 10);
        var pSimple10 = Partitioners.ForKeyIdSimple(key, 10);
        Assert.InRange(pFast10, 0, 9);
        Assert.InRange(pSimple10, 0, 9);
        Assert.Equal(pFast10, pSimple10);

        // Distribution sanity: same input, same partitions
        Assert.Equal(Partitioners.ForKeyId(key, 16), Partitioners.ForKeyId(key, 16));
        Assert.Equal(Partitioners.ForKeyIdSimple(key, 16), Partitioners.ForKeyIdSimple(key, 16));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ForKeyId_Throws_When_Partitions_Invalid(int partitions)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Partitioners.ForKeyId(new KeyId(1), partitions));
        Assert.Throws<ArgumentOutOfRangeException>(() => Partitioners.ForKeyIdSimple(new KeyId(1), partitions));
    }

    [Fact]
    public void Partitions_One_Always_Maps_To_Zero()
    {
        // For key-based partitioning
        Assert.Equal(0, Partitioners.ForKey("alpha", 1));
        Assert.Equal(0, Partitioners.ForKey("beta", 1));
        Assert.Equal(0, Partitioners.ForKey(string.Empty, 1));

        // For KeyId-based partitioning (both fast and simple)
        var k0 = new KeyId(0);
        var k1 = new KeyId(123);
        Assert.Equal(0, Partitioners.ForKeyId(k0, 1));
        Assert.Equal(0, Partitioners.ForKeyId(k1, 1));
        Assert.Equal(0, Partitioners.ForKeyIdSimple(k0, 1));
        Assert.Equal(0, Partitioners.ForKeyIdSimple(k1, 1));
    }
}
