using System;
using System.Threading;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class PerformanceHelpersTests
{
    [Fact]
    public void FastCopySpan_Copies_And_Throws_On_Length_Mismatch()
    {
        var src = new[] { 1, 2, 3 };
        var dst = new int[3];
        PerformanceHelpers.FastCopySpan(src, dst);
        Assert.Equal(src, dst);

        var bad = new int[2];
        Assert.Throws<ArgumentException>(() => PerformanceHelpers.FastCopySpan(src, bad));
    }

    [Fact]
    public void Volatile_Read_Write_Works()
    {
        string? location = null;
        PerformanceHelpers.VolatileWrite(ref location, "x");
        var read = PerformanceHelpers.VolatileRead(ref location);
        Assert.Equal("x", read);
    }

    [Fact]
    public void BoundsCheck_Throws_When_Out_Of_Range()
    {
        PerformanceHelpers.BoundsCheck(0, 1); // ok
        Assert.Throws<IndexOutOfRangeException>(() => PerformanceHelpers.BoundsCheck(1, 1));
    }

    [Fact]
    public void FastMod_Int_And_Long_Work_For_PowerOfTwo()
    {
        Assert.Equal(3, PerformanceHelpers.FastMod(19, 16));
        Assert.Equal(3L, PerformanceHelpers.FastMod(19L, 16L));
    }

    [Fact]
    public void IsPowerOfTwo_And_RoundUpToPowerOfTwo()
    {
        Assert.True(PerformanceHelpers.IsPowerOfTwo(16));
        Assert.False(PerformanceHelpers.IsPowerOfTwo(18));
        Assert.Equal(1, PerformanceHelpers.RoundUpToPowerOfTwo(0));
        Assert.Equal(1, PerformanceHelpers.RoundUpToPowerOfTwo(1));
        Assert.Equal(2, PerformanceHelpers.RoundUpToPowerOfTwo(2));
        Assert.Equal(4, PerformanceHelpers.RoundUpToPowerOfTwo(3));
        Assert.Equal(32, PerformanceHelpers.RoundUpToPowerOfTwo(17));
    }
}
