using System;
using Xunit;

namespace LockFree.EventStore.Tests;

public class PerformanceHelpersEdgeTests
{
    [Fact]
    public void FastMod_Int_Edge_Values_PowerOfTwo()
    {
        Assert.Equal(0, PerformanceHelpers.FastMod(32, 32));
        Assert.Equal(31, PerformanceHelpers.FastMod(31, 32));
        Assert.Equal(0, PerformanceHelpers.FastMod(int.MaxValue & ~31, 32)); // multiple of 32
    }

    [Fact]
    public void FastMod_Long_Edge_Values_PowerOfTwo()
    {
        Assert.Equal(0L, PerformanceHelpers.FastMod(64L, 64L));
        Assert.Equal(63L, PerformanceHelpers.FastMod(63L, 64L));
        Assert.Equal(0L, PerformanceHelpers.FastMod(long.MaxValue & ~63L, 64L));
    }
}
