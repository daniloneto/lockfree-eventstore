using System;
using System.Threading;
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
    public void Volatile_Read_Write_Works_CrossThread()
    {
        string? location = null;
        var done = false;

        var writer = new Thread(() =>
        {
            PerformanceHelpers.VolatileWrite(ref location, "y");
            Volatile.Write(ref done, true);
        });

        writer.Start();

        var spin = new SpinWait();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(1);
        while (!Volatile.Read(ref done) && DateTime.UtcNow < deadline)
        {
            spin.SpinOnce();
        }

        var read = PerformanceHelpers.VolatileRead(ref location);
        Assert.Equal("y", read);

        writer.Join();
    }

    [Fact]
    public void BoundsCheck_Throws_When_Out_Of_Range()
    {
        PerformanceHelpers.BoundsCheck(0, 1); // ok
        Assert.Throws<IndexOutOfRangeException>(() => PerformanceHelpers.BoundsCheck(1, 1));
        Assert.Throws<IndexOutOfRangeException>(() => PerformanceHelpers.BoundsCheck(unchecked((uint)-1), 1));
    }

    [Fact]
    public void FastMod_Int_And_Long_Work_For_PowerOfTwo()
    {
        Assert.Equal(3, PerformanceHelpers.FastMod(19, 16));
        Assert.Equal(3L, PerformanceHelpers.FastMod(19L, 16L));

        // Additional key cases
        Assert.Equal(0, PerformanceHelpers.FastMod(0, 16));
        Assert.Equal(0, PerformanceHelpers.FastMod(16, 16));
        Assert.Equal(0, PerformanceHelpers.FastMod(123, 1));
        Assert.Equal(0L, PerformanceHelpers.FastMod(0L, 16L));
        Assert.Equal(0L, PerformanceHelpers.FastMod(16L, 16L));
        Assert.Equal(0L, PerformanceHelpers.FastMod(123L, 1L));
    }

    [Fact]
    public void FastMod_Throws_For_Non_PowerOfTwo_Divisors()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => PerformanceHelpers.FastMod(19, 5));
        Assert.Throws<ArgumentOutOfRangeException>(() => PerformanceHelpers.FastMod(19L, 5L));
        Assert.Throws<ArgumentOutOfRangeException>(() => PerformanceHelpers.FastMod(19, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => PerformanceHelpers.FastMod(19L, 0L));
        Assert.Throws<ArgumentOutOfRangeException>(() => PerformanceHelpers.FastMod(19, -2));
    }

    [Fact]
    public void IsPowerOfTwo_Works()
    {
        Assert.True(PerformanceHelpers.IsPowerOfTwo(16));
        Assert.False(PerformanceHelpers.IsPowerOfTwo(18));
        Assert.False(PerformanceHelpers.IsPowerOfTwo(0));
        Assert.False(PerformanceHelpers.IsPowerOfTwo(-2));
    }

    [Fact]
    public void RoundUpToPowerOfTwo_Works_And_Boundaries()
    {
        Assert.Equal(1, PerformanceHelpers.RoundUpToPowerOfTwo(0));
        Assert.Equal(1, PerformanceHelpers.RoundUpToPowerOfTwo(1));
        Assert.Equal(2, PerformanceHelpers.RoundUpToPowerOfTwo(2));
        Assert.Equal(4, PerformanceHelpers.RoundUpToPowerOfTwo(3));
        Assert.Equal(32, PerformanceHelpers.RoundUpToPowerOfTwo(17));

        // Boundary near int.MaxValue: next power (2^31) overflows to int.MinValue for int
        var nearMax = int.MaxValue - 3;
        var rounded = PerformanceHelpers.RoundUpToPowerOfTwo(nearMax);
        Assert.Equal(1 << 31, rounded); // equals int.MinValue due to overflow in int
    }
}
