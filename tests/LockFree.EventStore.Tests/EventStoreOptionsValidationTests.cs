using System;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class EventStoreOptionsValidationTests
{
    [Fact]
    public void Validate_Throws_When_BucketCount_Zero()
    {
        var opts = new EventStoreOptions<Order>
        {
            BucketCount = 0
        };
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.Validate());
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    public void Validate_Throws_When_BucketWidthTicks_NonPositive(long width)
    {
        var opts = new EventStoreOptions<Order>
        {
            BucketWidthTicks = width
        };
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_Throws_When_Window_Less_Than_Width()
    {
        var opts = new EventStoreOptions<Order>
        {
            WindowSizeTicks = TimeSpan.FromMinutes(1).Ticks,
            BucketWidthTicks = TimeSpan.FromMinutes(2).Ticks
        };
        Assert.Throws<ArgumentException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_Throws_When_Window_Not_Multiple_Of_Width()
    {
        var opts = new EventStoreOptions<Order>
        {
            WindowSizeTicks = TimeSpan.FromMinutes(5).Ticks,
            BucketWidthTicks = TimeSpan.FromMinutes(2).Ticks
        };
        Assert.Throws<ArgumentException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_Allows_Valid_Config()
    {
        var opts = new EventStoreOptions<Order>
        {
            BucketCount = 4,
            WindowSizeTicks = TimeSpan.FromMinutes(5).Ticks,
            BucketWidthTicks = TimeSpan.FromMinutes(1).Ticks
        };
        opts.Validate();
    }

    [Fact]
    public void EventStore_Constructor_Invokes_Validate()
    {
        var opts = new EventStoreOptions<Order>
        {
            BucketCount = 0
        };
        Assert.Throws<ArgumentOutOfRangeException>(() => new EventStore<Order>(opts));
    }
}
