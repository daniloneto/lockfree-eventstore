using System;
using System.Reflection;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class EventStorePrivateHelpersTests
{
    private static MethodInfo GetMethod(Type t, string name, bool isStatic, params Type[] parameterTypes)
    {
        var flags = BindingFlags.NonPublic | (isStatic ? BindingFlags.Static : BindingFlags.Instance);
        var mi = t.GetMethod(name, flags, binder: null, types: parameterTypes, modifiers: null);
        Assert.NotNull(mi);
        return mi!;
    }

    private static EventStore<Event> CreateStore(long windowSizeTicks, int bucketCount = 8)
    {
        return new EventStore<Event>(new EventStoreOptions<Event>
        {
            CapacityPerPartition = 64,
            Partitions = 1,
            TimestampSelector = new EventTimestampSelector(),
            ValueSelector = e => e.Value,
            WindowSizeTicks = windowSizeTicks,
            BucketCount = bucketCount,
            EnableWindowTracking = true
        });
    }

    [Fact]
    public void Mod_Handles_Positive_And_Negative()
    {
        var t = typeof(EventStore<Event>);
        var mi = GetMethod(t, "Mod", isStatic: true, typeof(int), typeof(int));
        var res1 = (int)mi.Invoke(null, new object[] { 6, 5 })!;
        var res2 = (int)mi.Invoke(null, new object[] { -1, 5 })!;
        var res3 = (int)mi.Invoke(null, new object[] { -6, 5 })!;
        Assert.Equal(1, res1);
        Assert.Equal(4, res2);
        Assert.Equal(4, res3);
    }

    [Fact]
    public void AlignDown_Aligns_Floor_For_Positive_And_Negative()
    {
        var t = typeof(EventStore<Event>);
        var mi = GetMethod(t, "AlignDown", isStatic: true, typeof(long), typeof(long));
        long a = (long)mi.Invoke(null, new object[] { 17L, 5L })!;
        long b = (long)mi.Invoke(null, new object[] { 20L, 5L })!;
        long c = (long)mi.Invoke(null, new object[] { -1L, 5L })!;
        Assert.Equal(15L, a);
        Assert.Equal(20L, b);
        Assert.Equal(-5L, c);
    }

    [Fact]
    public void EnsureBucketsInitialized_Sets_Aligned_Buckets_And_Defaults()
    {
        var window = 1000L;
        var store = CreateStore(window, bucketCount: 5);
        var t = store.GetType();
        var mi = GetMethod(t, "EnsureBucketsInitialized", isStatic: false,
            typeof(int), typeof(PartitionWindowState).MakeByRefType(), typeof(long));

        var state = new PartitionWindowState();
        state.Reset();
        object[] args = { 0, state, 1000L };
        _ = mi.Invoke(store, args);
        state = (PartitionWindowState)args[1]!;

        Assert.Equal(200L, state.BucketWidthTicks); // window/buckets
        Assert.NotNull(state.Buckets);
        Assert.Equal(5, state.Buckets!.Length);
        Assert.Equal(0L, state.WindowStartTicks); // 1000 - 1000
        Assert.Equal(1000L, state.WindowEndTicks);
        Assert.Equal(0, state.BucketHead);
        for (int i = 0; i < state.Buckets.Length; i++)
        {
            Assert.Equal(i * 200L, state.Buckets[i].StartTicks);
            Assert.Equal(0, state.Buckets[i].Count);
        }
        Assert.Equal(0, state.Count);
        Assert.Equal(0.0, state.Sum);
        Assert.Equal(double.MaxValue, state.Min);
        Assert.Equal(double.MinValue, state.Max);
    }

    [Fact]
    public void EnsureBucketsInitialized_Aligns_When_WindowStart_Not_Aligned()
    {
        var window = 1000L; // width will be 200
        var store = CreateStore(window, bucketCount: 5);
        var t = store.GetType();
        var mi = GetMethod(t, "EnsureBucketsInitialized", isStatic: false,
            typeof(int), typeof(PartitionWindowState).MakeByRefType(), typeof(long));

        var state = new PartitionWindowState();
        state.Reset();
        object[] args = { 0, state, 950L }; // windowStart = -50, alignDown to -200
        _ = mi.Invoke(store, args);
        state = (PartitionWindowState)args[1]!;

        Assert.Equal(-50L, state.WindowStartTicks);
        Assert.Equal(950L, state.WindowEndTicks);
        Assert.Equal(0, state.BucketHead);
        Assert.NotNull(state.Buckets);
        Assert.Equal(-200L, state.Buckets![0].StartTicks);
        Assert.Equal(0L, state.Buckets[1].StartTicks);
        Assert.Equal(200L, state.Buckets[2].StartTicks);
    }

    [Fact]
    public void TryPrepareWindowAdvance_NoOp_First_Call_Then_True_On_Progress()
    {
        var window = 1000L;
        var store = CreateStore(window, bucketCount: 5);
        var t = store.GetType();
        var mi = GetMethod(t, "TryPrepareWindowAdvance", isStatic: false,
            typeof(long), typeof(PartitionWindowState).MakeByRefType(), typeof(long).MakeByRefType());

        var state = new PartitionWindowState();
        state.Reset();

        // First call initializes buckets and returns false due to no-op advance
        object[] args1 = { 1000L, state, 0L };
        var res1 = (bool)mi.Invoke(store, args1)!;
        state = (PartitionWindowState)args1[1]!;
        var newStart1 = (long)args1[2]!;
        Assert.False(res1);
        Assert.Equal(0L, newStart1);
        Assert.Equal(0L, state.WindowStartTicks);
        Assert.Equal(1000L, state.WindowEndTicks);

        // Advance time a bit
        object[] args2 = { 1101L, state, 0L };
        var res2 = (bool)mi.Invoke(store, args2)!;
        state = (PartitionWindowState)args2[1]!;
        var newStart2 = (long)args2[2]!;
        Assert.True(res2);
        Assert.Equal(101L, newStart2); // 1101 - 1000
    }

    [Fact]
    public void AdvancePartitionWindow_Paths_Covered_NoBoundary_Then_Boundary_Cross()
    {
        var window = 1000L;
        var store = CreateStore(window, bucketCount: 5); // width = 200
        var t = store.GetType();
        var mi = GetMethod(t, "AdvancePartitionWindow", isStatic: false,
            typeof(int), typeof(PartitionWindowState).MakeByRefType(), typeof(long));

        var state = new PartitionWindowState();
        state.Reset();

        // Initial call (no-op as TryPrepare returns false) but initializes buckets
        object[] call0 = { 0, state, 1000L };
        _ = mi.Invoke(store, call0);
        state = (PartitionWindowState)call0[1]!;
        Assert.NotNull(state.Buckets);
        Assert.Equal(0L, state.WindowStartTicks);
        Assert.Equal(1000L, state.WindowEndTicks);

        // Small advance that does not cross bucket boundary (delta < 200)
        object[] call1 = { 0, state, 1100L };
        _ = mi.Invoke(store, call1);
        state = (PartitionWindowState)call1[1]!;
        Assert.Equal(100L, state.WindowStartTicks);
        Assert.Equal(1100L, state.WindowEndTicks);
        Assert.Equal(0, state.BucketHead); // no roll

        // Cross 2 bucket boundaries (delta 2*200 = 400)
        object[] call2 = { 0, state, 1500L };
        _ = mi.Invoke(store, call2);
        state = (PartitionWindowState)call2[1]!;

        // Now aligned start should be floor(WindowStartTicks) boundary, and head should point there
        var tAlignDown = GetMethod(t, "AlignDown", isStatic: true, typeof(long), typeof(long));
        var newAlignedStart = (long)tAlignDown.Invoke(null, new object[] { state.WindowStartTicks, state.BucketWidthTicks })!;
        Assert.Equal(newAlignedStart, state.Buckets![state.BucketHead].StartTicks);
    }

    [Fact]
    public void IsNoopWindowAdvance_Returns_Expected()
    {
        var state = new PartitionWindowState();
        state.Reset();
        state.WindowStartTicks = 1000L;
        state.WindowEndTicks = 2000L;

        var t = typeof(EventStore<Event>);
        var mi = GetMethod(t, "IsNoopWindowAdvance", isStatic: true,
            typeof(PartitionWindowState).MakeByRefType(), typeof(long), typeof(long));

        // Same start/end -> noop
        var args1 = new object[] { state, 1000L, 2000L };
        bool r1 = (bool)mi.Invoke(null, args1)!;
        Assert.True(r1);

        // New start moves forward -> not noop
        var args2 = new object[] { state, 1100L, 2000L };
        bool r2 = (bool)mi.Invoke(null, args2)!;
        Assert.False(r2);

        // Timestamp changed (even if start same) -> not noop
        var args3 = new object[] { state, 1000L, 2001L };
        bool r3 = (bool)mi.Invoke(null, args3)!;
        Assert.False(r3);
    }
}
