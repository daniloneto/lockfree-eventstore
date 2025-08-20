using System;
using System.Buffers;
using System.Linq;
using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class BuffersTests
{
    [Fact]
    public void Rent_And_Return_Arrays_Work()
    {
        var ev = Buffers.RentEvents(10);
        Assert.True(ev.Length >= 10);
        Buffers.ReturnEvents(ev);

        var ints = Buffers.RentInts(8);
        Assert.True(ints.Length >= 8);
        Buffers.ReturnInts(ints);

        var dbls = Buffers.RentDoubles(6);
        Assert.True(dbls.Length >= 6);
        Buffers.ReturnDoubles(dbls);

        var longs = Buffers.RentLongs(4);
        Assert.True(longs.Length >= 4);
        Buffers.ReturnLongs(longs);
    }

    [Fact]
    public void ProcessInChunks_Splits_Correctly()
    {
        var data = Enumerable.Range(1, 10).ToArray();
        var chunks = new int[0][];
        var list = new System.Collections.Generic.List<int[]>();
        Buffers.ProcessInChunks<int>(data, span => list.Add(span.ToArray()), chunkSize: 4);
        Assert.Equal(3, list.Count);
        Assert.Equal(new[]{1,2,3,4}, list[0]);
        Assert.Equal(new[]{5,6,7,8}, list[1]);
        Assert.Equal(new[]{9,10}, list[2]);
    }

    [Fact]
    public void WithRentedBuffer_Void_And_Result_Versions_Work()
    {
        Buffers.WithRentedBuffer<int>(5, buf =>
        {
            Assert.True(buf.Length >= 5);
            buf[0] = 42;
        }, ArrayPool<int>.Shared);

        var sum = Buffers.WithRentedBuffer<int, int>(6, buf =>
        {
            int s = 0;
            for (int i = 0; i < buf.Length && i < 6; i++) buf[i] = i+1;
            for (int i = 0; i < 6; i++) s += buf[i];
            return s;
        }, ArrayPool<int>.Shared);

        Assert.True(sum >= 21);
    }
}
