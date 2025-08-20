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
        var list = new System.Collections.Generic.List<int[]>();
        Buffers.ProcessInChunks<int>(data, span => list.Add(span.ToArray()), chunkSize: 4);
        Assert.Equal(3, list.Count);
        Assert.Equal(new[]{1,2,3,4}, list[0]);
        Assert.Equal(new[]{5,6,7,8}, list[1]);
        Assert.Equal(new[]{9,10}, list[2]);
    }

    [Theory]
    [InlineData(10, 5, 2)]      // exact multiple
    [InlineData(10, 4, 3)]      // remainder produces last smaller chunk
    [InlineData(10, 1, 10)]     // size = 1
    [InlineData(10, 20, 1)]     // size > length
    public void ProcessInChunks_Chunk_Count_Is_Correct(int length, int chunkSize, int expectedChunks)
    {
        var data = Enumerable.Range(1, length).ToArray();
        var chunks = new System.Collections.Generic.List<int[]>();
        Buffers.ProcessInChunks<int>(data, span => chunks.Add(span.ToArray()), chunkSize);
        Assert.Equal(expectedChunks, chunks.Count);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ProcessInChunks_Throws_On_Non_Positive_ChunkSize(int chunkSize)
    {
        var data = new int[5];
        Assert.Throws<ArgumentOutOfRangeException>(() => Buffers.ProcessInChunks<int>(data, _ => { }, chunkSize));
    }

    [Fact]
    public void WithRentedBuffer_Void_And_Result_Versions_Work()
    {
        Buffers.WithRentedBuffer<int>(5, buf =>
        {
            Assert.True(buf.Length >= 5);
            buf[0] = 42;
        }, ArrayPool<int>.Shared);

        int rentedLen = -1;
        var sum = Buffers.WithRentedBuffer<int, int>(6, buf =>
        {
            rentedLen = buf.Length;
            int s = 0;
            for (int i = 0; i < buf.Length && i < 6; i++) buf[i] = i+1;
            for (int i = 0; i < 6; i++) s += buf[i];
            return s;
        }, ArrayPool<int>.Shared);

        Assert.Equal(21, sum);
        Assert.True(rentedLen >= 6);
    }
}
