using LockFree.EventStore;
using Xunit;

namespace LockFree.EventStore.Tests;

public class StoreStatsTests
{
    [Fact]
    public void Constructor_Sets_All_Properties()
    {
        var s = new StoreStats(appendCount: 10, droppedCount: 2, snapshotBytesExposed: 128, windowAdvanceCount: 7);
        Assert.Equal(10, s.AppendCount);
        Assert.Equal(2, s.DroppedCount);
        Assert.Equal(128, s.SnapshotBytesExposed);
        Assert.Equal(7, s.WindowAdvanceCount);
    }

    [Fact]
    public void ToString_Formats_As_Expected()
    {
        var s = new StoreStats(3, 1, 64, 5);
        Assert.Equal("StoreStats(Appends: 3, Dropped: 1, SnapshotBytes: 64, WindowAdvances: 5)", s.ToString());
    }
}
