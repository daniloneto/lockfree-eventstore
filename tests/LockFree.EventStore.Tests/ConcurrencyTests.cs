using System.Threading.Tasks;
using Xunit;

namespace LockFree.EventStore.Tests;

public class ConcurrencyTests
{
    [Fact]
    public async Task Mpmc_Appends_DoNotBlock_And_CountGrows()
    {
        var options = new EventStoreOptions<int> { CapacityPerPartition = 1000, Partitions = 4 };
        var store = new EventStore<int>(options);
        int producers = 8;
        int eventsPerProducer = 5000;
        var tasks = new Task[producers];
        for (int p = 0; p < producers; p++)
        {
            int id = p;
            tasks[p] = Task.Run(() =>
            {
                for (int i = 0; i < eventsPerProducer; i++)
                    store.TryAppend(id * eventsPerProducer + i);
            }, TestContext.Current.CancellationToken);
        }
        await Task.WhenAll(tasks);
        Assert.True(store.CountApprox > 0);
        Assert.True(store.CountApprox <= options.CapacityPerPartition * options.Partitions);
    }
}
