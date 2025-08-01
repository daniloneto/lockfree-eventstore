using System;
using LockFree.EventStore;

namespace LockFree.EventStore.Tests;

public sealed record Order(int Id, decimal Amount, DateTime Timestamp);

public sealed class OrderTimestampSelector : IEventTimestampSelector<Order>
{
    public DateTime GetTimestamp(Order e) => e.Timestamp;
}
