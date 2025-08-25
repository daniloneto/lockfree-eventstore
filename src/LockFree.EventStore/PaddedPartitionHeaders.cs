using System.Runtime.InteropServices;

namespace LockFree.EventStore;

/// <summary>
/// Array of partition headers with cache line isolation to prevent false sharing.
/// Each header is guaranteed to be on its own 64-byte cache line.
/// </summary>
internal sealed class PaddedPartitionHeaders
{
    private readonly PartitionHeader[] _headers;

    /// <summary>
    /// Cache line size in bytes (typically 64 bytes on modern CPUs)
    /// </summary>
    public const int CacheLineSize = 64;

    /// <summary>
    /// Creates a new array of padded partition headers.
    /// </summary>
    /// <param name="partitionCount">Number of partitions</param>
    /// <param name="capacityPerPartition">Capacity for each partition</param>
    public PaddedPartitionHeaders(int partitionCount, int capacityPerPartition)
    {
        Count = partitionCount;

        // Allocate array with extra space for alignment
        // We need to ensure each PartitionHeader starts at a cache line boundary
        var headerSize = Marshal.SizeOf<PartitionHeader>();
        var paddedSize = (headerSize + CacheLineSize - 1) / CacheLineSize * CacheLineSize;

        // Calculate total padded headers needed
        var totalPaddedHeaders = ((paddedSize * partitionCount) + CacheLineSize - 1) / Marshal.SizeOf<PartitionHeader>();

        _headers = new PartitionHeader[Math.Max(partitionCount, totalPaddedHeaders)];

        // Initialize each header at cache line boundaries
        for (var i = 0; i < partitionCount; i++)
        {
            var index = GetPaddedIndex(i);
            _headers[index] = new PartitionHeader(capacityPerPartition);
        }
    }

    /// <summary>
    /// Gets the array index for a partition that ensures cache line isolation.
    /// </summary>
    private static int GetPaddedIndex(int partitionIndex)
    {
        // Calculate the index that places each header on its own cache line
        var headerSize = Marshal.SizeOf<PartitionHeader>();
        var headersPerCacheLine = CacheLineSize / headerSize;

        // If headers fit within cache line, use stride to separate them
        if (headersPerCacheLine > 1)
        {
            var stride = Math.Max(headersPerCacheLine, 2); // Minimum stride of 2
            return partitionIndex * stride;
        }

        // If header is larger than cache line, just use sequential indexing
        return partitionIndex;
    }

    /// <summary>
    /// Gets a reference to the header for the specified partition.
    /// </summary>
    public ref PartitionHeader GetHeader(int partitionIndex)
    {
        if (partitionIndex < 0 || partitionIndex >= Count)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionIndex));
        }

        var index = GetPaddedIndex(partitionIndex);
        return ref _headers[index];
    }

    /// <summary>
    /// Gets the number of partitions.
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// Provides enumeration over all partition headers.
    /// </summary>
    public HeaderEnumerator GetEnumerator()
    {
        return new(this);
    }

    /// <summary>
    /// Enumerator for partition headers that respects padding.
    /// </summary>
    public ref struct HeaderEnumerator
    {
        private readonly PaddedPartitionHeaders _headers;
        private int _currentIndex;

        internal HeaderEnumerator(PaddedPartitionHeaders headers)
        {
            _headers = headers;
            _currentIndex = -1;
        }

        public bool MoveNext()
        {
            _currentIndex++;
            return _currentIndex < _headers.Count;
        }

        public ref readonly PartitionHeader Current => ref _headers.GetHeader(_currentIndex);
    }
}
