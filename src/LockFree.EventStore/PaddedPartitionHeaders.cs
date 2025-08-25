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
    /// <summary>
    /// Creates a PaddedPartitionHeaders container for the specified number of partitions and initializes each partition header.
    /// </summary>
    /// <param name="partitionCount">Number of partition headers to allocate and expose via the <see cref="Count"/> property.</param>
    /// <param name="capacityPerPartition">Initial capacity passed to each <see cref="PartitionHeader"/> instance.</param>
    /// <remarks>
    /// The constructor allocates backing storage sized and laid out so each <see cref="PartitionHeader"/> starts on a 64-byte cache-line boundary (see <see cref="CacheLineSize"/>),
    /// then constructs a <see cref="PartitionHeader"/> for each partition index [0 .. partitionCount-1] using <paramref name="capacityPerPartition"/>.
    /// </remarks>
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
    /// <summary>
    /// Maps a logical partition index to the backing array index that ensures each partition's
    /// header is laid out to avoid false sharing by placing it on a separate cache line when possible.
    /// </summary>
    /// <param name="partitionIndex">Zero-based partition index.</param>
    /// <returns>The array index corresponding to the padded location for the given partition.</returns>
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
    /// <summary>
    /// Returns a reference to the PartitionHeader for the specified zero-based partition index,
    /// taking into account per-partition padding so each header occupies a separate cache line.
    /// </summary>
    /// <param name="partitionIndex">Zero-based index of the partition; must be in range [0, <see cref="Count"/>).</param>
    /// <returns>By-reference access to the PartitionHeader for the given partition.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="partitionIndex"/> is negative or is greater than or equal to <see cref="Count"/>.</exception>
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
    /// <summary>
    /// Returns an enumerator that iterates the partition headers in partition order while respecting the cache-line padding layout.
    /// </summary>
    /// <returns>A <see cref="HeaderEnumerator"/> for enumerating the padded partition headers.</returns>
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

        /// <summary>
        /// Initializes a new <see cref="HeaderEnumerator"/> for iterating the provided <see cref="PaddedPartitionHeaders"/>.
        /// </summary>
        /// <remarks>
        /// The enumerator is positioned before the first element (call <see cref="MoveNext"/> to advance to the first header).
        /// </remarks>
        internal HeaderEnumerator(PaddedPartitionHeaders headers)
        {
            _headers = headers;
            _currentIndex = -1;
        }

        /// <summary>
        /// Advances the enumerator to the next partition header.
        /// </summary>
        /// <returns>True if the enumerator successfully advanced to the next header; false if the end has been reached.</returns>
        public bool MoveNext()
        {
            _currentIndex++;
            return _currentIndex < _headers.Count;
        }

        public ref readonly PartitionHeader Current => ref _headers.GetHeader(_currentIndex);
    }
}
