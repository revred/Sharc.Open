// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

namespace Sharc.Vector.Hnsw;

/// <summary>
/// Public API facade for HNSW approximate nearest neighbor index.
/// Thread-safe for concurrent Search calls after Build/Load.
/// </summary>
public sealed class HnswIndex : IDisposable
{
    // Volatile: these references are replaced wholesale during MergePendingMutations (under write lock).
    // Search (under read lock) must see the latest reference — without volatile, a CPU core could cache
    // a stale reference and search against an old graph/resolver after a merge completes.
    private volatile HnswGraph _graph;
    private volatile IVectorResolver _resolver;
    private readonly VectorDistanceFunction _distanceFn;
    private readonly DistanceMetric _metric;
    private readonly HnswConfig _config;
    private readonly ReaderWriterLockSlim _stateLock = new();
    private readonly Dictionary<long, float[]> _deltaVectors = new();
    private readonly HashSet<long> _tombstones = new();
    private long _version = 1;
    private bool _disposed;

    internal HnswIndex(HnswGraph graph, IVectorResolver resolver,
        DistanceMetric metric, HnswConfig config)
    {
        _graph = graph;
        _resolver = resolver;
        _distanceFn = VectorDistanceFunctions.Resolve(metric);
        _metric = metric;
        _config = config;
    }

    /// <summary>Number of vectors in the index.</summary>
    public int Count
    {
        get
        {
            _stateLock.EnterReadLock();
            try
            {
                ThrowIfDisposed();
                return GetActiveCountNoLock();
            }
            finally
            {
                _stateLock.ExitReadLock();
            }
        }
    }

    /// <summary>Dimensions per vector.</summary>
    public int Dimensions
    {
        get
        {
            _stateLock.EnterReadLock();
            try
            {
                ThrowIfDisposed();
                return _resolver.Dimensions;
            }
            finally
            {
                _stateLock.ExitReadLock();
            }
        }
    }

    /// <summary>Distance metric used by the index.</summary>
    public DistanceMetric Metric => _metric;

    /// <summary>Configuration used to build the index.</summary>
    public HnswConfig Config => _config;

    /// <summary>The internal graph (for persistence).</summary>
    internal HnswGraph Graph => _graph;

    /// <summary>True when upserts/deletes are pending merge into the base graph.</summary>
    public bool HasPendingMutations
    {
        get
        {
            _stateLock.EnterReadLock();
            try
            {
                ThrowIfDisposed();
                return _deltaVectors.Count > 0 || _tombstones.Count > 0;
            }
            finally
            {
                _stateLock.ExitReadLock();
            }
        }
    }

    /// <summary>Monotonic version incremented on each mutation and merge.</summary>
    public long Version
    {
        get
        {
            _stateLock.EnterReadLock();
            try
            {
                ThrowIfDisposed();
                return _version;
            }
            finally
            {
                _stateLock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Returns a point-in-time snapshot of mutable index state, including
    /// pending-mutation counters and a deterministic checksum.
    /// </summary>
    public HnswIndexSnapshot GetSnapshot()
    {
        _stateLock.EnterReadLock();
        try
        {
            ThrowIfDisposed();
            return new HnswIndexSnapshot
            {
                Version = _version,
                BaseNodeCount = _graph.NodeCount,
                ActiveNodeCount = GetActiveCountNoLock(),
                PendingUpsertCount = _deltaVectors.Count,
                PendingDeleteCount = _tombstones.Count,
                HasPendingMutations = _deltaVectors.Count > 0 || _tombstones.Count > 0,
                Checksum = ComputeChecksumNoLock()
            };
        }
        finally
        {
            _stateLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Builds an HNSW index from vectors stored in a Sharc database table.
    /// Optionally persists the index to a shadow table for fast reload.
    /// </summary>
    /// <param name="db">The database instance.</param>
    /// <param name="tableName">Table containing vector data.</param>
    /// <param name="vectorColumn">BLOB column storing float vectors.</param>
    /// <param name="metric">Distance metric (default: Cosine).</param>
    /// <param name="config">HNSW configuration (default: HnswConfig.Default).</param>
    /// <param name="persist">If true, saves the index to a shadow table (default: true).</param>
    /// <returns>A ready-to-search HNSW index.</returns>
    public static HnswIndex Build(SharcDatabase db, string tableName,
        string vectorColumn, DistanceMetric metric = DistanceMetric.Cosine,
        HnswConfig? config = null, bool persist = true)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentException.ThrowIfNullOrEmpty(tableName);
        ArgumentException.ThrowIfNullOrEmpty(vectorColumn);

        var cfg = config ?? HnswConfig.Default;
        cfg.Validate();

        // Scan all vectors from the table
        var vectors = new List<float[]>();
        var rowIds = new List<long>();
        int? expectedDims = null;

        using var jit = db.Jit(tableName);
        using var reader = jit.Query(vectorColumn);
        while (reader.Read())
        {
            long rowId = reader.RowId;
            var blobSpan = reader.GetBlobSpan(0);
            if (!BlobVectorCodec.TryDecode(blobSpan, out ReadOnlySpan<float> decoded) || decoded.Length == 0)
                throw new InvalidOperationException(
                    $"Table '{tableName}' rowid {rowId} has an invalid vector payload length ({blobSpan.Length} bytes).");

            // Validate consistent dimensions
            if (expectedDims == null)
                expectedDims = decoded.Length;
            else if (decoded.Length != expectedDims.Value)
                throw new InvalidOperationException(
                    $"Vector at rowid {rowId} has {decoded.Length} dimensions but expected {expectedDims.Value}.");

            vectors.Add(decoded.ToArray());
            rowIds.Add(rowId);
        }

        if (vectors.Count == 0)
            throw new InvalidOperationException(
                $"Table '{tableName}' has no rows — cannot build HNSW index.");

        var resolver = new MemoryVectorResolver(vectors.ToArray());
        int dimensions = vectors[0].Length;
        var graph = HnswGraphBuilder.Build(resolver, rowIds.ToArray(), metric, cfg);

        if (persist)
        {
            string shadowName = HnswShadowTable.GetTableName(tableName, vectorColumn);
            HnswShadowTable.Save(db, shadowName, graph, cfg, dimensions, metric);
        }

        var index = new HnswIndex(graph, resolver, metric, cfg);
        HnswIndexAutoSyncRegistry.Register(db, tableName, vectorColumn, index);
        return index;
    }

    /// <summary>
    /// Loads a previously persisted HNSW index from its shadow table.
    /// Vectors are re-read from the source table and bound to graph nodes by rowId.
    /// </summary>
    /// <returns>The loaded index, or null if no persisted index exists.</returns>
    public static HnswIndex? Load(SharcDatabase db, string tableName,
        string vectorColumn)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentException.ThrowIfNullOrEmpty(tableName);
        ArgumentException.ThrowIfNullOrEmpty(vectorColumn);

        string shadowName = HnswShadowTable.GetTableName(tableName, vectorColumn);
        var loaded = HnswShadowTable.Load(db, shadowName);
        if (loaded == null)
            return null;

        var (graph, config, dimensions, metric) = loaded.Value;

        // Build rowId → nodeIndex map for correct binding
        var rowIdToIndex = new Dictionary<long, int>(graph.NodeCount);
        for (int i = 0; i < graph.NodeCount; i++)
            rowIdToIndex[graph.GetRowId(i)] = i;

        // Re-read vectors from source table and bind by rowId
        var vectors = new float[graph.NodeCount][];
        int resolved = 0;

        using var jit = db.Jit(tableName);
        using var reader = jit.Query(vectorColumn);
        while (reader.Read())
        {
            long rowId = reader.RowId;
            if (rowIdToIndex.TryGetValue(rowId, out int nodeIndex))
            {
                var blobSpan = reader.GetBlobSpan(0);
                if (!BlobVectorCodec.TryDecode(blobSpan, out ReadOnlySpan<float> decoded))
                    throw new InvalidOperationException(
                        $"Table '{tableName}' rowid {rowId} has an invalid vector payload length ({blobSpan.Length} bytes).");

                if (decoded.Length != dimensions)
                {
                    throw new InvalidOperationException(
                        $"Table '{tableName}' rowid {rowId} has {decoded.Length} dimensions, " +
                        $"but persisted HNSW index expects {dimensions}.");
                }

                vectors[nodeIndex] = decoded.ToArray();
                resolved++;
            }
        }

        if (resolved != graph.NodeCount)
            throw new InvalidOperationException(
                $"HNSW index is stale: graph has {graph.NodeCount} nodes but only {resolved} " +
                $"matching rows found in table '{tableName}'. Rebuild the index.");

        var resolver = new MemoryVectorResolver(vectors);
        var index = new HnswIndex(graph, resolver, metric, config);
        HnswIndexAutoSyncRegistry.Register(db, tableName, vectorColumn, index);
        return index;
    }

    /// <summary>
    /// Loads an existing persisted index if available and its metric matches,
    /// otherwise builds and persists. Throws if the loaded index has a different
    /// metric than requested.
    /// </summary>
    public static HnswIndex LoadOrBuild(SharcDatabase db, string tableName,
        string vectorColumn, DistanceMetric metric = DistanceMetric.Cosine,
        HnswConfig? config = null)
    {
        var loaded = Load(db, tableName, vectorColumn);
        if (loaded != null)
        {
            // Validate metric matches
            if (loaded.Metric != metric)
                throw new InvalidOperationException(
                    $"Persisted HNSW index uses {loaded.Metric} but {metric} was requested. " +
                    $"Rebuild the index with Build(persist: true) using the desired metric.");
            return loaded;
        }

        return Build(db, tableName, vectorColumn, metric, config, persist: true);
    }

    /// <summary>
    /// Builds an HNSW index from in-memory vectors (for testing and non-database use).
    /// </summary>
    internal static HnswIndex BuildFromMemory(float[][] vectors, long[] rowIds,
        DistanceMetric metric = DistanceMetric.Cosine, HnswConfig? config = null)
    {
        var cfg = config ?? HnswConfig.Default;
        cfg.Validate();

        if (vectors.Length == 0)
            throw new InvalidOperationException("Cannot build HNSW index from zero vectors.");

        if (vectors.Length != rowIds.Length)
            throw new ArgumentException(
                $"vectors.Length ({vectors.Length}) must match rowIds.Length ({rowIds.Length}).",
                nameof(rowIds));

        var resolver = new MemoryVectorResolver(vectors);
        var graph = HnswGraphBuilder.Build(resolver, rowIds, metric, cfg);

        return new HnswIndex(graph, resolver, metric, cfg);
    }

    /// <summary>
    /// Adds or updates a vector in the mutable delta layer.
    /// Changes are visible to search immediately and can be compacted via
    /// <see cref="MergePendingMutations"/>.
    /// </summary>
    public void Upsert(long rowId, ReadOnlySpan<float> vector)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (vector.Length != Dimensions)
            throw new ArgumentException(
                $"Vector has {vector.Length} dimensions but index has {Dimensions}.",
                nameof(vector));

        var vectorCopy = vector.ToArray();

        _stateLock.EnterWriteLock();
        try
        {
            ThrowIfDisposed();
            _deltaVectors[rowId] = vectorCopy;
            _tombstones.Remove(rowId);
            _version++;
        }
        finally
        {
            _stateLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Marks the given row as deleted from search results.
    /// Returns false if the row does not exist in the current mutable view.
    /// </summary>
    public bool Delete(long rowId)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _stateLock.EnterWriteLock();
        try
        {
            ThrowIfDisposed();

            bool changed = false;
            bool isBaseRow = _graph.TryGetNodeIndex(rowId, out _);

            if (_deltaVectors.Remove(rowId))
                changed = true;

            if (isBaseRow && _tombstones.Add(rowId))
                changed = true;

            if (changed)
                _version++;

            return changed;
        }
        finally
        {
            _stateLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Incrementally merges pending mutations into the base graph.
    /// Updated vectors are patched in-place in the resolver (topology preserved).
    /// New vectors are inserted into the graph using the HNSW insertion algorithm.
    /// Tombstones remain as-is — use <see cref="Compact"/> to remove them.
    /// </summary>
    public void MergePendingMutations()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _stateLock.EnterWriteLock();
        try
        {
            ThrowIfDisposed();

            // When tombstones exist, nodes must be removed from graph topology —
            // this requires a full rebuild since we can't incrementally remove HNSW nodes.
            // Check inside the lock to avoid TOCTOU race with concurrent Delete().
            if (_tombstones.Count > 0)
            {
                CompactNoLock();
                return;
            }

            if (_deltaVectors.Count == 0)
                return;

            var memResolver = _resolver as MemoryVectorResolver;
            if (memResolver == null)
            {
                // Non-memory resolvers cannot do incremental merge — fall back to full rebuild.
                CompactNoLock();
                return;
            }

            // Phase 1: Update existing vectors in-place (no topology change).
            foreach (var pair in _deltaVectors)
            {
                if (_tombstones.Contains(pair.Key))
                    continue;

                if (_graph.TryGetNodeIndex(pair.Key, out int nodeIndex))
                {
                    memResolver.UpdateVector(nodeIndex, pair.Value);
                }
            }

            // Phase 2: Insert new vectors into the graph topology.
            var rawDistanceFn = VectorDistanceFunctions.Resolve(_metric);
            bool isDotProduct = _metric == DistanceMetric.DotProduct;
            VectorDistanceFunction distanceFn = isDotProduct
                ? (a, b) => -rawDistanceFn(a, b)
                : rawDistanceFn;

            var rng = _config.Seed != 0 ? new Random(_config.Seed) : new Random();

            foreach (var pair in _deltaVectors)
            {
                if (_tombstones.Contains(pair.Key))
                    continue;
                if (_graph.TryGetNodeIndex(pair.Key, out _))
                    continue; // Already updated in phase 1

                // Assign a random level and add the node to the graph.
                int level = HnswLevelAssigner.AssignSingleLevel(rng, _config.ML);
                memResolver.AppendVector(pair.Value);
                int newNodeIndex = _graph.AddNode(pair.Key, level);

                // Insert into graph topology using Algorithm 1.
                // InsertNode handles entry point / max level updates internally.
                HnswGraphBuilder.InsertNode(_graph, newNodeIndex, memResolver, distanceFn, _config);
            }

            _deltaVectors.Clear();
            // Tombstones are kept — they filter search results until Compact().
            _version++;
        }
        finally
        {
            _stateLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Fully rebuilds the base graph from scratch, removing all tombstoned nodes
    /// and reclaiming space. Use this when delete/tombstone count is high.
    /// This is the legacy behavior of MergePendingMutations before incremental merge.
    /// </summary>
    public void Compact()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _stateLock.EnterWriteLock();
        try
        {
            ThrowIfDisposed();
            CompactNoLock();
        }
        finally
        {
            _stateLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Core compaction logic. Caller must hold the write lock.
    /// </summary>
    private void CompactNoLock()
    {
        var vectors = new List<float[]>(_graph.NodeCount + _deltaVectors.Count);
        var rowIds = new List<long>(_graph.NodeCount + _deltaVectors.Count);

        for (int i = 0; i < _graph.NodeCount; i++)
        {
            long rowId = _graph.GetRowId(i);
            if (_tombstones.Contains(rowId))
                continue;

            if (_deltaVectors.TryGetValue(rowId, out var updated))
                vectors.Add(updated);
            else
                vectors.Add(_resolver.GetVector(i).ToArray());

            rowIds.Add(rowId);
        }

        foreach (var pair in _deltaVectors)
        {
            if (_tombstones.Contains(pair.Key))
                continue;
            if (_graph.TryGetNodeIndex(pair.Key, out _))
                continue; // already included as update

            rowIds.Add(pair.Key);
            vectors.Add(pair.Value);
        }

        if (vectors.Count == 0)
            throw new InvalidOperationException(
                "Cannot compact because it would produce an empty index.");

        var resolver = new MemoryVectorResolver(vectors.ToArray());
        var graph = HnswGraphBuilder.Build(resolver, rowIds.ToArray(), _metric, _config);

        _resolver = resolver;
        _graph = graph;
        _deltaVectors.Clear();
        _tombstones.Clear();
        _version++;
    }

    /// <summary>
    /// Searches for the k nearest neighbors to the query vector.
    /// </summary>
    /// <param name="queryVector">The query vector (must match index dimensions).</param>
    /// <param name="k">Number of nearest neighbors to return.</param>
    /// <param name="ef">Beam width (higher = better recall, slower). Null = use config default.</param>
    /// <returns>Search results ordered by distance.</returns>
    public VectorSearchResult Search(ReadOnlySpan<float> queryVector, int k, int? ef = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _stateLock.EnterReadLock();
        try
        {
            ThrowIfDisposed();

            if (queryVector.Length != _resolver.Dimensions)
                throw new ArgumentException(
                    $"Query vector has {queryVector.Length} dimensions but index has {_resolver.Dimensions}.",
                    nameof(queryVector));

            if (k <= 0)
                throw new ArgumentOutOfRangeException(nameof(k), k, "k must be positive.");

            int effectiveEf = ef ?? _config.EfSearch;
            effectiveEf = Math.Max(effectiveEf, k);

            if (_deltaVectors.Count == 0 && _tombstones.Count == 0)
            {
                return HnswGraphSearcher.Search(_graph, queryVector, k, effectiveEf,
                    _resolver, _distanceFn, _metric);
            }

            return SearchWithMutableOverlayNoLock(queryVector, k, effectiveEf);
        }
        finally
        {
            _stateLock.ExitReadLock();
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;

        _stateLock.EnterWriteLock();
        try
        {
            if (_disposed) return;
            _disposed = true;
            _deltaVectors.Clear();
            _tombstones.Clear();
        }
        finally
        {
            _stateLock.ExitWriteLock();
        }
    }

    private VectorSearchResult SearchWithMutableOverlayNoLock(
        ReadOnlySpan<float> queryVector, int k, int effectiveEf)
    {
        int activeCount = GetActiveCountNoLock();
        if (activeCount == 0)
            return new VectorSearchResult(new List<VectorMatch>());

        int targetK = Math.Min(k, activeCount);
        var heap = new VectorTopKHeap(targetK, isMinHeap: _metric != DistanceMetric.DotProduct);
        var seen = new HashSet<long>();

        // Fast path: get a broad ANN candidate set from the stable base graph.
        if (_graph.NodeCount > 0)
        {
            int searchK = Math.Min(_graph.NodeCount, Math.Max(targetK * 4, effectiveEf));
            var baseCandidates = HnswGraphSearcher.Search(
                _graph, queryVector, searchK, effectiveEf, _resolver, _distanceFn, _metric);

            for (int i = 0; i < baseCandidates.Count; i++)
            {
                var match = baseCandidates[i];
                if (_tombstones.Contains(match.RowId))
                    continue;

                if (_deltaVectors.TryGetValue(match.RowId, out var updated))
                {
                    heap.TryInsert(match.RowId, _distanceFn(queryVector, updated));
                }
                else
                {
                    heap.TryInsert(match.RowId, match.Distance);
                }

                seen.Add(match.RowId);
            }
        }

        // Include pending inserts/updates from delta that may not be in ANN candidates.
        foreach (var pair in _deltaVectors)
        {
            if (_tombstones.Contains(pair.Key) || seen.Contains(pair.Key))
                continue;

            heap.TryInsert(pair.Key, _distanceFn(queryVector, pair.Value));
            seen.Add(pair.Key);
        }

        // Correctness fallback: if ANN+delta did not produce enough rows, scan remaining base rows.
        if (heap.Count < targetK)
        {
            for (int i = 0; i < _graph.NodeCount; i++)
            {
                long rowId = _graph.GetRowId(i);
                if (seen.Contains(rowId) || _tombstones.Contains(rowId))
                    continue;

                float distance = _deltaVectors.TryGetValue(rowId, out var updated)
                    ? _distanceFn(queryVector, updated)
                    : _distanceFn(queryVector, _resolver.GetVector(i));

                heap.TryInsert(rowId, distance);
                seen.Add(rowId);
            }
        }

        return heap.ToResult();
    }

    private int GetActiveCountNoLock()
    {
        int count = 0;
        for (int i = 0; i < _graph.NodeCount; i++)
        {
            long rowId = _graph.GetRowId(i);
            if (_tombstones.Contains(rowId))
                continue;
            count++;
        }

        foreach (var pair in _deltaVectors)
        {
            if (_tombstones.Contains(pair.Key))
                continue;
            if (_graph.TryGetNodeIndex(pair.Key, out _))
                continue;
            count++;
        }

        return count;
    }

    private uint ComputeChecksumNoLock()
    {
        const uint offset = 2166136261;
        uint hash = offset;

        // Base graph topology and immutable config/metric.
        byte[] serialized = HnswSerializer.Serialize(_graph, _config, _resolver.Dimensions, _metric);
        for (int i = 0; i < serialized.Length; i++)
            hash = AddByte(hash, serialized[i]);

        // Pending tombstones in deterministic order.
        if (_tombstones.Count > 0)
        {
            var deleted = _tombstones.ToArray();
            Array.Sort(deleted);
            for (int i = 0; i < deleted.Length; i++)
                hash = AddInt64(hash, deleted[i]);
        }

        // Pending upserts in deterministic order.
        if (_deltaVectors.Count > 0)
        {
            var rowIds = _deltaVectors.Keys.ToArray();
            Array.Sort(rowIds);
            for (int i = 0; i < rowIds.Length; i++)
            {
                long rowId = rowIds[i];
                hash = AddInt64(hash, rowId);
                var vector = _deltaVectors[rowId];
                hash = AddInt32(hash, vector.Length);
                for (int d = 0; d < vector.Length; d++)
                    hash = AddInt32(hash, BitConverter.SingleToInt32Bits(vector[d]));
            }
        }

        return hash;
    }

    private static uint AddByte(uint hash, byte value)
    {
        const uint prime = 16777619;
        return (hash ^ value) * prime;
    }

    private static uint AddInt32(uint hash, int value)
    {
        unchecked
        {
            hash = AddByte(hash, (byte)value);
            hash = AddByte(hash, (byte)(value >> 8));
            hash = AddByte(hash, (byte)(value >> 16));
            hash = AddByte(hash, (byte)(value >> 24));
            return hash;
        }
    }

    private static uint AddInt64(uint hash, long value)
    {
        unchecked
        {
            hash = AddByte(hash, (byte)value);
            hash = AddByte(hash, (byte)(value >> 8));
            hash = AddByte(hash, (byte)(value >> 16));
            hash = AddByte(hash, (byte)(value >> 24));
            hash = AddByte(hash, (byte)(value >> 32));
            hash = AddByte(hash, (byte)(value >> 40));
            hash = AddByte(hash, (byte)(value >> 48));
            hash = AddByte(hash, (byte)(value >> 56));
            return hash;
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}
