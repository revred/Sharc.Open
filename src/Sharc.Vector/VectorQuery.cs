// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sharc.Core.Trust;
using Sharc.Trust;
using Sharc.Vector.Hnsw;
using Sharc.Views;

namespace Sharc.Vector;

/// <summary>
/// A pre-compiled vector similarity search handle. Pre-resolves table schema,
/// vector column ordinal, distance metric, and dimension count at creation time.
/// Reusable across multiple searches — only the query vector changes per call.
/// </summary>
/// <remarks>
/// <para>Created via <see cref="SharcDatabaseExtensions.Vector"/>.
/// Follows the same lifecycle pattern as <see cref="JitQuery"/>: create once,
/// search many times, dispose when done.</para>
/// <para>Internally uses <see cref="JitQuery"/> for table scanning and filter
/// composition, layering distance computation on top.</para>
/// <para>This type is <b>not thread-safe</b>.</para>
/// </remarks>
public sealed class VectorQuery : IDisposable
{
    private SharcDatabase? _db;
    private readonly JitQuery _innerJit;
    private readonly string _tableName;
    private readonly string _vectorColumnName;
    private readonly int _dimensions;
    private readonly DistanceMetric _metric;
    private readonly VectorDistanceFunction _distanceFn;

    internal VectorQuery(
        SharcDatabase db,
        JitQuery innerJit,
        string tableName,
        string vectorColumnName,
        int dimensions,
        DistanceMetric metric)
    {
        _db = db;
        _innerJit = innerJit;
        _tableName = tableName;
        _vectorColumnName = vectorColumnName;
        _dimensions = dimensions;
        _metric = metric;
        _distanceFn = VectorDistanceFunctions.Resolve(metric);
    }

    // ── Agent Entitlements ─────────────────────────────────────

    /// <summary>
    /// Sets the agent whose entitlements are enforced on every search.
    /// Table and column access is validated at search time; throws
    /// <see cref="UnauthorizedAccessException"/> if the agent lacks permission.
    /// </summary>
    /// <param name="agent">The agent to enforce, or null to clear.</param>
    public VectorQuery WithAgent(AgentInfo? agent)
    {
        _agent = agent;
        return this;
    }

    /// <summary>
    /// Sets a row-level access evaluator for multi-tenant isolation.
    /// Rows that fail the evaluator are silently skipped during scans —
    /// they are never distance-computed or returned in results.
    /// </summary>
    /// <param name="evaluator">The evaluator, or null to clear.</param>
    public VectorQuery WithRowEvaluator(IRowAccessEvaluator? evaluator)
    {
        _innerJit.WithRowAccess(evaluator);
        return this;
    }

    private AgentInfo? _agent;

    // ── HNSW Index ────────────────────────────────────────────────

    private HnswIndex? _hnswIndex;
    // Below this threshold, flat scan is preferred over HNSW graph traversal.
    // At 256 vectors, flat scan costs ~12us (256 * ~50ns SIMD). HNSW overhead
    // (lock, multi-layer descent, beam search) is comparable or higher.
    internal const int SmallDatasetFlatScanThreshold = 256;
    private const int PostFilterSelectivityMultiplier = 2;
    private const int PostFilterMinCandidateThreshold = 8;
    private const int WithinDistanceInitialCandidateCount = 32;
    private const int WithinDistanceMaxCandidateCount = 512;
    private const int WithinDistanceNoGrowthStopRounds = 2;
    private const int WithinDistanceSelectiveAllowListThreshold = 16;
    private const int WithinDistanceFilterMinSeenCandidates = 32;
    private const double WithinDistanceFilterCoverageRatio = 0.20;

    /// <summary>
    /// Execution diagnostics for the most recent
    /// vector query call.
    /// Useful for benchmark instrumentation and planner validation.
    /// </summary>
    public VectorExecutionInfo LastExecutionInfo { get; private set; } = VectorExecutionInfo.None;

    /// <summary>
    /// Attaches an HNSW index for approximate nearest neighbor search.
    /// When set, nearest/radius queries can use ANN widening paths.
    /// Direct index search is used for unfiltered nearest-neighbor lookups;
    /// filtered/radius paths widen ANN candidates and may fall back to exact scan.
    /// </summary>
    /// <param name="index">The HNSW index to use, or null to detach.</param>
    public VectorQuery UseIndex(HnswIndex? index)
    {
        if (index != null)
        {
            if (index.Dimensions != _dimensions)
                throw new ArgumentException(
                    $"Index has {index.Dimensions} dimensions but VectorQuery expects {_dimensions}.",
                    nameof(index));
            if (index.Metric != _metric)
                throw new ArgumentException(
                    $"Index uses {index.Metric} but VectorQuery expects {_metric}.",
                    nameof(index));
        }
        _hnswIndex = index;
        return this;
    }

    // ── Metadata Filtering (pre-search) ─────────────────────────

    /// <summary>
    /// Adds a metadata filter. Applied BEFORE distance computation —
    /// rows that fail this filter are never distance-computed.
    /// This is the "pre-filter" pattern from vector DB literature.
    /// </summary>
    public VectorQuery Where(IFilterStar filter)
    {
        _innerJit.Where(filter);
        return this;
    }

    /// <summary>Clears all metadata filters.</summary>
    public VectorQuery ClearFilters()
    {
        _innerJit.ClearFilters();
        return this;
    }

    // ── Similarity Search ───────────────────────────────────────

    /// <summary>
    /// Returns the K nearest neighbors to the query vector.
    /// Scans all rows (after metadata filter), computes distance, returns top-K.
    /// </summary>
    /// <param name="queryVector">The query vector (must match configured dimensions).</param>
    /// <param name="k">Number of nearest neighbors to return.</param>
    /// <param name="columnNames">Optional list of column names to retrieve as metadata.</param>
    /// <returns>Results ordered by distance (ascending for Cosine/Euclidean, descending for DotProduct).</returns>
    public VectorSearchResult NearestTo(ReadOnlySpan<float> queryVector, int k, params string[] columnNames)
        => NearestTo(queryVector, k, VectorSearchOptions.Default, columnNames);

    /// <summary>
    /// Returns the K nearest neighbors using explicit execution options.
    /// </summary>
    /// <param name="queryVector">The query vector (must match configured dimensions).</param>
    /// <param name="k">Number of nearest neighbors to return.</param>
    /// <param name="options">Execution controls (flat-scan forcing, ef override).</param>
    /// <param name="columnNames">Optional list of column names to retrieve as metadata.</param>
    /// <returns>Results ordered by distance (ascending for Cosine/Euclidean, descending for DotProduct).</returns>
    public VectorSearchResult NearestTo(
        ReadOnlySpan<float> queryVector, int k, VectorSearchOptions options, params string[] columnNames)
    {
        ThrowIfDisposed();
        ValidateDimensions(queryVector);
        EnforceAgentAccess(columnNames);

        long startTimestamp = Stopwatch.GetTimestamp();

        if (options.ForceFlatScan)
        {
            var forcedFlat = FlatScanNearestTo(queryVector, k, columnNames, out int forcedScannedRows);
            LastExecutionInfo = new VectorExecutionInfo(
                Strategy: VectorExecutionStrategy.FlatScan,
                CandidateCount: forcedScannedRows,
                RequestedK: k,
                ReturnedCount: forcedFlat.Count,
                UsedFallbackScan: false,
                ElapsedMs: Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds);
            return forcedFlat;
        }

        // HNSW fast path: when index is attached AND no filters/row evaluator/metadata requested
        // Small datasets fall through to flat scan — graph traversal overhead exceeds brute force.
        bool canUseHnsw = _hnswIndex != null && !_innerJit.HasActiveFilters && !_innerJit.HasRowAccessEvaluator
            && _hnswIndex.Count >= SmallDatasetFlatScanThreshold;

        if (canUseHnsw && columnNames.Length == 0)
        {
            var result = _hnswIndex!.Search(queryVector, k, options.EfSearch);
            LastExecutionInfo = new VectorExecutionInfo(
                Strategy: VectorExecutionStrategy.HnswDirect,
                CandidateCount: _hnswIndex.Count,
                RequestedK: k,
                ReturnedCount: result.Count,
                UsedFallbackScan: false,
                ElapsedMs: Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds);
            return result;
        }

        // HNSW with metadata enrichment: search HNSW then enrich results from table
        if (canUseHnsw && columnNames.Length > 0)
        {
            var hnswResult = _hnswIndex!.Search(queryVector, k, options.EfSearch);
            var enriched = EnrichWithMetadata(hnswResult, columnNames);
            LastExecutionInfo = new VectorExecutionInfo(
                Strategy: VectorExecutionStrategy.HnswMetadataEnrichment,
                CandidateCount: _hnswIndex.Count,
                RequestedK: k,
                ReturnedCount: enriched.Count,
                UsedFallbackScan: false,
                ElapsedMs: Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds);
            return enriched;
        }

        // Filter-aware HNSW path: build filtered allow-list then widen ANN candidates.
        if (_hnswIndex != null && _innerJit.HasActiveFilters && !_innerJit.HasRowAccessEvaluator
            && _hnswIndex.Count >= SmallDatasetFlatScanThreshold)
        {
            var indexed = IndexedPostFilterNearestTo(queryVector, k, options, columnNames);
            StampElapsed(startTimestamp);
            return indexed;
        }

        // Flat scan path (no HNSW index or row evaluator active).
        var flat = FlatScanNearestTo(queryVector, k, columnNames, out int scannedRows);
        LastExecutionInfo = new VectorExecutionInfo(
            Strategy: VectorExecutionStrategy.FlatScan,
            CandidateCount: scannedRows,
            RequestedK: k,
            ReturnedCount: flat.Count,
            UsedFallbackScan: false,
            ElapsedMs: Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds);
        return flat;
    }

    /// <summary>
    /// Returns the K nearest neighbors using ANN candidate generation from HNSW,
    /// then reranks candidates using a custom scorer for exact results.
    /// Falls back to flat scan with scorer when no HNSW index is attached.
    /// </summary>
    /// <param name="queryVector">The query vector (must match configured dimensions).</param>
    /// <param name="k">Number of nearest neighbors to return.</param>
    /// <param name="rerankScorer">Custom scorer applied to ANN candidates. Lower scores are better.</param>
    /// <param name="oversampleFactor">Multiplier for HNSW candidate count (default: 4). More candidates = better recall.</param>
    /// <param name="columnNames">Column names to project for the scorer and returned metadata.</param>
    public VectorSearchResult NearestTo(
        ReadOnlySpan<float> queryVector, int k,
        Func<IRowAccessor, double> rerankScorer,
        int oversampleFactor = 4,
        params string[] columnNames)
    {
        ThrowIfDisposed();
        ValidateDimensions(queryVector);
        EnforceAgentAccess(columnNames);
        ArgumentNullException.ThrowIfNull(rerankScorer);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(k, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(oversampleFactor, 0);

        long startTimestamp = Stopwatch.GetTimestamp();

        // Guard against integer overflow: k * oversampleFactor must fit in int
        long candidateKLong = (long)k * oversampleFactor;
        int candidateK = candidateKLong > int.MaxValue
            ? int.MaxValue
            : (int)candidateKLong;

        if (_hnswIndex == null)
        {
            // No index — flat scan with custom scorer
            var flat = FlatScanWithRerank(k, rerankScorer, columnNames, out int scannedRows);
            LastExecutionInfo = new VectorExecutionInfo(
                Strategy: VectorExecutionStrategy.FlatScan,
                CandidateCount: scannedRows,
                RequestedK: k,
                ReturnedCount: flat.Count,
                UsedFallbackScan: true,
                ElapsedMs: Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds);
            return flat;
        }

        // Phase 1: Get ANN candidates (oversampled)
        var annCandidates = _hnswIndex.Search(queryVector, candidateK);

        // Phase 2: Rerank via custom scorer — seek each candidate and score
        var reranked = new List<(long RowId, double Score, IReadOnlyDictionary<string, object?>? Meta)>(
            annCandidates.Count);

        string[] allColumns = columnNames.Length > 0 ? columnNames : Array.Empty<string>();

        using (var jit = _db!.Jit(_tableName))
        {
            using var reader = jit.Query(allColumns);
            for (int i = 0; i < annCandidates.Count; i++)
            {
                long rowId = annCandidates[i].RowId;
                if (!reader.Seek(rowId))
                    continue;

                double score = rerankScorer(reader);
                IReadOnlyDictionary<string, object?>? meta = null;
                if (columnNames.Length > 0)
                    meta = ExtractMetadataDirectProjection(reader, columnNames);
                reranked.Add((rowId, score, meta));
            }
        }

        // Phase 3: Select top-K from reranked candidates (lower score = better)
        reranked.Sort((a, b) => a.Score.CompareTo(b.Score));
        int resultCount = Math.Min(k, reranked.Count);
        var matches = new List<VectorMatch>(resultCount);
        for (int i = 0; i < resultCount; i++)
        {
            var (rowId, score, meta) = reranked[i];
            matches.Add(new VectorMatch(rowId, (float)score, meta));
        }

        LastExecutionInfo = new VectorExecutionInfo(
            Strategy: VectorExecutionStrategy.HnswReranked,
            CandidateCount: annCandidates.Count,
            RequestedK: k,
            ReturnedCount: matches.Count,
            UsedFallbackScan: false,
            ElapsedMs: Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds);

        return new VectorSearchResult(matches);
    }

    /// <summary>Flat scan reranking fallback when no HNSW index is available.</summary>
    private VectorSearchResult FlatScanWithRerank(
        int k, Func<IRowAccessor, double> scorer, string[] columnNames, out int scannedRows)
    {
        scannedRows = 0;
        var heap = new (long RowId, double Score, IReadOnlyDictionary<string, object?>? Meta)[k];
        int count = 0;

        using var reader = _innerJit.Query(columnNames);
        while (reader.Read())
        {
            scannedRows++;
            double score = scorer(reader);

            if (count < k)
            {
                IReadOnlyDictionary<string, object?>? meta = columnNames.Length > 0
                    ? ExtractMetadataDirectProjection(reader, columnNames) : null;
                heap[count] = (reader.RowId, score, meta);
                count++;
                SiftUpRerank(heap, count - 1);
            }
            else if (score < heap[0].Score)
            {
                IReadOnlyDictionary<string, object?>? meta = columnNames.Length > 0
                    ? ExtractMetadataDirectProjection(reader, columnNames) : null;
                heap[0] = (reader.RowId, score, meta);
                SiftDownRerank(heap, 0, count);
            }
        }

        // Extract sorted ascending by score
        var results = new VectorMatch[count];
        int remaining = count;
        int writeIdx = remaining - 1;
        while (remaining > 0)
        {
            var (rowId, score, meta) = heap[0];
            results[writeIdx--] = new VectorMatch(rowId, (float)score, meta);
            remaining--;
            if (remaining > 0)
            {
                heap[0] = heap[remaining];
                SiftDownRerank(heap, 0, remaining);
            }
        }

        return new VectorSearchResult(new List<VectorMatch>(results));
    }

    private static void SiftUpRerank(
        (long RowId, double Score, IReadOnlyDictionary<string, object?>? Meta)[] heap, int index)
    {
        while (index > 0)
        {
            int parent = (index - 1) / 2;
            if (heap[index].Score > heap[parent].Score)
            {
                (heap[index], heap[parent]) = (heap[parent], heap[index]);
                index = parent;
            }
            else break;
        }
    }

    private static void SiftDownRerank(
        (long RowId, double Score, IReadOnlyDictionary<string, object?>? Meta)[] heap, int index, int count)
    {
        while (true)
        {
            int left = index * 2 + 1;
            int right = left + 1;
            int worst = index;
            if (left < count && heap[left].Score > heap[worst].Score) worst = left;
            if (right < count && heap[right].Score > heap[worst].Score) worst = right;
            if (worst == index) break;
            (heap[index], heap[worst]) = (heap[worst], heap[index]);
            index = worst;
        }
    }

    /// <summary>Flat brute-force scan for NearestTo.</summary>
    private VectorSearchResult FlatScanNearestTo(
        ReadOnlySpan<float> queryVector, int k, string[] columnNames, out int scannedRows)
    {
        scannedRows = 0;

        // Project the vector column plus any requested metadata columns
        string[] projection = new string[1 + columnNames.Length];
        projection[0] = _vectorColumnName;
        columnNames.CopyTo(projection, 1);

        using var reader = _innerJit.Query(projection);

        // Top-K via heap
        var heap = new VectorTopKHeap(k, _metric != DistanceMetric.DotProduct);

        while (reader.Read())
        {
            scannedRows++;

            ReadOnlySpan<float> storedVector = DecodeAndValidateStoredVector(reader);

            float distance = _distanceFn(queryVector, storedVector);
            long rowid = reader.RowId;

            // Only extract metadata when the heap will actually keep this candidate,
            // avoiding a Dictionary allocation for every scanned row.
            if (heap.ShouldInsert(distance))
            {
                IReadOnlyDictionary<string, object?>? metadata = null;
                if (columnNames.Length > 0)
                    metadata = ExtractMetadata(reader, columnNames);
                heap.ForceInsert(rowid, distance, metadata);
            }
        }

        return heap.ToResult();
    }

    private VectorSearchResult IndexedPostFilterNearestTo(
        ReadOnlySpan<float> queryVector, int k, VectorSearchOptions options, string[] columnNames)
    {
        var allowList = BuildFilterAllowList();
        if (allowList.Count == 0)
        {
            LastExecutionInfo = new VectorExecutionInfo(
                Strategy: VectorExecutionStrategy.HnswPostFilterWidening,
                CandidateCount: 0,
                RequestedK: k,
                ReturnedCount: 0,
                UsedFallbackScan: false);
            return new VectorSearchResult(new List<VectorMatch>());
        }

        int selectiveThreshold = Math.Max(k * PostFilterSelectivityMultiplier, PostFilterMinCandidateThreshold);
        if (allowList.Count <= selectiveThreshold)
        {
            var selectiveFlat = FlatScanNearestTo(queryVector, k, columnNames, out int scannedRows);
            LastExecutionInfo = new VectorExecutionInfo(
                Strategy: VectorExecutionStrategy.FlatScan,
                CandidateCount: scannedRows,
                RequestedK: k,
                ReturnedCount: selectiveFlat.Count,
                UsedFallbackScan: false);
            return selectiveFlat;
        }

        var index = _hnswIndex!;
        int targetCount = Math.Min(k, allowList.Count);
        var heap = new VectorTopKHeap(targetCount, isMinHeap: _metric != DistanceMetric.DotProduct);
        var seen = new HashSet<long>();

        int searchK = Math.Min(index.Count, Math.Max(k * 4, 32));
        while (true)
        {
            var candidateBatch = index.Search(queryVector, searchK, options.EfSearch);
            for (int i = 0; i < candidateBatch.Count; i++)
            {
                var match = candidateBatch[i];
                if (!allowList.Contains(match.RowId) || !seen.Add(match.RowId))
                    continue;

                heap.TryInsert(match.RowId, match.Distance);
            }

            if (heap.Count >= targetCount || searchK >= index.Count)
                break;

            int next = Math.Min(index.Count, searchK * 2);
            if (next == searchK)
                break;

            searchK = next;
        }

        if (heap.Count < targetCount)
        {
            var fallback = FlatScanNearestTo(queryVector, k, columnNames, out int scannedRows);
            LastExecutionInfo = new VectorExecutionInfo(
                Strategy: VectorExecutionStrategy.HnswPostFilterWidening,
                CandidateCount: scannedRows,
                RequestedK: k,
                ReturnedCount: fallback.Count,
                UsedFallbackScan: true);
            return fallback;
        }

        var filtered = heap.ToResult();
        if (columnNames.Length > 0)
            filtered = EnrichWithMetadata(filtered, columnNames);

        LastExecutionInfo = new VectorExecutionInfo(
            Strategy: VectorExecutionStrategy.HnswPostFilterWidening,
            CandidateCount: allowList.Count,
            RequestedK: k,
            ReturnedCount: filtered.Count,
            UsedFallbackScan: false);
        return filtered;
    }

    private HashSet<long> BuildFilterAllowList()
    {
        string probeColumn = GetFilterProbeColumn();
        using var reader = _innerJit.Query(probeColumn);
        var allowed = new HashSet<long>();
        while (reader.Read())
            allowed.Add(reader.RowId);
        return allowed;
    }

    private string GetFilterProbeColumn()
    {
        var table = _innerJit.Table;
        if (table is null || table.Columns.Count == 0)
            return _vectorColumnName;

        int rowIdAliasOrdinal = _innerJit.RowidAliasOrdinal;
        if (rowIdAliasOrdinal >= 0 && rowIdAliasOrdinal < table.Columns.Count)
            return table.Columns[rowIdAliasOrdinal].Name;

        return table.Columns[0].Name;
    }

    /// <summary>Enriches HNSW results with metadata columns from the source table.</summary>
    private VectorSearchResult EnrichWithMetadata(VectorSearchResult hnswResult, string[] columnNames)
    {
        var enriched = new List<VectorMatch>(hnswResult.Count);
        var db = _db!;
        var table = _innerJit.Table!;

        // Seek each result row and extract metadata
        using var reader = db.CreateReader(table.Name, columnNames);
        for (int i = 0; i < hnswResult.Count; i++)
        {
            var match = hnswResult[i];
            if (reader.Seek(match.RowId))
            {
                var metadata = ExtractMetadataFromSeek(reader, columnNames);
                enriched.Add(new VectorMatch(match.RowId, match.Distance, metadata));
            }
            else
            {
                enriched.Add(match); // row may have been deleted; keep result without metadata
            }
        }

        return new VectorSearchResult(enriched);
    }

    private static Dictionary<string, object?> ExtractMetadataFromSeek(SharcDataReader reader, string[] columnNames)
    {
        var dict = new Dictionary<string, object?>(columnNames.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < columnNames.Length; i++)
            dict[columnNames[i]] = reader.GetValue(i);
        return dict;
    }

    /// <summary>
    /// Returns all rows within the specified distance threshold.
    /// </summary>
    /// <param name="queryVector">The query vector.</param>
    /// <param name="maxDistance">
    /// Maximum distance threshold. For DotProduct, this is a minimum similarity threshold.
    /// </param>
    /// <param name="columnNames">Optional list of column names to retrieve as metadata.</param>
    public VectorSearchResult WithinDistance(ReadOnlySpan<float> queryVector, float maxDistance, params string[] columnNames)
        => WithinDistance(queryVector, maxDistance, VectorSearchOptions.Default, columnNames);

    /// <summary>
    /// Returns all rows within the specified distance threshold using explicit execution options.
    /// </summary>
    /// <param name="queryVector">The query vector.</param>
    /// <param name="maxDistance">
    /// Maximum distance threshold. For DotProduct, this is a minimum similarity threshold.
    /// </param>
    /// <param name="options">Execution controls (flat-scan forcing, ef override).</param>
    /// <param name="columnNames">Optional list of column names to retrieve as metadata.</param>
    public VectorSearchResult WithinDistance(
        ReadOnlySpan<float> queryVector, float maxDistance, VectorSearchOptions options, params string[] columnNames)
    {
        ThrowIfDisposed();
        ValidateDimensions(queryVector);
        EnforceAgentAccess(columnNames);

        long startTimestamp = Stopwatch.GetTimestamp();

        if (options.ForceFlatScan)
        {
            var forcedFlat = FlatScanWithinDistance(queryVector, maxDistance, columnNames, out int forcedScannedRows);
            LastExecutionInfo = new VectorExecutionInfo(
                Strategy: VectorExecutionStrategy.FlatScan,
                CandidateCount: forcedScannedRows,
                RequestedK: 0,
                ReturnedCount: forcedFlat.Count,
                UsedFallbackScan: false,
                ElapsedMs: Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds);
            return forcedFlat;
        }

        if (_hnswIndex != null && !_innerJit.HasRowAccessEvaluator
            && _hnswIndex.Count >= SmallDatasetFlatScanThreshold)
        {
            var indexed = IndexedWithinDistance(queryVector, maxDistance, options, columnNames);
            StampElapsed(startTimestamp);
            return indexed;
        }

        var flat = FlatScanWithinDistance(queryVector, maxDistance, columnNames, out int scannedRows);
        LastExecutionInfo = new VectorExecutionInfo(
            Strategy: VectorExecutionStrategy.FlatScan,
            CandidateCount: scannedRows,
            RequestedK: 0,
            ReturnedCount: flat.Count,
            UsedFallbackScan: false,
            ElapsedMs: Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds);
        return flat;
    }

    /// <summary>Flat brute-force scan for WithinDistance.</summary>
    private VectorSearchResult FlatScanWithinDistance(
        ReadOnlySpan<float> queryVector, float maxDistance, string[] columnNames, out int scannedRows)
    {
        scannedRows = 0;

        string[] projection = new string[1 + columnNames.Length];
        projection[0] = _vectorColumnName;
        columnNames.CopyTo(projection, 1);

        using var reader = _innerJit.Query(projection);

        var results = new List<VectorMatch>();

        while (reader.Read())
        {
            scannedRows++;

            ReadOnlySpan<float> storedVector = DecodeAndValidateStoredVector(reader);

            float distance = _distanceFn(queryVector, storedVector);
            if (IsWithinThreshold(distance, maxDistance))
            {
                IReadOnlyDictionary<string, object?>? metadata = null;
                if (columnNames.Length > 0)
                    metadata = ExtractMetadata(reader, columnNames);

                results.Add(new VectorMatch(reader.RowId, distance, metadata));
            }
        }

        SortMatchesByMetric(results);
        return new VectorSearchResult(results);
    }

    private VectorSearchResult IndexedWithinDistance(
        ReadOnlySpan<float> queryVector, float maxDistance, VectorSearchOptions options, string[] columnNames)
    {
        var index = _hnswIndex!;
        HashSet<long>? allowList = null;
        int candidateUniverse = index.Count;

        if (_innerJit.HasActiveFilters)
        {
            allowList = BuildFilterAllowList();
            candidateUniverse = allowList.Count;

            if (candidateUniverse == 0)
            {
                LastExecutionInfo = new VectorExecutionInfo(
                    Strategy: VectorExecutionStrategy.HnswWithinDistanceWidening,
                    CandidateCount: 0,
                    RequestedK: 0,
                    ReturnedCount: 0,
                    UsedFallbackScan: false);
                return new VectorSearchResult(new List<VectorMatch>());
            }

            if (candidateUniverse <= WithinDistanceSelectiveAllowListThreshold)
            {
                var selectiveFlat = FlatScanWithinDistance(queryVector, maxDistance, columnNames, out int scannedRows);
                LastExecutionInfo = new VectorExecutionInfo(
                    Strategy: VectorExecutionStrategy.FlatScan,
                    CandidateCount: scannedRows,
                    RequestedK: 0,
                    ReturnedCount: selectiveFlat.Count,
                    UsedFallbackScan: false);
                return selectiveFlat;
            }
        }

        var annMatches = new List<VectorMatch>();
        var acceptedIds = new HashSet<long>();
        var seenCandidateIds = new HashSet<long>();
        int eligibleSeenCount = 0;
        int roundsWithoutGrowth = 0;
        bool observedOutsideThreshold = false;

        int searchK = Math.Min(index.Count, WithinDistanceInitialCandidateCount);
        int maxSearchK = Math.Min(index.Count, WithinDistanceMaxCandidateCount);

        while (searchK > 0)
        {
            var candidateBatch = index.Search(queryVector, searchK, options.EfSearch);
            int acceptedBefore = annMatches.Count;

            for (int i = 0; i < candidateBatch.Count; i++)
            {
                var match = candidateBatch[i];
                if (!seenCandidateIds.Add(match.RowId))
                    continue;

                if (allowList != null && !allowList.Contains(match.RowId))
                    continue;

                eligibleSeenCount++;

                if (IsOutsideThreshold(match.Distance, maxDistance))
                    observedOutsideThreshold = true;

                if (IsWithinThreshold(match.Distance, maxDistance) && acceptedIds.Add(match.RowId))
                    annMatches.Add(match);
            }

            if (annMatches.Count == acceptedBefore)
                roundsWithoutGrowth++;
            else
                roundsWithoutGrowth = 0;

            bool filterCoverageReached = allowList == null
                || eligibleSeenCount >= GetWithinDistanceFilterCoverageTarget(allowList.Count);

            if (observedOutsideThreshold && filterCoverageReached && roundsWithoutGrowth >= WithinDistanceNoGrowthStopRounds)
                break;

            if (searchK >= maxSearchK || searchK >= index.Count)
                break;

            int next = Math.Min(Math.Min(index.Count, maxSearchK), searchK * 2);
            if (next <= searchK)
                break;
            searchK = next;
        }

        bool shouldFallback = !observedOutsideThreshold
            || (allowList != null && eligibleSeenCount < GetWithinDistanceFilterCoverageTarget(allowList.Count));

        if (shouldFallback)
        {
            var fallback = FlatScanWithinDistance(queryVector, maxDistance, columnNames, out int scannedRows);
            LastExecutionInfo = new VectorExecutionInfo(
                Strategy: VectorExecutionStrategy.HnswWithinDistanceWidening,
                CandidateCount: scannedRows,
                RequestedK: 0,
                ReturnedCount: fallback.Count,
                UsedFallbackScan: true);
            return fallback;
        }

        SortMatchesByMetric(annMatches);
        var indexed = new VectorSearchResult(annMatches);
        if (columnNames.Length > 0)
            indexed = EnrichWithMetadata(indexed, columnNames);

        LastExecutionInfo = new VectorExecutionInfo(
            Strategy: VectorExecutionStrategy.HnswWithinDistanceWidening,
            CandidateCount: Math.Min(candidateUniverse, eligibleSeenCount),
            RequestedK: 0,
            ReturnedCount: indexed.Count,
            UsedFallbackScan: false);
        return indexed;
    }

    private static int GetWithinDistanceFilterCoverageTarget(int allowListCount)
    {
        if (allowListCount <= 0)
            return 0;

        int ratioTarget = (int)Math.Ceiling(allowListCount * WithinDistanceFilterCoverageRatio);
        return Math.Min(allowListCount, Math.Max(WithinDistanceFilterMinSeenCandidates, ratioTarget));
    }

    private bool IsWithinThreshold(float distance, float maxDistance)
    {
        // DotProduct uses similarity (higher is better); distance metrics use lower-is-better.
        return _metric == DistanceMetric.DotProduct
            ? distance >= maxDistance
            : distance <= maxDistance;
    }

    private bool IsOutsideThreshold(float distance, float maxDistance)
    {
        return _metric == DistanceMetric.DotProduct
            ? distance < maxDistance
            : distance > maxDistance;
    }

    private void SortMatchesByMetric(List<VectorMatch> results)
    {
        results.Sort((a, b) => _metric == DistanceMetric.DotProduct
            ? b.Distance.CompareTo(a.Distance)
            : a.Distance.CompareTo(b.Distance));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _innerJit.Dispose();
        _db = null;
    }

    /// <summary>
    /// Updates the elapsed time on the already-assigned <see cref="LastExecutionInfo"/>.
    /// Used when the info was set by a sub-method (e.g. IndexedPostFilter paths).
    /// </summary>
    private void StampElapsed(long startTimestamp)
    {
        var info = LastExecutionInfo;
        LastExecutionInfo = info with
        {
            ElapsedMs = Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds
        };
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_db is null, this);
    }

    private void ValidateDimensions(ReadOnlySpan<float> queryVector)
    {
        if (queryVector.Length != _dimensions)
            throw new ArgumentException(
                $"Query vector has {queryVector.Length} dimensions but the index expects {_dimensions}.");
    }

    private void EnforceAgentAccess(string[] columnNames)
    {
        if (_agent == null) return;
        // Build full column list: vector column + requested metadata columns
        var allColumns = new string[1 + columnNames.Length];
        allColumns[0] = _vectorColumnName;
        columnNames.CopyTo(allColumns, 1);
        EntitlementEnforcer.Enforce(_agent, _innerJit.Table!.Name, allColumns);
    }

    private static Dictionary<string, object?> ExtractMetadata(SharcDataReader reader, string[] columnNames)
    {
        var dict = new Dictionary<string, object?>(columnNames.Length, StringComparer.OrdinalIgnoreCase);
        // Metadata columns start at index 1 because index 0 is always the vector column.
        for (int i = 0; i < columnNames.Length; i++)
        {
            dict[columnNames[i]] = reader.GetValue(i + 1);
        }
        return dict;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<float> DecodeAndValidateStoredVector(SharcDataReader reader)
    {
        ReadOnlySpan<byte> blob = reader.GetBlobSpan(0);
        if (!BlobVectorCodec.TryDecode(blob, out ReadOnlySpan<float> vector))
            ThrowInvalidVectorPayload(reader.RowId, blob.Length);

        if (vector.Length != _dimensions)
            ThrowVectorDimensionMismatch(reader.RowId, vector.Length);

        return vector;
    }

    private void ThrowInvalidVectorPayload(long rowId, int byteLength) =>
        throw new InvalidOperationException(
            $"Row {rowId} in table '{_innerJit.Table!.Name}' has an invalid vector payload " +
            $"length ({byteLength} bytes).");

    private void ThrowVectorDimensionMismatch(long rowId, int foundDimensions) =>
        throw new InvalidOperationException(
            $"Row {rowId} in table '{_innerJit.Table!.Name}' has {foundDimensions} dimensions " +
            $"but this query expects {_dimensions}.");

    /// <summary>
    /// Extracts metadata from a reader where all projected columns are metadata
    /// (no vector column at index 0). Used by the rerank flat-scan path.
    /// </summary>
    private static Dictionary<string, object?> ExtractMetadataDirectProjection(
        SharcDataReader reader, string[] columnNames)
    {
        var dict = new Dictionary<string, object?>(columnNames.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < columnNames.Length; i++)
        {
            dict[columnNames[i]] = reader.GetValue(i);
        }
        return dict;
    }
}
