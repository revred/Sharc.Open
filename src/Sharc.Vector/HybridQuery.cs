// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Runtime.CompilerServices;
using Sharc.Core.Trust;
using Sharc.Trust;
using Sharc.Vector.Hnsw;

namespace Sharc.Vector;

/// <summary>
/// A pre-compiled hybrid search handle combining vector similarity and text relevance
/// with Reciprocal Rank Fusion (RRF). Pre-resolves table schema, vector/text column ordinals,
/// distance metric, and dimensions at creation time.
/// </summary>
/// <remarks>
/// <para>Created via <see cref="SharcDatabaseExtensions.Hybrid"/>.
/// Follows the same lifecycle as <see cref="VectorQuery"/>: create once, search many, dispose.</para>
/// <para>Internally performs two independent scans (vector distance + text TF scoring),
/// then fuses results via RRF for a unified ranking.</para>
/// <para>This type is <b>not thread-safe</b>.</para>
/// </remarks>
public sealed class HybridQuery : IDisposable
{
    private SharcDatabase? _db;
    private readonly JitQuery _vectorJit;
    private readonly JitQuery _textJit;
    private readonly string _vectorColumnName;
    private readonly string _textColumnName;
    private readonly int _dimensions;
    private readonly DistanceMetric _metric;
    private readonly VectorDistanceFunction _distanceFn;

    internal HybridQuery(
        SharcDatabase db,
        JitQuery vectorJit,
        JitQuery textJit,
        string vectorColumnName,
        string textColumnName,
        int dimensions,
        DistanceMetric metric)
    {
        _db = db;
        _vectorJit = vectorJit;
        _textJit = textJit;
        _vectorColumnName = vectorColumnName;
        _textColumnName = textColumnName;
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
    public HybridQuery WithAgent(AgentInfo? agent)
    {
        _agent = agent;
        return this;
    }

    /// <summary>
    /// Sets a row-level access evaluator for multi-tenant isolation.
    /// Rows that fail the evaluator are silently skipped during both
    /// vector and text scans.
    /// </summary>
    /// <param name="evaluator">The evaluator, or null to clear.</param>
    public HybridQuery WithRowEvaluator(IRowAccessEvaluator? evaluator)
    {
        _vectorJit.WithRowAccess(evaluator);
        _textJit.WithRowAccess(evaluator);
        return this;
    }

    private AgentInfo? _agent;

    // ── HNSW Index ────────────────────────────────────────────────

    private HnswIndex? _hnswIndex;

    /// <summary>
    /// Attaches an HNSW index for the vector leg of hybrid search.
    /// When set and the dataset exceeds the small-N threshold, the vector
    /// ranking phase uses ANN search instead of flat scan, then fuses with
    /// text results via RRF as before.
    /// </summary>
    /// <param name="index">The HNSW index to use, or null to detach.</param>
    public HybridQuery UseIndex(HnswIndex? index)
    {
        if (index != null)
        {
            if (index.Dimensions != _dimensions)
                throw new ArgumentException(
                    $"Index has {index.Dimensions} dimensions but HybridQuery expects {_dimensions}.",
                    nameof(index));
            if (index.Metric != _metric)
                throw new ArgumentException(
                    $"Index uses {index.Metric} but HybridQuery expects {_metric}.",
                    nameof(index));
        }
        _hnswIndex = index;
        return this;
    }

    // ── Metadata Filtering (pre-search) ─────────────────────────

    /// <summary>
    /// Adds a metadata filter applied BEFORE both vector and text searches.
    /// Rows that fail this filter are excluded from both search paths.
    /// </summary>
    public HybridQuery Where(IFilterStar filter)
    {
        _vectorJit.Where(filter);
        _textJit.Where(filter);
        return this;
    }

    /// <summary>Clears all metadata filters from both search paths.</summary>
    public HybridQuery ClearFilters()
    {
        _vectorJit.ClearFilters();
        _textJit.ClearFilters();
        return this;
    }

    // ── Hybrid Search ───────────────────────────────────────────

    /// <summary>
    /// Performs a hybrid search combining vector similarity and text keyword relevance.
    /// </summary>
    /// <param name="queryVector">Query vector for similarity search (must match configured dimensions).</param>
    /// <param name="queryText">Query text for keyword matching (whitespace-separated terms).</param>
    /// <param name="k">Number of top results to return after fusion.</param>
    /// <param name="columnNames">Optional metadata column names to include in results.</param>
    /// <returns>Results ordered by fused RRF score (descending).</returns>
    /// <exception cref="ArgumentException">If query vector dimensions don't match or query text is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">If k is less than 1.</exception>
    public HybridSearchResult Search(
        ReadOnlySpan<float> queryVector,
        string queryText,
        int k,
        params string[] columnNames)
    {
        ThrowIfDisposed();
        ValidateDimensions(queryVector);
        ArgumentException.ThrowIfNullOrEmpty(queryText);
        ArgumentOutOfRangeException.ThrowIfLessThan(k, 1);
        EnforceAgentAccess(columnNames);

        byte[][] queryTermsUtf8 = TextScorer.TokenizeQuery(queryText);
        int poolSize = k * 3;

        // ── Step 1: Vector ranking (no metadata — deferred to post-fusion) ──
        var vectorRanks = new Dictionary<long, int>();

        bool useHnswForVector = _hnswIndex != null
            && !_vectorJit.HasActiveFilters
            && !_vectorJit.HasRowAccessEvaluator
            && _hnswIndex.Count >= VectorQuery.SmallDatasetFlatScanThreshold;

        if (useHnswForVector)
        {
            // HNSW fast path: ANN search for vector rankings
            var hnswResult = _hnswIndex!.Search(queryVector, poolSize);
            for (int i = 0; i < hnswResult.Count; i++)
                vectorRanks[hnswResult[i].RowId] = i + 1;
        }
        else
        {
            // Flat scan path: project only the vector column (skip metadata decode)
            var heap = new VectorTopKHeap(poolSize, _metric != DistanceMetric.DotProduct);

            using var reader = _vectorJit.Query(_vectorColumnName);
            while (reader.Read())
            {
                if (!TryDecodeHybridVector(reader, out ReadOnlySpan<float> storedVector))
                    continue;

                float distance = _distanceFn(queryVector, storedVector);

                if (heap.ShouldInsert(distance))
                    heap.ForceInsert(reader.RowId, distance);
            }

            var vectorResult = heap.ToResult();
            for (int i = 0; i < vectorResult.Count; i++)
                vectorRanks[vectorResult[i].RowId] = i + 1;
        }

        // ── Step 2: Text ranking (no metadata — deferred to post-fusion) ──
        var textRanks = new Dictionary<long, int>();

        if (queryTermsUtf8.Length > 0)
        {
            // Bounded heap: keep top poolSize text results without full-list materialization.
            // Min-heap by TF score: root = worst retained score, evict when new score is better.
            var heap = new TextTopKHeap(poolSize);

            using var reader = _textJit.Query(_textColumnName);
            while (reader.Read())
            {
                ReadOnlySpan<byte> textBytes = reader.GetUtf8Span(0);
                float tfScore = TextScorer.Score(textBytes, queryTermsUtf8);

                if (tfScore > 0 && heap.ShouldInsert(tfScore))
                    heap.ForceInsert(reader.RowId, tfScore, null);
            }

            var topText = heap.ToSortedDescending();
            for (int i = 0; i < topText.Count; i++)
                textRanks[topText[i].RowId] = i + 1;
        }

        // ── Step 3: Fuse ─────────────────────────────────────
        var fused = RankFusion.Fuse(vectorRanks, textRanks, k);

        // ── Step 4: Materialize metadata only for final winners ──
        Dictionary<long, IReadOnlyDictionary<string, object?>>? metadataLookup = null;
        if (columnNames.Length > 0 && fused.Count > 0)
        {
            metadataLookup = new Dictionary<long, IReadOnlyDictionary<string, object?>>(fused.Count);
            var table = _vectorJit.Table!;

            using var seekReader = _db!.CreateReader(table.Name, columnNames);
            for (int i = 0; i < fused.Count; i++)
            {
                long rowId = fused[i].RowId;
                if (seekReader.Seek(rowId))
                {
                    var dict = new Dictionary<string, object?>(columnNames.Length, StringComparer.OrdinalIgnoreCase);
                    for (int c = 0; c < columnNames.Length; c++)
                        dict[columnNames[c]] = seekReader.GetValue(c);
                    metadataLookup[rowId] = dict;
                }
            }
        }

        // ── Step 5: Build results ────────────────────────────
        var matches = new List<HybridMatch>(fused.Count);
        foreach (var (rowId, score, vr, tr) in fused)
        {
            IReadOnlyDictionary<string, object?>? metadata = null;
            metadataLookup?.TryGetValue(rowId, out metadata);
            matches.Add(new HybridMatch(
                rowId,
                score,
                VectorRank: vr == RankFusion.UnrankedSentinel ? 0 : vr,
                TextRank: tr == RankFusion.UnrankedSentinel ? 0 : tr,
                metadata));
        }

        return new HybridSearchResult(matches);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _vectorJit.Dispose();
        _textJit.Dispose();
        _db = null;
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
        // Enforce access to vector column, text column, and all requested metadata columns
        var allColumns = new string[2 + columnNames.Length];
        allColumns[0] = _vectorColumnName;
        allColumns[1] = _textColumnName;
        columnNames.CopyTo(allColumns, 2);
        EntitlementEnforcer.Enforce(_agent, _vectorJit.Table!.Name, allColumns);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryDecodeHybridVector(SharcDataReader reader, out ReadOnlySpan<float> vector)
    {
        ReadOnlySpan<byte> blob = reader.GetBlobSpan(0);
        if (blob.IsEmpty)
        {
            vector = default;
            return false; // Preserve existing behavior: skip NULL/empty vectors.
        }

        if (!BlobVectorCodec.TryDecode(blob, out vector))
            ThrowInvalidVectorPayload(reader.RowId, blob.Length);

        if (vector.Length != _dimensions)
            ThrowVectorDimensionMismatch(reader.RowId, vector.Length);

        return true;
    }

    private void ThrowInvalidVectorPayload(long rowId, int byteLength) =>
        throw new InvalidOperationException(
            $"Row {rowId} in table '{_vectorJit.Table!.Name}' has an invalid vector payload " +
            $"length ({byteLength} bytes).");

    private void ThrowVectorDimensionMismatch(long rowId, int foundDimensions) =>
        throw new InvalidOperationException(
            $"Row {rowId} in table '{_vectorJit.Table!.Name}' has {foundDimensions} dimensions " +
            $"but this query expects {_dimensions}.");

    /// <summary>
    /// Fixed-capacity min-heap for top-K text scoring. Root = lowest (worst) retained
    /// TF score. New candidates replace root when their score exceeds it.
    /// </summary>
    private sealed class TextTopKHeap
    {
        private readonly (long RowId, float TfScore, IReadOnlyDictionary<string, object?>? Metadata)[] _heap;
        private readonly int _capacity;
        private int _count;

        internal TextTopKHeap(int k)
        {
            _capacity = k;
            _heap = new (long, float, IReadOnlyDictionary<string, object?>?)[k];
        }

        internal bool ShouldInsert(float tfScore)
        {
            if (_count < _capacity) return true;
            return tfScore > _heap[0].TfScore;
        }

        internal void ForceInsert(long rowId, float tfScore, IReadOnlyDictionary<string, object?>? metadata)
        {
            if (_count < _capacity)
            {
                _heap[_count] = (rowId, tfScore, metadata);
                _count++;
                if (_count == _capacity) BuildHeap();
            }
            else
            {
                _heap[0] = (rowId, tfScore, metadata);
                SiftDown(0);
            }
        }

        internal List<(long RowId, float TfScore, IReadOnlyDictionary<string, object?>? Metadata)> ToSortedDescending()
        {
            var list = new List<(long RowId, float TfScore, IReadOnlyDictionary<string, object?>? Metadata)>(_count);
            for (int i = 0; i < _count; i++)
                list.Add(_heap[i]);
            list.Sort((a, b) => b.TfScore.CompareTo(a.TfScore));
            return list;
        }

        private void BuildHeap()
        {
            for (int i = _count / 2 - 1; i >= 0; i--)
                SiftDown(i);
        }

        private void SiftDown(int i)
        {
            while (true)
            {
                int smallest = i;
                int left = 2 * i + 1;
                int right = 2 * i + 2;
                // Min-heap: root = smallest TF score (worst match to evict)
                if (left < _count && _heap[left].TfScore < _heap[smallest].TfScore) smallest = left;
                if (right < _count && _heap[right].TfScore < _heap[smallest].TfScore) smallest = right;
                if (smallest == i) break;
                (_heap[i], _heap[smallest]) = (_heap[smallest], _heap[i]);
                i = smallest;
            }
        }
    }
}
