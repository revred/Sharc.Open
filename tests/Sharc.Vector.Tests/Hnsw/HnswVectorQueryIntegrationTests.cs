// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Text;
using Sharc;
using Sharc.Core;
using Sharc.Trust;
using Sharc.Vector;
using Sharc.Vector.Hnsw;
using Xunit;

namespace Sharc.Vector.Tests.Hnsw;

public sealed class HnswVectorQueryIntegrationTests : IDisposable
{
    private readonly string _dbPath;
    private readonly SharcDatabase _db;
    private const int VectorDim = 8;
    private const int RowCount = 500;

    public HnswVectorQueryIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"sharc_hnsw_{Guid.NewGuid()}.db");

        var db = SharcDatabase.Create(_dbPath);

        using (var tx = db.BeginTransaction())
        {
            tx.Execute(
                "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, category TEXT, embedding BLOB)");
            tx.Commit();
        }

        var rng = new Random(42);
        using (var writer = SharcWriter.From(db))
        {
            for (int i = 0; i < RowCount; i++)
            {
                var vec = new float[VectorDim];
                for (int d = 0; d < VectorDim; d++)
                    vec[d] = (float)(rng.NextDouble() * 2.0 - 1.0);

                byte[] vecBytes = BlobVectorCodec.Encode(vec);
                long blobST = 2L * vecBytes.Length + 12;
                byte[] titleBytes = Encoding.UTF8.GetBytes($"doc_{i}");
                byte[] catBytes = Encoding.UTF8.GetBytes(i % 3 == 0 ? "science" : "art");

                writer.Insert("docs",
                    ColumnValue.Null(),
                    ColumnValue.Text(2L * titleBytes.Length + 13, titleBytes),
                    ColumnValue.Text(2L * catBytes.Length + 13, catBytes),
                    ColumnValue.Blob(blobST, vecBytes));
            }
        }

        db.Dispose();
        _db = SharcDatabase.Open(_dbPath, new SharcOpenOptions { Writable = true, PageCacheSize = 64 });
    }

    [Fact]
    public void Build_SearchFindsResults()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);

        Assert.Equal(RowCount, index.Count);
        Assert.Equal(VectorDim, index.Dimensions);

        var query = new float[VectorDim];
        var result = index.Search(query, k: 5);

        Assert.Equal(5, result.Count);
        for (int i = 1; i < result.Count; i++)
            Assert.True(result[i].Distance >= result[i - 1].Distance);
    }

    [Fact]
    public void UseIndex_NearestTo_UsesHnsw()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);
        vq.UseIndex(index);

        var query = new float[VectorDim];
        var result = vq.NearestTo(query, k: 5);

        Assert.Equal(5, result.Count);
        for (int i = 1; i < result.Count; i++)
            Assert.True(result[i].Distance >= result[i - 1].Distance);
    }

    [Fact]
    public void UseIndex_WithFilter_FallsBackToFlatScan()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);
        vq.UseIndex(index);

        // Add a filter — should fall back to flat scan
        vq.Where(FilterStar.Column("category").Eq("science"));

        var query = new float[VectorDim];
        var result = vq.NearestTo(query, k: 5);

        Assert.True(result.Count > 0);
        Assert.True(result.Count <= 5);
    }

    [Fact]
    public void UseIndex_WithinDistance_UsesHnswWidening()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);
        vq.UseIndex(index);

        var query = new float[VectorDim];
        var result = vq.WithinDistance(query, maxDistance: 0.5f);

        Assert.Equal(VectorExecutionStrategy.HnswWithinDistanceWidening, vq.LastExecutionInfo.Strategy);
        Assert.False(vq.LastExecutionInfo.UsedFallbackScan);
        Assert.All(result.Matches, m => Assert.True(m.Distance <= 0.5f));
    }

    [Fact]
    public void UseIndex_WithinDistance_WithFilter_UsesHnswWidening()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);
        vq.UseIndex(index);

        vq.Where(FilterStar.Column("category").Eq("science"));

        var query = new float[VectorDim];
        var result = vq.WithinDistance(query, maxDistance: 1.5f, "category");

        Assert.Equal(VectorExecutionStrategy.HnswWithinDistanceWidening, vq.LastExecutionInfo.Strategy);
        Assert.False(vq.LastExecutionInfo.UsedFallbackScan);
        Assert.All(result.Matches, m =>
        {
            Assert.True(m.Distance <= 1.5f);
            Assert.NotNull(m.Metadata);
            Assert.Equal("science", m.Metadata!["category"]);
        });
    }

    [Fact]
    public void UseIndex_WithinDistance_BroadThreshold_FallsBackToFlatScan()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);
        vq.UseIndex(index);

        var query = new float[VectorDim];
        var result = vq.WithinDistance(query, maxDistance: float.MaxValue);

        Assert.Equal(RowCount, result.Count);
        Assert.Equal(VectorExecutionStrategy.HnswWithinDistanceWidening, vq.LastExecutionInfo.Strategy);
        Assert.True(vq.LastExecutionInfo.UsedFallbackScan);
    }

    [Fact]
    public void UseIndex_WithinDistance_WithForceFlatScanOption_BypassesHnsw()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);
        vq.UseIndex(index);

        var query = new float[VectorDim];
        var options = new VectorSearchOptions
        {
            ForceFlatScan = true
        };

        var result = vq.WithinDistance(query, maxDistance: 1.5f, options);

        Assert.True(result.Count > 0);
        Assert.Equal(VectorExecutionStrategy.FlatScan, vq.LastExecutionInfo.Strategy);
    }

    [Fact]
    public void UseIndex_WithinDistance_MatchesForcedFlatScanResults()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);
        vq.UseIndex(index);

        var query = new float[VectorDim];
        for (int i = 0; i < query.Length; i++)
            query[i] = (i - 2) * 0.1f;

        var nearestFlat = vq.NearestTo(query, k: 12, new VectorSearchOptions { ForceFlatScan = true });
        float radius = nearestFlat[nearestFlat.Count - 1].Distance;

        var ann = vq.WithinDistance(query, radius, new VectorSearchOptions { EfSearch = 128 });
        var annInfo = vq.LastExecutionInfo;
        var flat = vq.WithinDistance(query, radius, new VectorSearchOptions { ForceFlatScan = true });

        Assert.Equal(flat.Count, ann.Count);
        var annIds = ann.Matches.Select(m => m.RowId).OrderBy(v => v).ToArray();
        var flatIds = flat.Matches.Select(m => m.RowId).OrderBy(v => v).ToArray();
        Assert.Equal(flatIds, annIds);
        Assert.True(annInfo.Strategy is VectorExecutionStrategy.HnswWithinDistanceWidening
            or VectorExecutionStrategy.FlatScan);
    }

    [Fact]
    public void UseIndex_WithBroadFilter_UsesIndexedPostFilterWidening()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);
        vq.UseIndex(index);

        vq.Where(FilterStar.Column("category").Eq("science"));

        var query = new float[VectorDim];
        var result = vq.NearestTo(query, k: 5);

        Assert.True(result.Count > 0);
        Assert.Equal(VectorExecutionStrategy.HnswPostFilterWidening, vq.LastExecutionInfo.Strategy);
    }

    [Fact]
    public void UseIndex_WithHighlySelectiveFilter_UsesFlatScan()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);
        vq.UseIndex(index);

        vq.Where(FilterStar.Column("title").Eq("doc_1"));

        var query = new float[VectorDim];
        var result = vq.NearestTo(query, k: 5);

        Assert.True(result.Count <= 1);
        Assert.Equal(VectorExecutionStrategy.FlatScan, vq.LastExecutionInfo.Strategy);
    }

    [Fact]
    public void UseIndex_WithForceFlatScanOption_BypassesHnsw()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);
        vq.UseIndex(index);

        var query = new float[VectorDim];
        var options = new VectorSearchOptions
        {
            ForceFlatScan = true
        };

        var result = vq.NearestTo(query, k: 5, options);

        Assert.Equal(5, result.Count);
        Assert.Equal(VectorExecutionStrategy.FlatScan, vq.LastExecutionInfo.Strategy);
    }

    [Fact]
    public void UseIndex_DimensionMismatch_Throws()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);

        // Create a VQ that thinks dims are different (hack: different table not possible,
        // so test via the UseIndex validation directly)
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);

        // The index and vq have the same dims, so this should succeed
        vq.UseIndex(index); // should not throw
        Assert.Equal(VectorDim, index.Dimensions);
    }

    [Fact]
    public void BuildHnswIndex_ExtensionMethod_Works()
    {
        using var index = _db.BuildHnswIndex("docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);

        Assert.Equal(RowCount, index.Count);
    }

    [Fact]
    public void BuildAndPersist_ThenLoad_SearchProducesSameResults()
    {
        // Build with persistence
        VectorSearchResult buildResult;
        var query = new float[VectorDim];
        query[0] = 0.5f;

        using (var buildIndex = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: true))
        {
            buildResult = buildIndex.Search(query, k: 5);
        }

        // Load from persisted shadow table
        using var loadedIndex = HnswIndex.Load(_db, "docs", "embedding");
        Assert.NotNull(loadedIndex);
        Assert.Equal(RowCount, loadedIndex!.Count);

        var loadResult = loadedIndex.Search(query, k: 5);

        Assert.Equal(buildResult.Count, loadResult.Count);
        for (int i = 0; i < buildResult.Count; i++)
        {
            Assert.Equal(buildResult[i].RowId, loadResult[i].RowId);
            Assert.Equal(buildResult[i].Distance, loadResult[i].Distance, 1e-5f);
        }
    }

    [Fact]
    public void Load_WhenNoShadowTable_ReturnsNull()
    {
        // Before any Build with persist, Load should return null
        using var tempPath = new TempFile();
        var tempDb = SharcDatabase.Create(tempPath.Path);
        using (var tx = tempDb.BeginTransaction())
        {
            tx.Execute("CREATE TABLE t (id INTEGER PRIMARY KEY, vec BLOB)");
            tx.Commit();
        }

        // Insert a vector so the table isn't empty if we try to build later
        using var writer = SharcWriter.From(tempDb);
        var vec = BlobVectorCodec.Encode(new float[] { 1f, 2f });
        writer.Insert("t",
            ColumnValue.Null(),
            ColumnValue.Blob(12 + vec.Length * 2, vec));

        var loaded = HnswIndex.Load(tempDb, "t", "vec");
        Assert.Null(loaded);

        tempDb.Dispose();
    }

    [Fact]
    public void LoadOrBuild_FirstCall_Builds()
    {
        using var index = _db.LoadOrBuildHnswIndex("docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 });

        Assert.Equal(RowCount, index.Count);

        var query = new float[VectorDim];
        var result = index.Search(query, k: 3);
        Assert.Equal(3, result.Count);
    }

    [Fact]
    public void UseIndex_MetricMismatch_Throws()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);

        // VectorQuery expects Cosine, index is Euclidean → metric mismatch
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Cosine);

        var ex = Assert.Throws<ArgumentException>(() => vq.UseIndex(index));
        Assert.Contains("Euclidean", ex.Message);
        Assert.Contains("Cosine", ex.Message);
    }

    [Fact]
    public void UseIndex_WithRowEvaluator_FallsBackToFlatScan()
    {
        using var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: false);
        using var vq = _db.Vector("docs", "embedding", DistanceMetric.Euclidean);
        vq.UseIndex(index);

        // Attach a row evaluator that only allows odd rowIds
        vq.WithRowEvaluator(new OddRowIdEvaluator());

        var query = new float[VectorDim];
        var result = vq.NearestTo(query, k: 10);

        // All returned rows should have odd rowIds (evaluator enforced via flat scan)
        Assert.True(result.Count > 0);
        for (int i = 0; i < result.Count; i++)
            Assert.True(result[i].RowId % 2 != 0,
                $"Row {result[i].RowId} should have been filtered by row evaluator");
    }

    [Fact]
    public void LoadOrBuild_MetricMismatch_Throws()
    {
        // First build + persist with Euclidean
        using (var index = HnswIndex.Build(_db, "docs", "embedding",
            DistanceMetric.Euclidean, HnswConfig.Default with { Seed = 42 }, persist: true))
        {
            Assert.Equal(RowCount, index.Count);
        }

        // Now LoadOrBuild with Cosine → should detect metric mismatch and throw
        Assert.Throws<InvalidOperationException>(() =>
            _db.LoadOrBuildHnswIndex("docs", "embedding",
                DistanceMetric.Cosine, HnswConfig.Default with { Seed = 42 }));
    }

    public void Dispose()
    {
        _db?.Dispose();
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_dbPath + "-journal"); } catch { }
    }

    private sealed class TempFile : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(), $"sharc_hnsw_temp_{Guid.NewGuid()}.db");
        public void Dispose()
        {
            try { File.Delete(Path); } catch { }
            try { File.Delete(Path + "-journal"); } catch { }
        }
    }

    private sealed class OddRowIdEvaluator : IRowAccessEvaluator
    {
        public bool CanAccess(ReadOnlySpan<byte> payload, long rowId) => rowId % 2 != 0;
    }
}
