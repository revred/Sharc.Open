/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Licensed under the MIT License — free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using System.Runtime.InteropServices;
using System.Numerics.Tensors;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Sharc.Vector;
using Sharc.Vector.Hnsw;

namespace Sharc.Comparisons;

/// <summary>
/// Vector similarity search benchmarks: Sharc vs SQLite.
///
/// Sharc advantages:
///   - Zero-copy BLOB → float reinterpret via MemoryMarshal.Cast
///   - TensorPrimitives SIMD distance (AVX-512 when available)
///   - Pre-compiled VectorQuery handle (schema + distance resolved once)
///   - Metadata pre-filtering via FilterStar (rows skipped before distance)
///
/// SQLite baseline:
///   - GetBytes() to copy BLOB into managed buffer
///   - Buffer.BlockCopy to float[] for distance computation
///   - Manual top-K tracking
///
/// Dataset: 5,000 vectors × 128 dimensions (2.5 MB of vector data).
/// </summary>
[MemoryDiagnoser]
public class VectorSearchBenchmarks
{
    private const int RowCount = 5000;
    private const int Dimensions = 128;

    private string _dbPath = null!;
    private byte[] _dbBytes = null!;
    private SqliteConnection _conn = null!;
    private SharcDatabase _sharcDb = null!;
    private HnswIndex _hnswIndex = null!;
    private float[] _queryVector = null!;

    // Pre-allocated SQLite decode buffer (fair: amortize alloc across iterations)
    private byte[] _sqliteBlobBuf = null!;
    private float[] _sqliteFloatBuf = null!;

    // ── Prepared VectorQuery handles (zero per-call schema resolution) ──
    private VectorQuery _vqCosine = null!;
    private VectorQuery _vqCosineHnsw = null!;
    private VectorQuery _vqCosineFilter = null!;
    private VectorQuery _vqCosineHnswFilter = null!;
    private VectorQuery _vqEuclidean = null!;
    private VectorQuery _vqDotProduct = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dbPath = BenchmarkTempDb.CreatePath("vector");
        VectorDataGenerator.GenerateSQLite(_dbPath, RowCount, Dimensions);
        _dbBytes = BenchmarkTempDb.ReadAllBytesWithRetry(_dbPath);

        _conn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadOnly;Pooling=False");
        _conn.Open();

        _sharcDb = SharcDatabase.OpenMemory(_dbBytes, new SharcOpenOptions { PageCacheSize = 100 });
        _hnswIndex = _sharcDb.BuildHnswIndex(
            "vectors",
            "embedding",
            DistanceMetric.Cosine,
            HnswConfig.Default with { Seed = 42 },
            persist: false);
        _queryVector = VectorDataGenerator.GenerateQueryVector(Dimensions);

        _sqliteBlobBuf = new byte[Dimensions * sizeof(float)];
        _sqliteFloatBuf = new float[Dimensions];

        // Pre-compiled VectorQuery handles — schema + distance resolved once
        _vqCosine = _sharcDb.Vector("vectors", "embedding", DistanceMetric.Cosine);
        _vqCosineHnsw = _sharcDb.Vector("vectors", "embedding", DistanceMetric.Cosine);
        _vqCosineHnsw.UseIndex(_hnswIndex);
        _vqCosineFilter = _sharcDb.Vector("vectors", "embedding", DistanceMetric.Cosine);
        _vqCosineFilter.Where(FilterStar.Column("category").Eq("science"));
        _vqCosineHnswFilter = _sharcDb.Vector("vectors", "embedding", DistanceMetric.Cosine);
        _vqCosineHnswFilter.UseIndex(_hnswIndex);
        _vqCosineHnswFilter.Where(FilterStar.Column("category").Eq("science"));
        _vqEuclidean = _sharcDb.Vector("vectors", "embedding", DistanceMetric.Euclidean);
        _vqDotProduct = _sharcDb.Vector("vectors", "embedding", DistanceMetric.DotProduct);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _vqCosine?.Dispose();
        _vqCosineHnsw?.Dispose();
        _vqCosineFilter?.Dispose();
        _vqCosineHnswFilter?.Dispose();
        _vqEuclidean?.Dispose();
        _vqDotProduct?.Dispose();
        _conn?.Dispose();
        _hnswIndex?.Dispose();
        _sharcDb?.Dispose();
        BenchmarkTempDb.TryDelete(_dbPath);
    }

    // ═══════════════════════════════════════════════════════════════
    //  1. FULL VECTOR SCAN — scan all rows, compute distance to each
    //     Measures: BLOB decode + distance computation throughput
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("VectorScan")]
    public float Sharc_VectorScan()
    {
        var results = _vqCosine.NearestTo(_queryVector, k: RowCount);
        return results[0].Distance;
    }

    [Benchmark]
    [BenchmarkCategory("VectorScan")]
    public float SQLite_VectorScan()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT embedding FROM vectors";
        using var reader = cmd.ExecuteReader();

        float bestDistance = float.MaxValue;
        while (reader.Read())
        {
            reader.GetBytes(0, 0, _sqliteBlobBuf, 0, _sqliteBlobBuf.Length);
            Buffer.BlockCopy(_sqliteBlobBuf, 0, _sqliteFloatBuf, 0, _sqliteBlobBuf.Length);

            float distance = 1f - TensorPrimitives.CosineSimilarity(
                _queryVector.AsSpan(), _sqliteFloatBuf.AsSpan());
            if (distance < bestDistance)
                bestDistance = distance;
        }
        return bestDistance;
    }

    // ═══════════════════════════════════════════════════════════════
    //  2. TOP-K NEAREST NEIGHBOR — find K closest vectors
    //     Measures: end-to-end similarity search (scan + distance + heap)
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("NearestNeighbor")]
    public int Sharc_NearestTo_Top10()
    {
        var results = _vqCosine.NearestTo(_queryVector, k: 10);
        return results.Count;
    }

    [Benchmark]
    [BenchmarkCategory("NearestNeighbor")]
    [BenchmarkCategory("Hnsw")]
    public int Sharc_Hnsw_NearestTo_Top10()
    {
        var results = _vqCosineHnsw.NearestTo(_queryVector, k: 10);
        return results.Count;
    }

    [Benchmark]
    [BenchmarkCategory("NearestNeighbor")]
    public int SQLite_NearestTo_Top10()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT rowid, embedding FROM vectors";
        using var reader = cmd.ExecuteReader();

        // Manual top-10 via sorted list (simple but fair)
        var topK = new SortedList<float, long>(11);
        while (reader.Read())
        {
            long rowid = reader.GetInt64(0);
            reader.GetBytes(1, 0, _sqliteBlobBuf, 0, _sqliteBlobBuf.Length);
            Buffer.BlockCopy(_sqliteBlobBuf, 0, _sqliteFloatBuf, 0, _sqliteBlobBuf.Length);

            float distance = 1f - TensorPrimitives.CosineSimilarity(
                _queryVector.AsSpan(), _sqliteFloatBuf.AsSpan());

            // Use distance + rowid to avoid duplicate key issues
            while (topK.ContainsKey(distance))
                distance = float.BitIncrement(distance);
            topK.Add(distance, rowid);

            if (topK.Count > 10)
                topK.RemoveAt(topK.Count - 1);
        }
        return topK.Count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  3. FILTERED VECTOR SEARCH — metadata pre-filter + distance
    //     Measures: filter-then-distance vs scan-all-then-filter
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("FilteredVector")]
    public int Sharc_FilteredVector_Top10()
    {
        var results = _vqCosineFilter.NearestTo(_queryVector, k: 10);
        return results.Count;
    }

    [Benchmark]
    [BenchmarkCategory("FilteredVector")]
    [BenchmarkCategory("Hnsw")]
    public int Sharc_Hnsw_FilterAware_Top10()
    {
        var results = _vqCosineHnswFilter.NearestTo(_queryVector, k: 10);
        return results.Count;
    }

    [Benchmark]
    [BenchmarkCategory("FilteredVector")]
    [BenchmarkCategory("Planner")]
    public int Sharc_Hnsw_FilterAware_Strategy()
    {
        _ = _vqCosineHnswFilter.NearestTo(_queryVector, k: 10);
        return (int)_vqCosineHnswFilter.LastExecutionInfo.Strategy;
    }

    [Benchmark]
    [BenchmarkCategory("FilteredVector")]
    public int SQLite_FilteredVector_Top10()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT rowid, embedding FROM vectors WHERE category = 'science'";
        using var reader = cmd.ExecuteReader();

        var topK = new SortedList<float, long>(11);
        while (reader.Read())
        {
            long rowid = reader.GetInt64(0);
            reader.GetBytes(1, 0, _sqliteBlobBuf, 0, _sqliteBlobBuf.Length);
            Buffer.BlockCopy(_sqliteBlobBuf, 0, _sqliteFloatBuf, 0, _sqliteBlobBuf.Length);

            float distance = 1f - TensorPrimitives.CosineSimilarity(
                _queryVector.AsSpan(), _sqliteFloatBuf.AsSpan());

            while (topK.ContainsKey(distance))
                distance = float.BitIncrement(distance);
            topK.Add(distance, rowid);

            if (topK.Count > 10)
                topK.RemoveAt(topK.Count - 1);
        }
        return topK.Count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  4. DISTANCE METRICS — compare all three metrics
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("DistanceMetric")]
    public float Sharc_Cosine_Top10()
    {
        var results = _vqCosine.NearestTo(_queryVector, k: 10);
        return results[0].Distance;
    }

    [Benchmark]
    [BenchmarkCategory("DistanceMetric")]
    public float Sharc_Euclidean_Top10()
    {
        var results = _vqEuclidean.NearestTo(_queryVector, k: 10);
        return results[0].Distance;
    }

    [Benchmark]
    [BenchmarkCategory("DistanceMetric")]
    public float Sharc_DotProduct_Top10()
    {
        var results = _vqDotProduct.NearestTo(_queryVector, k: 10);
        return results[0].Distance;
    }
}
