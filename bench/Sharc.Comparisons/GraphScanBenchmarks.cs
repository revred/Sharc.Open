/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Licensed under the MIT License â€” free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Sharc.Core.Query;

namespace Sharc.Comparisons;

/// <summary>
/// Graph scan benchmarks: full table scans over concept (node) and relation (edge) tables.
/// Compares Sharc sequential scan vs SQLite SELECT for graph-shaped data.
/// Database: 5K nodes, 15K edges (code dependency graph topology).
/// </summary>
[BenchmarkCategory("Comparative", "GraphScan")]
[MemoryDiagnoser]
public class GraphScanBenchmarks
{
    private string _dbPath = null!;
    private byte[] _dbBytes = null!;
    private SqliteConnection _conn = null!;
    private SharcDatabase _sharcDb = null!;

    // ── Prepared handles (zero-alloc hot paths) ──────────────────────
    private PreparedReader _preparedNodes = null!;
    private PreparedReader _preparedEdges = null!;
    private PreparedReader _preparedEdgesProjected = null!;  // (source_key, kind, target_key) — no id TEXT decode
    private PreparedReader _preparedNodesProjection = null!;
    private PreparedQuery _preparedEdgeFilterZeroAlloc = null!;
    private PreparedQuery _preparedEdgeFilterPushdown = null!;

    // ── Cached static filters (avoid per-call allocation) ────────────
    private static readonly SharcFilter[] KindEqualsFilter =
    [
        new SharcFilter("kind", SharcOperator.Equal, (long)15),
    ];

    [GlobalSetup]
    public void Setup()
    {
        _dbPath = BenchmarkTempDb.CreatePath("graph_scan");
        GraphGenerator.GenerateSQLite(_dbPath, nodeCount: 5000, edgeCount: 15000);
        _dbBytes = BenchmarkTempDb.ReadAllBytesWithRetry(_dbPath);

        _conn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadOnly;Pooling=False");
        _conn.Open();

        _sharcDb = SharcDatabase.OpenMemory(_dbBytes, new SharcOpenOptions { PageCacheSize = 0 });

        // Prepared readers — resolve schema + cursor once
        _preparedNodes = _sharcDb.PrepareReader("_concepts");
        _preparedEdges = _sharcDb.PrepareReader("_relations");
        _preparedEdgesProjected = _sharcDb.PrepareReader("_relations", "source_key", "kind", "target_key");
        _preparedNodesProjection = _sharcDb.PrepareReader("_concepts", "id", "type_id");

        // Prepared queries for filter benchmarks
        _preparedEdgeFilterZeroAlloc = _sharcDb.Prepare(
            "SELECT kind FROM _relations WHERE kind = 15");
        _preparedEdgeFilterPushdown = _sharcDb.Prepare(
            "SELECT source_key, kind, target_key FROM _relations WHERE kind = 15");
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        SqliteConnection.ClearAllPools();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _preparedNodes?.Dispose();
        _preparedEdges?.Dispose();
        _preparedEdgesProjected?.Dispose();
        _preparedNodesProjection?.Dispose();
        _preparedEdgeFilterZeroAlloc?.Dispose();
        _preparedEdgeFilterPushdown?.Dispose();
        _conn?.Dispose();
        _sharcDb?.Dispose();
        BenchmarkTempDb.TryDelete(_dbPath);
    }

    // --- Scan all nodes (concepts) ---

    [Benchmark]
    [BenchmarkCategory("NodeScan")]
    public long Sharc_ScanAllNodes()
    {
        using var reader = _preparedNodes.CreateReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetUtf8Span(0);  // id — UTF-8 span, no string alloc
            _ = reader.GetInt64(1);     // key
            _ = reader.GetInt64(2);     // type_id
            _ = reader.GetUtf8Span(3);  // data — UTF-8 span, no string alloc
            count++;
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("NodeScan")]
    public long SQLite_ScanAllNodes()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT id, key, type_id, data FROM _concepts";
        long count = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            _ = reader.GetString(0);
            _ = reader.GetInt64(1);
            _ = reader.GetInt64(2);
            _ = reader.GetString(3);
            count++;
        }
        return count;
    }

    // --- Scan all edges (relations) ---

    [Benchmark]
    [BenchmarkCategory("EdgeScan")]
    public long Sharc_ScanAllEdges()
    {
        using var reader = _preparedEdgesProjected.CreateReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);   // source_key
            _ = reader.GetInt64(1);   // kind
            _ = reader.GetInt64(2);   // target_key
            count++;
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("EdgeScan")]
    public long SQLite_ScanAllEdges()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT source_key, kind, target_key FROM _relations";
        long count = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetInt64(1);
            _ = reader.GetInt64(2);
            count++;
        }
        return count;
    }

    // --- Scan nodes with projection (id + type_id only) ---

    [Benchmark]
    [BenchmarkCategory("NodeProjection")]
    public long Sharc_ScanNodes_Projection()
    {
        using var reader = _preparedNodesProjection.CreateReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetUtf8Span(0);  // id — UTF-8 span, no string alloc
            _ = reader.GetInt64(1);
            count++;
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("NodeProjection")]
    public long SQLite_ScanNodes_Projection()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT id, type_id FROM _concepts";
        long count = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            _ = reader.GetString(0);
            _ = reader.GetInt64(1);
            count++;
        }
        return count;
    }

    // --- Scan edges and count by kind (filter simulation) ---

    [Benchmark]
    [BenchmarkCategory("EdgeFilter")]
    public long Sharc_ScanEdges_ZeroAlloc()
    {
        using var reader = _preparedEdgeFilterZeroAlloc.Execute();
        long matchCount = 0;
        while (reader.Read())
        {
            matchCount++;
        }
        return matchCount;
    }

    [Benchmark]
    [BenchmarkCategory("EdgeFilter")]
    public long Sharc_ScanEdges_PushdownKind()
    {
        using var reader = _preparedEdgeFilterPushdown.Execute();
        long matchCount = 0;
        while (reader.Read())
        {
            matchCount++;
        }
        return matchCount;
    }

    [Benchmark]
    [BenchmarkCategory("EdgeFilter")]
    public long Sharc_ScanEdges_CountByKind()
    {
        using var reader = _preparedEdges.CreateReader();
        long matchCount = 0;
        while (reader.Read())
        {
            long kind = reader.GetInt64(2);
            if (kind == 15) // "Calls" relationship
                matchCount++;
        }
        return matchCount;
    }

    [Benchmark]
    [BenchmarkCategory("EdgeFilter")]
    public long SQLite_ScanEdges_CountByKind()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT kind FROM _relations";
        long matchCount = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            long kind = reader.GetInt64(0);
            if (kind == 15)
                matchCount++;
        }
        return matchCount;
    }
}
