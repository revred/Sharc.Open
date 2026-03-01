/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Licensed under the MIT License — free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Sharc.Core;
using Sharc.Core.BTree;
using Sharc.Core.Format;
using Sharc.Core.IO;
using Sharc.Graph;
using Sharc.Graph.Model;
using Sharc.Graph.Schema;

namespace Sharc.Comparisons;

/// <summary>
/// Graph traversal benchmarks: 2-hop BFS using Index Scans.
/// Compares SharcContextGraph (index scan) vs SQLite (index scan via join/subquery).
/// Database: 5K nodes, 15K edges.
/// </summary>
[BenchmarkCategory("Comparative", "GraphTraversal")]
[MemoryDiagnoser]
public class GraphTraversalBenchmarks
{
    private string _dbPath = null!;
    private byte[] _dbBytes = null!;
    private SqliteConnection _conn = null!;
    private SharcContextGraph _graph = null!;
    private IBTreeReader _bTreeReader = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dbPath = BenchmarkTempDb.CreatePath("graph_traversal");
        GraphGenerator.GenerateSQLite(_dbPath, nodeCount: 5000, edgeCount: 15000);
        _dbBytes = BenchmarkTempDb.ReadAllBytesWithRetry(_dbPath);

        _conn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadOnly;Pooling=False");
        _conn.Open();

        // Sharc Setup
        var pageSource = new MemoryPageSource(_dbBytes);
        var header = DatabaseHeader.Parse(pageSource.GetPage(1));
        _bTreeReader = new BTreeReader<MemoryPageSource>(pageSource, header);
        _graph = new SharcContextGraph(_bTreeReader, new NativeSchemaAdapter());
        _graph.Initialize();
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        SqliteConnection.ClearAllPools();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _conn?.Dispose();
        _graph?.Dispose();
        BenchmarkTempDb.TryDelete(_dbPath);
    }

    [Benchmark]
    [BenchmarkCategory("BFS")]
    public int Sharc_BFS_2Hop()
    {
        // Zero-allocation cursor: edge-only 2-hop traversal.
        // Uses GetEdgeCursor + Reset — no GraphEdge allocation, no concept B-tree lookups.
        using var cursor = _graph.GetEdgeCursor(new NodeKey(1));

        // Hop 1: collect unique target keys
        var hop1Targets = new HashSet<long>();
        while (cursor.MoveNext())
            hop1Targets.Add(cursor.TargetKey);

        // Hop 2: count edges from each hop-1 target
        int hop2Count = 0;
        foreach (var target in hop1Targets)
        {
            cursor.Reset(target);
            while (cursor.MoveNext())
                hop2Count++;
        }

        return hop1Targets.Count + hop2Count;
    }

    [Benchmark]
    [BenchmarkCategory("BFS")]
    public (int, int) SQLite_BFS_2Hop()
    {
        long startKey = 1;

        // Hop 1
        var hop1Targets = new HashSet<long>();
        using (var cmd1 = _conn.CreateCommand())
        {
            cmd1.CommandText = "SELECT target_key FROM _relations WHERE source_key = $k";
            cmd1.Parameters.AddWithValue("$k", startKey);
            using var reader = cmd1.ExecuteReader();
            while (reader.Read())
            {
                hop1Targets.Add(reader.GetInt64(0));
            }
        }

        // Hop 2
        var hop2Count = 0;
        // Naive iteration to match Sharc logic (could be JOIN, but we compare client-side traversal logic often used in ORMs/Graph libs)
        // Or for fairness, use prepared statement reused.
        using (var cmd2 = _conn.CreateCommand())
        {
            cmd2.CommandText = "SELECT count(*) FROM _relations WHERE source_key = $k";
            var p = cmd2.Parameters.Add("$k", SqliteType.Integer);
            cmd2.Prepare();

            foreach (var target in hop1Targets)
            {
                p.Value = target;
                hop2Count += Convert.ToInt32(cmd2.ExecuteScalar());
            }
        }

        return (hop1Targets.Count, hop2Count);
    }
}
