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

[BenchmarkCategory("Comparative", "GraphTraversal")]
[MemoryDiagnoser]
[InProcess]
public class GraphDirectionBenchmarks
{
    private string _dbPath = null!;
    private byte[] _dbBytes = null!;
    private SqliteConnection _conn = null!;
    private SharcContextGraph _graph = null!;
    private IBTreeReader _bTreeReader = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dbPath = BenchmarkTempDb.CreatePath("graph_direction");
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

    private static readonly TraversalPolicy IncomingPolicy = new()
    {
        Direction = TraversalDirection.Incoming,
        MaxDepth = 1,
        IncludeData = false,
    };

    [Benchmark]
    [BenchmarkCategory("Incoming")]
    public int Sharc_Incoming_1Hop()
    {
        // Zero-allocation BFS via Traverse — replaces allocating GetIncomingEdges path.
        var result = _graph.Traverse(new NodeKey(500), IncomingPolicy);
        return result.Nodes.Count;
    }

    [Benchmark]
    [BenchmarkCategory("Incoming")]
    public int SQLite_Incoming_1Hop()
    {
        int count = 0;
        using (var cmd = _conn.CreateCommand())
        {
            // Reverse lookup: find edges where target is 500
            cmd.CommandText = "SELECT source_key FROM _relations WHERE target_key = 500";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                count++;
            }
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("Bidirectional")]
    public int Sharc_BiDir_1Hop()
    {
        var policy = new TraversalPolicy
        {
            Direction = TraversalDirection.Both,
            MaxDepth = 1
        };
        var result = _graph.Traverse(new NodeKey(500), policy);
        return result.Nodes.Count;
    }

    [Benchmark]
    [BenchmarkCategory("Bidirectional")]
    public int SQLite_BiDir_1Hop()
    {
        int count = 0;
        var visited = new HashSet<long>();
        visited.Add(500);

        using (var cmd = _conn.CreateCommand())
        {
            // Outgoing
            cmd.CommandText = "SELECT target_key FROM _relations WHERE source_key = 500 UNION SELECT source_key FROM _relations WHERE target_key = 500";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                long id = reader.GetInt64(0);
                if (visited.Add(id))
                {
                    count++;
                }
            }
        }
        return visited.Count;
    }
}
