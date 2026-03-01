/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Licensed under the MIT License â€” free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Sharc.Core.BTree;
using Sharc.Core.Format;
using Sharc.Core.IO;
using Sharc.Core.Records;

namespace Sharc.Comparisons;

/// <summary>
/// Graph seek benchmarks: point lookups on concept (node) table using B-tree Seek().
/// Compares Sharc cursor.Seek() vs SQLite WHERE rowid = ? for graph node retrieval.
/// Database: 5K nodes, 15K edges.
/// </summary>
[BenchmarkCategory("Comparative", "GraphSeek")]
[MemoryDiagnoser]
public class GraphSeekBenchmarks
{
    private string _dbPath = null!;
    private byte[] _dbBytes = null!;
    private SqliteConnection _conn = null!;
    private SqliteCommand _seekCmd = null!;
    private uint _conceptsRoot;

    // Target rowids for seeks â€” spread across the B-tree
    private static readonly long[] SeekTargets = [1, 50, 500, 1000, 2500, 4999];

    [GlobalSetup]
    public void Setup()
    {
        _dbPath = BenchmarkTempDb.CreatePath("graph_seek");
        GraphGenerator.GenerateSQLite(_dbPath, nodeCount: 5000, edgeCount: 15000);
        _dbBytes = BenchmarkTempDb.ReadAllBytesWithRetry(_dbPath);

        _conn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadOnly;Pooling=False");
        _conn.Open();

        _seekCmd = _conn.CreateCommand();
        _seekCmd.CommandText = "SELECT id, key, type_id, data FROM _concepts WHERE key = $key";
        _seekCmd.Parameters.Add("$key", SqliteType.Integer);
        _seekCmd.Prepare();

        // Pre-calculate conceptsRoot for pure seek benchmarks
        var pageSource = new MemoryPageSource(_dbBytes);
        var header = Sharc.Core.Format.DatabaseHeader.Parse(pageSource.GetPage(1));
        var bTreeReader = new BTreeReader<MemoryPageSource>(pageSource, header);
        using var schemaCursor = bTreeReader.CreateCursor(1);
        while (schemaCursor.MoveNext())
        {
            var decoder = new RecordDecoder();
            var cols = decoder.DecodeRecord(schemaCursor.Payload);
            if (cols.Length >= 5 && cols[1].AsString() == "_concepts")
            {
                _conceptsRoot = (uint)cols[3].AsInt64();
                break;
            }
        }
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        SqliteConnection.ClearAllPools();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _seekCmd?.Dispose();
        _conn?.Dispose();
        BenchmarkTempDb.TryDelete(_dbPath);
    }

    // --- Single node lookup by rowid ---

    [Benchmark]
    [BenchmarkCategory("SingleSeek")]
    public string Sharc_SeekSingleNode()
    {
        var pageSource = new MemoryPageSource(_dbBytes);
        var header = DatabaseHeader.Parse(pageSource.GetPage(1));
        var bTreeReader = new BTreeReader<MemoryPageSource>(pageSource, header);

        using var cursor = bTreeReader.CreateCursor(_conceptsRoot);
        cursor.Seek(2500);
        var rec = new RecordDecoder();
        var row = rec.DecodeRecord(cursor.Payload);
        return row[0].AsString(); // id
    }

    [Benchmark]
    [BenchmarkCategory("SingleSeek")]
    public string SQLite_SeekSingleNode()
    {
        _seekCmd.Parameters[0].Value = 2500L;
        using var reader = _seekCmd.ExecuteReader();
        reader.Read();
        return reader.GetString(0);
    }

    // --- Batch seek: 6 lookups spread across B-tree ---

    [Benchmark]
    [BenchmarkCategory("BatchSeek")]
    public long Sharc_SeekBatch6Nodes()
    {
        var pageSource = new MemoryPageSource(_dbBytes);
        var header = Sharc.Core.Format.DatabaseHeader.Parse(pageSource.GetPage(1));
        var bTreeReader = new BTreeReader<MemoryPageSource>(pageSource, header);

        long sum = 0;
        using var cursor = bTreeReader.CreateCursor(_conceptsRoot);
        var rec = new RecordDecoder();

        foreach (long target in SeekTargets)
        {
            if (cursor.Seek(target))
            {
                var row = rec.DecodeRecord(cursor.Payload);
                sum += row[2].AsInt64(); // type_id
            }
        }
        return sum;
    }

    [Benchmark]
    [BenchmarkCategory("BatchSeek")]
    public long SQLite_SeekBatch6Nodes()
    {
        long sum = 0;
        foreach (long target in SeekTargets)
        {
            _seekCmd.Parameters[0].Value = target;
            using var reader = _seekCmd.ExecuteReader();
            if (reader.Read())
            {
                sum += reader.GetInt64(2); // type_id
            }
        }
        return sum;
    }

    // --- Schema + seek (realistic open-seek-close pattern) ---

    [Benchmark]
    [BenchmarkCategory("OpenSeekClose")]
    public string? Sharc_OpenSeekClose()
    {
        using var db = SharcDatabase.OpenMemory(_dbBytes, new SharcOpenOptions { PageCacheSize = 0 });
        using var reader = db.CreateReader("_concepts");
        // B-tree Seek: binary search descent to rowid 2500 (key is INTEGER PRIMARY KEY)
        if (reader.Seek(2500))
            return reader.GetString(0);
        return null;
    }

    [Benchmark]
    [BenchmarkCategory("OpenSeekClose")]
    public string? SQLite_OpenSeekClose()
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadOnly;Pooling=False");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM _concepts WHERE key = 2500";
        return cmd.ExecuteScalar()?.ToString();
    }
}
