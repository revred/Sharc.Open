/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Software here is treated not as static text, but as a living system designed to learn and evolve.
  Built on the belief that architecture and context often define outcomes before code is written.

  This file reflects an AI-aware, agentic, context-driven, and continuously evolving approach
  to modern engineering. If you seek to transform a traditional codebase into an adaptive,
  intelligence-guided system, you may find resonance in these patterns and principles.

  Subtle conversations often begin with a single message — or a prompt with the right context.
  https://www.linkedin.com/in/revodoc/

  Licensed under the MIT License — free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Sharc.Benchmarks.Helpers;
using Sharc.Core;

namespace Sharc.Benchmarks.Comparative;

/// <summary>
/// Benchmarks for index maintenance overhead during write operations.
/// Measures the cost of maintaining secondary indexes on Insert, Update, and Delete
/// compared to SQLite's index maintenance.
///
/// The canonical database has 5 indexes:
///   idx_users_username, idx_users_email (users table, 10K rows)
///   idx_events_user_id, idx_events_timestamp, idx_events_user_time (events table, 100K rows)
///
/// Each benchmark restores a fresh database per iteration so mutations are real (not miss paths).
/// </summary>
[BenchmarkCategory("Comparative", "IndexMutator")]
[MemoryDiagnoser]
public class IndexMutatorBenchmarks
{
    private string _dbPath = null!;
    private byte[] _dbBytes = null!;

    // Pre-built ColumnValue arrays matching the events table:
    // user_id INTEGER, event_type INTEGER, timestamp INTEGER, value REAL
    private static readonly ColumnValue[] InsertValues =
    [
        ColumnValue.FromInt64(4, 5000),         // user_id
        ColumnValue.FromInt64(1, 7),            // event_type
        ColumnValue.FromInt64(4, 1700000001),   // timestamp
        ColumnValue.FromDouble(42.5),           // value
    ];

    private static readonly ColumnValue[] UpdateValues =
    [
        ColumnValue.FromInt64(4, 9999),         // user_id  (changed — triggers index update)
        ColumnValue.FromInt64(1, 50),           // event_type
        ColumnValue.FromInt64(4, 1700099999),   // timestamp (changed — triggers index update)
        ColumnValue.FromDouble(999.99),         // value
    ];

    [GlobalSetup]
    public void Setup()
    {
        var dir = Path.Combine(Path.GetTempPath(), "sharc_bench_index");
        Directory.CreateDirectory(dir);
        _dbPath = TestDatabaseGenerator.CreateCanonicalDatabase(dir);
        _dbBytes = File.ReadAllBytes(_dbPath);
    }

    [IterationSetup]
    public void RestoreDatabase()
    {
        SqliteConnection.ClearAllPools();
        File.WriteAllBytes(_dbPath, _dbBytes);
    }

    // ═══════════════════════════════════════════════════════════════
    //  INSERT with index maintenance (events table has 3 indexes)
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("IndexInsert")]
    public long Sharc_Insert_WithIndexes()
    {
        using var writer = SharcWriter.Open(_dbPath);
        return writer.Insert("events", InsertValues);
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("IndexInsert")]
    public int SQLite_Insert_WithIndexes()
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO events (user_id, event_type, timestamp, value) VALUES (5000, 7, 1700000001, 42.5)";
        return cmd.ExecuteNonQuery();
    }

    // ═══════════════════════════════════════════════════════════════
    //  INSERT batch 100 rows with index maintenance
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("IndexInsertBatch")]
    public int Sharc_InsertBatch100_WithIndexes()
    {
        using var writer = SharcWriter.Open(_dbPath);
        using var tx = writer.BeginTransaction();
        for (int i = 0; i < 100; i++)
        {
            ColumnValue[] row =
            [
                ColumnValue.FromInt64(4, 1 + (i % 10000)),
                ColumnValue.FromInt64(1, 1 + (i % 50)),
                ColumnValue.FromInt64(4, 1700000000 + i),
                ColumnValue.FromDouble(i * 1.1),
            ];
            tx.Insert("events", row);
        }
        tx.Commit();
        return 100;
    }

    [Benchmark]
    [BenchmarkCategory("IndexInsertBatch")]
    public int SQLite_InsertBatch100_WithIndexes()
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var tx = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "INSERT INTO events (user_id, event_type, timestamp, value) VALUES (@uid, @et, @ts, @val)";
        var pUid = cmd.Parameters.Add("@uid", SqliteType.Integer);
        var pEt = cmd.Parameters.Add("@et", SqliteType.Integer);
        var pTs = cmd.Parameters.Add("@ts", SqliteType.Integer);
        var pVal = cmd.Parameters.Add("@val", SqliteType.Real);

        for (int i = 0; i < 100; i++)
        {
            pUid.Value = 1 + (i % 10000);
            pEt.Value = 1 + (i % 50);
            pTs.Value = 1700000000 + i;
            pVal.Value = i * 1.1;
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
        return 100;
    }

    // ═══════════════════════════════════════════════════════════════
    //  UPDATE with index maintenance (old index entry removed, new inserted)
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("IndexUpdate")]
    public bool Sharc_Update_WithIndexes()
    {
        using var writer = SharcWriter.Open(_dbPath);
        return writer.Update("events", 500, UpdateValues);
    }

    [Benchmark]
    [BenchmarkCategory("IndexUpdate")]
    public int SQLite_Update_WithIndexes()
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE events SET user_id=9999, event_type=50, timestamp=1700099999, value=999.99 WHERE rowid = 500";
        return cmd.ExecuteNonQuery();
    }

    // ═══════════════════════════════════════════════════════════════
    //  DELETE with index maintenance (index entries removed)
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("IndexDelete")]
    public bool Sharc_Delete_WithIndexes()
    {
        using var writer = SharcWriter.Open(_dbPath);
        return writer.Delete("events", 500);
    }

    [Benchmark]
    [BenchmarkCategory("IndexDelete")]
    public int SQLite_Delete_WithIndexes()
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM events WHERE rowid = 500";
        return cmd.ExecuteNonQuery();
    }

    // ═══════════════════════════════════════════════════════════════
    //  UPDATE batch 100 rows with index maintenance
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("IndexUpdateBatch")]
    public int Sharc_UpdateBatch100_WithIndexes()
    {
        using var writer = SharcWriter.Open(_dbPath);
        int count = 0;
        using var tx = writer.BeginTransaction();
        for (long rowId = 2000; rowId < 2100; rowId++)
        {
            if (tx.Update("events", rowId, UpdateValues))
                count++;
        }
        tx.Commit();
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("IndexUpdateBatch")]
    public int SQLite_UpdateBatch100_WithIndexes()
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var tx = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "UPDATE events SET user_id=9999, event_type=50, timestamp=1700099999, value=999.99 WHERE rowid = @id";
        var pId = cmd.Parameters.Add("@id", SqliteType.Integer);
        int count = 0;
        for (long rowId = 2000; rowId < 2100; rowId++)
        {
            pId.Value = rowId;
            count += cmd.ExecuteNonQuery();
        }
        tx.Commit();
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  DELETE batch 100 rows with index maintenance
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("IndexDeleteBatch")]
    public int Sharc_DeleteBatch100_WithIndexes()
    {
        using var writer = SharcWriter.Open(_dbPath);
        int count = 0;
        using var tx = writer.BeginTransaction();
        for (long rowId = 1000; rowId < 1100; rowId++)
        {
            if (tx.Delete("events", rowId))
                count++;
        }
        tx.Commit();
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("IndexDeleteBatch")]
    public int SQLite_DeleteBatch100_WithIndexes()
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var tx = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "DELETE FROM events WHERE rowid = @id";
        var pId = cmd.Parameters.Add("@id", SqliteType.Integer);
        int count = 0;
        for (long rowId = 1000; rowId < 1100; rowId++)
        {
            pId.Value = rowId;
            count += cmd.ExecuteNonQuery();
        }
        tx.Commit();
        return count;
    }
}
