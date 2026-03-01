/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Licensed under the MIT License — free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using Microsoft.Data.Sqlite;

namespace Sharc.Comparisons;

/// <summary>
/// Full query roundtrip benchmarks: Sharc vs SQLite.
/// Measures the complete pipeline — parse → compile → execute → read all rows —
/// with memory allocation tracking via [MemoryDiagnoser].
///
/// Categories:
///   SimpleQuery:     SELECT * / SELECT cols WHERE condition
///   MediumQuery:     WHERE + ORDER BY + LIMIT
///   AggregateQuery:  GROUP BY + COUNT/SUM
///   CompoundQuery:   UNION / UNION ALL / INTERSECT / EXCEPT
///   CoteQuery:        WITH ... AS (...) SELECT ...
///
/// Database: Two tables (users_a, users_b) with identical schemas and overlapping data.
///   - users_a: 2,500 rows (id 1–2500)
///   - users_b: 2,500 rows (id 2001–4500) — 500 overlap with users_a
///
/// Run with:
///   dotnet run -c Release --project bench/Sharc.Comparisons -- --filter *QueryRoundtrip*
/// </summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[RankColumn]
public class QueryRoundtripBenchmarks
{
    private string _dbPath = null!;
    private byte[] _dbBytes = null!;
    private SqliteConnection _conn = null!;
    private SharcDatabase _sharcDb = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dbPath = BenchmarkTempDb.CreatePath("roundtrip");
        RoundtripDataGenerator.Generate(_dbPath, rowsPerTable: 2500, overlapStart: 2001);
        _dbBytes = BenchmarkTempDb.ReadAllBytesWithRetry(_dbPath);

        _conn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadOnly;Pooling=False");
        _conn.Open();

        _sharcDb = SharcDatabase.OpenMemory(_dbBytes, new SharcOpenOptions { PageCacheSize = 100 });
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        SqliteConnection.ClearAllPools();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _sharcDb?.Dispose();
        _conn?.Dispose();
        BenchmarkTempDb.TryDelete(_dbPath);
    }

    // ═══════════════════════════════════════════════════════════════
    //  1. SIMPLE QUERY — SELECT * / SELECT cols
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: SELECT * FROM users_a")]
    [BenchmarkCategory("SimpleQuery")]
    public long Sharc_Simple_SelectAll()
    {
        using var reader = _sharcDb.Query("SELECT * FROM users_a");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT * FROM users_a")]
    [BenchmarkCategory("SimpleQuery")]
    public long SQLite_Simple_SelectAll()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM users_a";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  2. FILTERED QUERY — SELECT cols WHERE condition
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: SELECT WHERE age > 30")]
    [BenchmarkCategory("FilteredQuery")]
    public long Sharc_Filtered_WhereAge()
    {
        using var reader = _sharcDb.Query("SELECT id, name, age FROM users_a WHERE age > 30");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetInt64(2);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: SELECT WHERE age > 30")]
    [BenchmarkCategory("FilteredQuery")]
    public long SQLite_Filtered_WhereAge()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, age FROM users_a WHERE age > 30";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetInt32(2);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  3. MEDIUM QUERY — WHERE + ORDER BY + LIMIT
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: WHERE + ORDER BY + LIMIT 100")]
    [BenchmarkCategory("MediumQuery")]
    public long Sharc_Medium_OrderLimit()
    {
        using var reader = _sharcDb.Query(
            "SELECT id, name, score FROM users_a WHERE age >= 25 AND active = 1 ORDER BY score DESC LIMIT 100");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetDouble(2);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: WHERE + ORDER BY + LIMIT 100")]
    [BenchmarkCategory("MediumQuery")]
    public long SQLite_Medium_OrderLimit()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, score FROM users_a WHERE age >= 25 AND active = 1 ORDER BY score DESC LIMIT 100";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetDouble(2);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  4. AGGREGATE QUERY — GROUP BY + COUNT/SUM
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: GROUP BY dept + COUNT + AVG")]
    [BenchmarkCategory("AggregateQuery")]
    public long Sharc_Aggregate_GroupBy()
    {
        using var reader = _sharcDb.Query(
            "SELECT dept, COUNT(*) AS cnt, AVG(score) AS avg_score FROM users_a GROUP BY dept");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetString(0);
            _ = reader.GetInt64(1);
            _ = reader.GetDouble(2);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: GROUP BY dept + COUNT + AVG")]
    [BenchmarkCategory("AggregateQuery")]
    public long SQLite_Aggregate_GroupBy()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT dept, COUNT(*) AS cnt, AVG(score) AS avg_score FROM users_a GROUP BY dept";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetString(0);
            _ = reader.GetInt64(1);
            _ = reader.GetDouble(2);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  5. UNION ALL — concatenate two result sets
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: UNION ALL (2×2500 rows)")]
    [BenchmarkCategory("CompoundQuery")]
    public long Sharc_UnionAll()
    {
        using var reader = _sharcDb.Query(
            "SELECT id, name, dept FROM users_a UNION ALL SELECT id, name, dept FROM users_b");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetString(2);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: UNION ALL (2×2500 rows)")]
    [BenchmarkCategory("CompoundQuery")]
    public long SQLite_UnionAll()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, dept FROM users_a UNION ALL SELECT id, name, dept FROM users_b";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetString(2);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  6. UNION — deduplicated merge
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: UNION (dedup, 500 overlap)")]
    [BenchmarkCategory("CompoundQuery")]
    public long Sharc_Union()
    {
        using var reader = _sharcDb.Query(
            "SELECT id, name, dept FROM users_a UNION SELECT id, name, dept FROM users_b");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: UNION (dedup, 500 overlap)")]
    [BenchmarkCategory("CompoundQuery")]
    public long SQLite_Union()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, dept FROM users_a UNION SELECT id, name, dept FROM users_b";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  7. INTERSECT — rows in both tables
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: INTERSECT (500 common)")]
    [BenchmarkCategory("CompoundQuery")]
    public long Sharc_Intersect()
    {
        using var reader = _sharcDb.Query(
            "SELECT id, name, dept FROM users_a INTERSECT SELECT id, name, dept FROM users_b");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: INTERSECT (500 common)")]
    [BenchmarkCategory("CompoundQuery")]
    public long SQLite_Intersect()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, dept FROM users_a INTERSECT SELECT id, name, dept FROM users_b";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  8. EXCEPT — rows in left but not right
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: EXCEPT (left − right)")]
    [BenchmarkCategory("CompoundQuery")]
    public long Sharc_Except()
    {
        using var reader = _sharcDb.Query(
            "SELECT id, name, dept FROM users_a EXCEPT SELECT id, name, dept FROM users_b");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: EXCEPT (left − right)")]
    [BenchmarkCategory("CompoundQuery")]
    public long SQLite_Except()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, dept FROM users_a EXCEPT SELECT id, name, dept FROM users_b";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  9. UNION + ORDER BY + LIMIT — compound with post-processing
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: UNION ALL + ORDER BY + LIMIT 50")]
    [BenchmarkCategory("CompoundQuery")]
    public long Sharc_UnionAll_OrderLimit()
    {
        using var reader = _sharcDb.Query(
            "SELECT id, name, score FROM users_a UNION ALL SELECT id, name, score FROM users_b ORDER BY score DESC LIMIT 50");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetDouble(2);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: UNION ALL + ORDER BY + LIMIT 50")]
    [BenchmarkCategory("CompoundQuery")]
    public long SQLite_UnionAll_OrderLimit()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, score FROM users_a UNION ALL SELECT id, name, score FROM users_b ORDER BY score DESC LIMIT 50";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetDouble(2);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  10. Cote — WITH clause + SELECT from Cote
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: Cote → SELECT WHERE")]
    [BenchmarkCategory("CoteQuery")]
    public long Sharc_Cote_SimpleSelect()
    {
        using var reader = _sharcDb.Query(
            "WITH active AS (SELECT id, name, score FROM users_a WHERE active = 1) " +
            "SELECT id, name, score FROM active WHERE score > 50");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetDouble(2);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: Cote → SELECT WHERE")]
    [BenchmarkCategory("CoteQuery")]
    public long SQLite_Cote_SimpleSelect()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText =
            "WITH active AS (SELECT id, name, score FROM users_a WHERE active = 1) " +
            "SELECT id, name, score FROM active WHERE score > 50";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetDouble(2);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  11. Cote + UNION — Cote used in compound query
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: Cote + UNION ALL")]
    [BenchmarkCategory("CoteQuery")]
    public long Sharc_Cote_UnionAll()
    {
        using var reader = _sharcDb.Query(
            "WITH eng AS (SELECT id, name, score FROM users_a WHERE dept = 'eng') " +
            "SELECT id, name, score FROM eng UNION ALL SELECT id, name, score FROM users_b");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: Cote + UNION ALL")]
    [BenchmarkCategory("CoteQuery")]
    public long SQLite_Cote_UnionAll()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText =
            "WITH eng AS (SELECT id, name, score FROM users_a WHERE dept = 'eng') " +
            "SELECT id, name, score FROM eng UNION ALL SELECT id, name, score FROM users_b";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  12. PARAMETERIZED QUERY — bound parameters
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: parameterized WHERE")]
    [BenchmarkCategory("ParameterizedQuery")]
    public long Sharc_Parameterized()
    {
        var parameters = new Dictionary<string, object>
        {
            ["min_age"] = (long)25,
            ["max_score"] = 75.0
        };
        using var reader = _sharcDb.Query(parameters,
            "SELECT id, name FROM users_a WHERE age > $min_age AND score < $max_score");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: parameterized WHERE")]
    [BenchmarkCategory("ParameterizedQuery")]
    public long SQLite_Parameterized()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT id, name FROM users_a WHERE age > $min_age AND score < $max_score";
        cmd.Parameters.AddWithValue("$min_age", 25);
        cmd.Parameters.AddWithValue("$max_score", 75.0);
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            count++;
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  13. THREE-WAY UNION — compound chain
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Description = "Sharc: 3-way UNION ALL")]
    [BenchmarkCategory("CompoundQuery")]
    public long Sharc_ThreeWayUnionAll()
    {
        using var reader = _sharcDb.Query(
            "SELECT id, name FROM users_a " +
            "UNION ALL SELECT id, name FROM users_b " +
            "UNION ALL SELECT id, name FROM users_a");
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SQLite: 3-way UNION ALL")]
    [BenchmarkCategory("CompoundQuery")]
    public long SQLite_ThreeWayUnionAll()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText =
            "SELECT id, name FROM users_a " +
            "UNION ALL SELECT id, name FROM users_b " +
            "UNION ALL SELECT id, name FROM users_a";
        using var reader = cmd.ExecuteReader();
        long count = 0;
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            count++;
        }
        return count;
    }
}
