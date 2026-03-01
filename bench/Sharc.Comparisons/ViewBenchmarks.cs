/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Licensed under the MIT License — free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using BenchmarkDotNet.Attributes;
using Sharc.Views;

namespace Sharc.Comparisons;

/// <summary>
/// Benchmarks for registered view query performance vs direct table access.
/// Measures:
///   - Direct table scan vs registered view scan (no filter)
///   - Subview chain depth impact (1/2/3 levels)
///   - Cross-type filter evaluation (integer column, double filter value)
///
/// Dataset: 5K users (reuses CoreDataGenerator).
/// </summary>
[MemoryDiagnoser]
public class ViewBenchmarks
{
    private string _dbPath = null!;
    private byte[] _dbBytes = null!;
    private SharcDatabase _sharcDb = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dbPath = BenchmarkTempDb.CreatePath("view");
        CoreDataGenerator.GenerateSQLite(_dbPath, userCount: 5000);
        _dbBytes = BenchmarkTempDb.ReadAllBytesWithRetry(_dbPath);

        _sharcDb = SharcDatabase.OpenMemory(_dbBytes, new SharcOpenOptions { PageCacheSize = 100 });

        // Register views for benchmarks
        var directView = ViewBuilder.From("users")
            .Select("name", "age", "score")
            .Named("v_users")
            .Build();
        _sharcDb.RegisterView(directView);

        // 1-deep subview
        var sub1 = ViewBuilder.From(directView)
            .Select("name", "age")
            .Named("v_sub1")
            .Build();
        _sharcDb.RegisterView(sub1);

        // 2-deep subview
        var sub2 = ViewBuilder.From(sub1)
            .Select("name")
            .Named("v_sub2")
            .Build();
        _sharcDb.RegisterView(sub2);

        // Filtered view (Func predicate)
        var filtered = ViewBuilder.From("users")
            .Select("name", "age", "score")
            .Where(row => row.GetInt64(1) > 30)
            .Named("v_filtered")
            .Build();
        _sharcDb.RegisterView(filtered);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _sharcDb?.Dispose();
        BenchmarkTempDb.TryDelete(_dbPath);
    }

    // ═══════════════════════════════════════════════════════════════
    //  1. DIRECT TABLE vs REGISTERED VIEW — sequential scan
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ViewScan")]
    public int DirectTable_SequentialScan()
    {
        using var reader = _sharcDb.CreateReader("users", "name", "age", "score");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("ViewScan")]
    public int RegisteredView_SequentialScan()
    {
        using var cursor = _sharcDb.OpenView("v_users");
        int count = 0;
        while (cursor.MoveNext()) count++;
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  2. SUBVIEW DEPTH — 0/1/2 levels of view-on-view
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("SubviewDepth")]
    public int Subview_Depth0_DirectView()
    {
        using var cursor = _sharcDb.OpenView("v_users");
        int count = 0;
        while (cursor.MoveNext()) count++;
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("SubviewDepth")]
    public int Subview_Depth1()
    {
        using var cursor = _sharcDb.OpenView("v_sub1");
        int count = 0;
        while (cursor.MoveNext()) count++;
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("SubviewDepth")]
    public int Subview_Depth2()
    {
        using var cursor = _sharcDb.OpenView("v_sub2");
        int count = 0;
        while (cursor.MoveNext()) count++;
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  3. SQL QUERY — direct table vs view reference
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("ViewQuery")]
    public int SqlQuery_DirectTable()
    {
        using var reader = _sharcDb.Query("SELECT name, age, score FROM users");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("ViewQuery")]
    public int SqlQuery_RegisteredView()
    {
        using var reader = _sharcDb.Query("SELECT * FROM v_users");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  4. FILTERED VIEW — pre-materialization overhead
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("FilteredView")]
    public int SqlQuery_FilteredView()
    {
        using var reader = _sharcDb.Query("SELECT * FROM v_filtered");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("FilteredView")]
    public int SqlQuery_DirectTableWithWhere()
    {
        using var reader = _sharcDb.Query("SELECT name, age, score FROM users WHERE age > 30");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  5. CROSS-TYPE FILTER — integer column, double filter value
    // ═══════════════════════════════════════════════════════════════

    [Benchmark]
    [BenchmarkCategory("CrossTypeFilter")]
    public int CrossType_IntColumnDoubleFilter()
    {
        // age is INTEGER, 30.5 is REAL — cross-type comparison
        using var reader = _sharcDb.Query("SELECT name FROM users WHERE age > 30.5");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("CrossTypeFilter")]
    public int SameType_IntColumnIntFilter()
    {
        using var reader = _sharcDb.Query("SELECT name FROM users WHERE age > 30");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}
