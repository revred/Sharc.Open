// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;

namespace Sharc.Comparisons;

/// <summary>
/// Head-to-head comparison of execution tiers: DIRECT vs CACHED vs JIT.
/// Also includes manual Prepare() and Jit() as reference points.
///
/// Measures per-call overhead (not result materialization).
/// Same 2,500-row database as QueryRoundtripBenchmarks.
///
/// Run with:
///   dotnet run -c Release --project bench/Sharc.Comparisons -- --filter *ExecutionTier*
/// </summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[RankColumn]
public class ExecutionTierBenchmarks
{
    private string _dbPath = null!;
    private byte[] _dbBytes = null!;
    private SharcDatabase _sharcDb = null!;

    // Pre-created handles for manual tier comparison
    private PreparedQuery _prepared = null!;
    private PreparedQuery _preparedParam = null!;
    private JitQuery _jitHandle = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dbPath = BenchmarkTempDb.CreatePath("roundtrip_tiers");
        RoundtripDataGenerator.Generate(_dbPath, rowsPerTable: 2500, overlapStart: 2001);
        _dbBytes = BenchmarkTempDb.ReadAllBytesWithRetry(_dbPath);

        _sharcDb = SharcDatabase.OpenMemory(_dbBytes, new SharcOpenOptions { PageCacheSize = 100 });

        // Pre-create handles for manual tier benchmarks
        _prepared = _sharcDb.Prepare("SELECT id, name, age FROM users_a WHERE age > 30");
        _preparedParam = _sharcDb.Prepare("SELECT id, name, age FROM users_a WHERE age > $minAge");
        _jitHandle = _sharcDb.Jit("users_a");
        _jitHandle.Where(FilterStar.Column("age").Gt(30L));
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _jitHandle?.Dispose();
        _preparedParam?.Dispose();
        _prepared?.Dispose();
        _sharcDb?.Dispose();
        BenchmarkTempDb.TryDelete(_dbPath);
    }

    // ═══════════════════════════════════════════════════════════════
    //  A. FILTERED SCAN — WHERE age > 30 (all tiers)
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Baseline = true, Description = "DIRECT: Query(sql)")]
    [BenchmarkCategory("FilteredScan")]
    public long Direct_Filtered()
    {
        using var reader = _sharcDb.Query("SELECT id, name, age FROM users_a WHERE age > 30");
        long count = 0;
        while (reader.Read()) { _ = reader.GetInt64(0); count++; }
        return count;
    }

    [Benchmark(Description = "CACHED: Query(hint sql)")]
    [BenchmarkCategory("FilteredScan")]
    public long Cached_Filtered()
    {
        using var reader = _sharcDb.Query("CACHED SELECT id, name, age FROM users_a WHERE age > 30");
        long count = 0;
        while (reader.Read()) { _ = reader.GetInt64(0); count++; }
        return count;
    }

    [Benchmark(Description = "JIT: Query(hint sql)")]
    [BenchmarkCategory("FilteredScan")]
    public long Jit_Filtered()
    {
        using var reader = _sharcDb.Query("JIT SELECT id, name, age FROM users_a WHERE age > 30");
        long count = 0;
        while (reader.Read()) { _ = reader.GetInt64(0); count++; }
        return count;
    }

    [Benchmark(Description = "Manual Prepare.Execute()")]
    [BenchmarkCategory("FilteredScan")]
    public long ManualPrepare_Filtered()
    {
        using var reader = _prepared.Execute();
        long count = 0;
        while (reader.Read()) { _ = reader.GetInt64(0); count++; }
        return count;
    }

    [Benchmark(Description = "Manual Jit.Query()")]
    [BenchmarkCategory("FilteredScan")]
    public long ManualJit_Filtered()
    {
        using var reader = _jitHandle.Query("id", "name", "age");
        long count = 0;
        while (reader.Read()) { _ = reader.GetInt64(0); count++; }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  B. FULL SCAN — SELECT * (no filter, all tiers)
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Baseline = true, Description = "DIRECT: SELECT *")]
    [BenchmarkCategory("FullScan")]
    public long Direct_SelectAll()
    {
        using var reader = _sharcDb.Query("SELECT * FROM users_a");
        long count = 0;
        while (reader.Read()) { _ = reader.GetInt64(0); count++; }
        return count;
    }

    [Benchmark(Description = "CACHED: SELECT *")]
    [BenchmarkCategory("FullScan")]
    public long Cached_SelectAll()
    {
        using var reader = _sharcDb.Query("CACHED SELECT * FROM users_a");
        long count = 0;
        while (reader.Read()) { _ = reader.GetInt64(0); count++; }
        return count;
    }

    [Benchmark(Description = "JIT: SELECT *")]
    [BenchmarkCategory("FullScan")]
    public long Jit_SelectAll()
    {
        using var reader = _sharcDb.Query("JIT SELECT * FROM users_a");
        long count = 0;
        while (reader.Read()) { _ = reader.GetInt64(0); count++; }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  C. PARAMETERIZED FILTER — WHERE age > $minAge (CACHED sweet spot)
    // ═══════════════════════════════════════════════════════════════

    private static readonly Dictionary<string, object> ParamAge40 = new() { ["minAge"] = 40L };

    [Benchmark(Baseline = true, Description = "DIRECT: parameterized")]
    [BenchmarkCategory("ParameterizedFilter")]
    public long Direct_Parameterized()
    {
        using var reader = _sharcDb.Query(ParamAge40,
            "SELECT id, name, age FROM users_a WHERE age > $minAge");
        long count = 0;
        while (reader.Read()) { _ = reader.GetInt64(0); count++; }
        return count;
    }

    [Benchmark(Description = "CACHED: parameterized")]
    [BenchmarkCategory("ParameterizedFilter")]
    public long Cached_Parameterized()
    {
        using var reader = _sharcDb.Query(ParamAge40,
            "CACHED SELECT id, name, age FROM users_a WHERE age > $minAge");
        long count = 0;
        while (reader.Read()) { _ = reader.GetInt64(0); count++; }
        return count;
    }

    [Benchmark(Description = "Manual Prepare: parameterized")]
    [BenchmarkCategory("ParameterizedFilter")]
    public long ManualPrepare_Parameterized()
    {
        using var reader = _preparedParam.Execute(ParamAge40);
        long count = 0;
        while (reader.Read()) { _ = reader.GetInt64(0); count++; }
        return count;
    }

    // ═══════════════════════════════════════════════════════════════
    //  D. NARROW PROJECTION — SELECT name (single column, all tiers)
    // ═══════════════════════════════════════════════════════════════

    [Benchmark(Baseline = true, Description = "DIRECT: narrow")]
    [BenchmarkCategory("NarrowProjection")]
    public long Direct_Narrow()
    {
        using var reader = _sharcDb.Query("SELECT name FROM users_a WHERE age > 30");
        long count = 0;
        while (reader.Read()) { _ = reader.GetString(0); count++; }
        return count;
    }

    [Benchmark(Description = "CACHED: narrow")]
    [BenchmarkCategory("NarrowProjection")]
    public long Cached_Narrow()
    {
        using var reader = _sharcDb.Query("CACHED SELECT name FROM users_a WHERE age > 30");
        long count = 0;
        while (reader.Read()) { _ = reader.GetString(0); count++; }
        return count;
    }

    [Benchmark(Description = "JIT: narrow")]
    [BenchmarkCategory("NarrowProjection")]
    public long Jit_Narrow()
    {
        using var reader = _sharcDb.Query("JIT SELECT name FROM users_a WHERE age > 30");
        long count = 0;
        while (reader.Read()) { _ = reader.GetString(0); count++; }
        return count;
    }
}
