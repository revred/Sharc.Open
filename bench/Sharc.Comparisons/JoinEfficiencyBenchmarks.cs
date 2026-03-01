using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using Microsoft.Data.Sqlite;
using Sharc;
using Sharc.Query;
using Sharc.Query.Execution;

namespace Sharc.Comparisons;

[Config(typeof(JoinStabilityConfig))]
[BenchmarkCategory("JoinEfficiency")]
public class JoinEfficiencyBenchmarks
{
    private string _dbPath = null!;
    private SharcDatabase _db = null!;

    [Params(1000, 5000)]
    public int UserCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"sharc_join_{Guid.NewGuid()}.db");
        JoinDataGenerator.Generate(_dbPath, UserCount, 2); // 2 orders per user, no indexes
        var dbBytes = File.ReadAllBytes(_dbPath);
        _db = SharcDatabase.OpenMemory(dbBytes, new SharcOpenOptions { PageCacheSize = 1000 });
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _db?.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { /* best-effort cleanup */ }
    }

    [Benchmark(Baseline = true)]
    public int Join_FullMaterialization()
    {
        var sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id";
        using var reader = _db.Query(sql);
        int count = 0;
        while (reader.Read())
        {
            count++;
        }
        return count;
    }

    [Benchmark]
    public int Join_WithSelectiveFilter()
    {
        var sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id < 100";
        using var reader = _db.Query(sql);
        int count = 0;
        while (reader.Read())
        {
            count++;
        }
        return count;
    }

    [Benchmark]
    public int FullOuterJoin_TieredHashJoin()
    {
        var sql = "SELECT u.name, o.amount FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id";
        using var reader = _db.Query(sql);
        int count = 0;
        while (reader.Read())
        {
            count++;
        }
        return count;
    }

    [Benchmark]
    public int LeftJoin_Baseline()
    {
        var sql = "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id";
        using var reader = _db.Query(sql);
        int count = 0;
        while (reader.Read())
        {
            count++;
        }
        return count;
    }
}

/// <summary>
/// Skewed-shape FULL OUTER JOIN benchmarks.
/// Exercises the three shapes that the all-matched baseline does not cover:
/// (1) unmatched-build heavy, (2) duplicate probe hot key, (3) null-key heavy.
/// </summary>
[Config(typeof(JoinStabilityConfig))]
[BenchmarkCategory("JoinShape")]
public class JoinShapeBenchmarks
{
    private SharcDatabase _unmatchedDb = null!;
    private SharcDatabase _hotKeyDb = null!;
    private SharcDatabase _nullKeyDb = null!;

    private string _unmatchedPath = null!;
    private string _hotKeyPath = null!;
    private string _nullKeyPath = null!;

    [Params(1000, 5000)]
    public int UserCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        // Shape 1: 90% of build rows are unmatched (only 10% of users have orders)
        _unmatchedPath = Path.Combine(Path.GetTempPath(), $"sharc_shape_unmatched_{Guid.NewGuid()}.db");
        JoinDataGenerator.GenerateSkewed(_unmatchedPath, UserCount,
            matchedFraction: 0.10, hotKeyCount: 0, nullKeyCount: 0);
        _unmatchedDb = SharcDatabase.OpenMemory(File.ReadAllBytes(_unmatchedPath),
            new SharcOpenOptions { PageCacheSize = 1000 });

        // Shape 2: 200 duplicate probe rows all targeting user_id = 1
        _hotKeyPath = Path.Combine(Path.GetTempPath(), $"sharc_shape_hotkey_{Guid.NewGuid()}.db");
        JoinDataGenerator.GenerateSkewed(_hotKeyPath, UserCount,
            matchedFraction: 0.50, hotKeyCount: 200, nullKeyCount: 0);
        _hotKeyDb = SharcDatabase.OpenMemory(File.ReadAllBytes(_hotKeyPath),
            new SharcOpenOptions { PageCacheSize = 1000 });

        // Shape 3: 20% of order rows have NULL user_id (never match)
        int nullCount = UserCount / 5;
        _nullKeyPath = Path.Combine(Path.GetTempPath(), $"sharc_shape_null_{Guid.NewGuid()}.db");
        JoinDataGenerator.GenerateSkewed(_nullKeyPath, UserCount,
            matchedFraction: 0.50, hotKeyCount: 0, nullKeyCount: nullCount);
        _nullKeyDb = SharcDatabase.OpenMemory(File.ReadAllBytes(_nullKeyPath),
            new SharcOpenOptions { PageCacheSize = 1000 });
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _unmatchedDb?.Dispose();
        _hotKeyDb?.Dispose();
        _nullKeyDb?.Dispose();
        try { if (File.Exists(_unmatchedPath)) File.Delete(_unmatchedPath); } catch { }
        try { if (File.Exists(_hotKeyPath)) File.Delete(_hotKeyPath); } catch { }
        try { if (File.Exists(_nullKeyPath)) File.Delete(_nullKeyPath); } catch { }
    }

    [Benchmark(Baseline = true)]
    public int FullOuter_UnmatchedBuildHeavy()
    {
        using var reader = _unmatchedDb.Query(
            "SELECT u.name, o.amount FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    public int FullOuter_DuplicateHotKey()
    {
        using var reader = _hotKeyDb.Query(
            "SELECT u.name, o.amount FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    public int FullOuter_NullKeyHeavy()
    {
        using var reader = _nullKeyDb.Query(
            "SELECT u.name, o.amount FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}

/// <summary>
/// Kernel-only join microbenchmark: calls TieredHashJoin.Execute directly with
/// pre-materialized QueryValue[] rows, isolating the join kernel from SQL parsing,
/// column resolution, and row materialization overhead.
/// </summary>
[Config(typeof(JoinStabilityConfig))]
[BenchmarkCategory("JoinKernel")]
public class JoinKernelBenchmarks
{
    private List<QueryValue[]> _buildRows = null!;
    private List<QueryValue[]> _probeRows = null!;

    [Params(1000, 5000)]
    public int BuildCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        // Build side: [key, payload] — all unique keys
        _buildRows = new List<QueryValue[]>(BuildCount);
        for (int i = 0; i < BuildCount; i++)
            _buildRows.Add([QueryValue.FromInt64(i), QueryValue.FromString($"b{i}")]);

        // Probe side: 80% match, 20% unmatched — 2 columns [key, payload]
        int probeCount = BuildCount;
        int matchedCount = (int)(probeCount * 0.8);
        _probeRows = new List<QueryValue[]>(probeCount);
        for (int i = 0; i < matchedCount; i++)
            _probeRows.Add([QueryValue.FromInt64(i), QueryValue.FromString($"p{i}")]);
        for (int i = 0; i < probeCount - matchedCount; i++)
            _probeRows.Add([QueryValue.FromInt64(BuildCount + i), QueryValue.FromString($"px{i}")]);
    }

    [Benchmark(Baseline = true)]
    public int Kernel_FullOuterJoin()
    {
        int count = 0;
        foreach (var row in TieredHashJoin.Execute(
            _buildRows, buildKeyIndex: 0, buildColumnCount: 2,
            _probeRows, probeKeyIndex: 0, probeColumnCount: 2,
            buildIsLeft: true, reuseBuffer: true))
        {
            count++;
        }
        return count;
    }

    [Benchmark]
    public int Kernel_FullOuterJoin_NoReuse()
    {
        int count = 0;
        foreach (var row in TieredHashJoin.Execute(
            _buildRows, buildKeyIndex: 0, buildColumnCount: 2,
            _probeRows, probeKeyIndex: 0, probeColumnCount: 2,
            buildIsLeft: true, reuseBuffer: false))
        {
            count++;
        }
        return count;
    }
}

/// <summary>
/// Benchmarks comparing index-accelerated WHERE queries (IndexSeekCursor) vs full table scans,
/// with SQLite comparative numbers.
/// </summary>
[MemoryDiagnoser]
[BenchmarkCategory("IndexAccelerated")]
public class IndexAcceleratedBenchmarks
{
    private string _dbPath = null!;
    private SharcDatabase _dbIndexed = null!;
    private SharcDatabase _dbNoIndex = null!;
    private SqliteConnection _sqliteConn = null!;
    private int _seekUserId;

    [Params(5000)]
    public int UserCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        // Create indexed database
        _dbPath = Path.Combine(Path.GetTempPath(), $"sharc_idx_{Guid.NewGuid()}.db");
        JoinDataGenerator.Generate(_dbPath, UserCount, 3, createIndexes: true);
        var dbBytes = File.ReadAllBytes(_dbPath);
        _dbIndexed = SharcDatabase.OpenMemory(dbBytes, new SharcOpenOptions { PageCacheSize = 1000 });

        // Create non-indexed version (same data, no indexes)
        var noIdxPath = Path.Combine(Path.GetTempPath(), $"sharc_noidx_{Guid.NewGuid()}.db");
        JoinDataGenerator.Generate(noIdxPath, UserCount, 3, createIndexes: false);
        var noIdxBytes = File.ReadAllBytes(noIdxPath);
        _dbNoIndex = SharcDatabase.OpenMemory(noIdxBytes, new SharcOpenOptions { PageCacheSize = 1000 });
        try { File.Delete(noIdxPath); } catch { }

        // SQLite connection (indexed database)
        _sqliteConn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadOnly;Pooling=False");
        _sqliteConn.Open();

        // Pick a user_id in the middle of the range for representative seeking
        _seekUserId = UserCount / 2;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _dbIndexed?.Dispose();
        _dbNoIndex?.Dispose();
        _sqliteConn?.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
    }

    // ── Index-accelerated point lookup: WHERE user_id = N on indexed column ──

    [Benchmark(Baseline = true)]
    public int Sharc_Where_IndexSeek()
    {
        using var reader = _dbIndexed.Query(
            $"SELECT id, user_id, amount FROM orders WHERE user_id = {_seekUserId}");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    public int Sharc_Where_FullScan()
    {
        using var reader = _dbNoIndex.Query(
            $"SELECT id, user_id, amount FROM orders WHERE user_id = {_seekUserId}");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    public int SQLite_Where_PointLookup()
    {
        using var cmd = _sqliteConn.CreateCommand();
        cmd.CommandText = $"SELECT id, user_id, amount FROM orders WHERE user_id = {_seekUserId}";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    // ── Text key index seek: WHERE dept = 'Engineering' on indexed text column ──

    [Benchmark]
    public int Sharc_WhereText_IndexSeek()
    {
        using var reader = _dbIndexed.Query(
            "SELECT id, name, dept FROM users WHERE dept = 'Engineering'");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    public int Sharc_WhereText_FullScan()
    {
        using var reader = _dbNoIndex.Query(
            "SELECT id, name, dept FROM users WHERE dept = 'Engineering'");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    public int SQLite_WhereText_Lookup()
    {
        using var cmd = _sqliteConn.CreateCommand();
        cmd.CommandText = "SELECT id, name, dept FROM users WHERE dept = 'Engineering'";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    // ── Unindexed column scan: WHERE status = 'Pending' (no index exists) ──

    [Benchmark]
    public int Sharc_WhereUnindexed_Scan()
    {
        using var reader = _dbIndexed.Query(
            "SELECT id, amount FROM orders WHERE status = 'Pending'");
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    public int SQLite_WhereUnindexed_Scan()
    {
        using var cmd = _sqliteConn.CreateCommand();
        cmd.CommandText = "SELECT id, amount FROM orders WHERE status = 'Pending'";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }
}
