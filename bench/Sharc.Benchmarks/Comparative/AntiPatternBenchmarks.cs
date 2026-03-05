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
using Sharc.Benchmarks.Helpers;
using Sharc.Core;

namespace Sharc.Benchmarks.Comparative;

/// <summary>
/// ╔══════════════════════════════════════════════════════════════════════════════╗
/// ║  ANTI-PATTERN BENCHMARKS: What NOT to do with Sharc                        ║
/// ║                                                                            ║
/// ║  These benchmarks make sub-optimal API usage painfully obvious by          ║
/// ║  contrasting fast vs slow paths side-by-side.                              ║
/// ║                                                                            ║
/// ║  Key anti-patterns measured:                                               ║
/// ║    1. Auto-commit per row  vs  Explicit transaction batch                  ║
/// ║    2. Open/close per write vs  Reuse SharcWriter                           ║
/// ║    3. Ad-hoc writer        vs  PreparedWriter                              ║
/// ║                                                                            ║
/// ║  Run: dotnet run -c Release --project bench/Sharc.Benchmarks              ║
/// ║       -- --filter *AntiPattern*                                            ║
/// ╚══════════════════════════════════════════════════════════════════════════════╝
/// </summary>
[BenchmarkCategory("Comparative", "AntiPattern")]
[MemoryDiagnoser]
public class AntiPatternBenchmarks
{
    private string _dbPath = null!;
    private byte[] _dbBytes = null!;
    private const int BatchSize = 100;

    [GlobalSetup]
    public void Setup()
    {
        var dir = Path.Combine(Path.GetTempPath(), "sharc_bench_antipattern");
        Directory.CreateDirectory(dir);
        _dbPath = TestDatabaseGenerator.CreateCanonicalDatabase(dir);
        _dbBytes = File.ReadAllBytes(_dbPath);
    }

    [IterationSetup]
    public void RestoreDatabase()
    {
        File.WriteAllBytes(_dbPath, _dbBytes);
    }

    private static ColumnValue[] MakeEventRow(int i) =>
    [
        ColumnValue.FromInt64(4, 1 + (i % 10000)),   // user_id
        ColumnValue.FromInt64(1, 1 + (i % 50)),       // event_type
        ColumnValue.FromInt64(4, 1700000000 + i),     // timestamp
        ColumnValue.FromDouble(i * 1.1),              // value
    ];

    // ═══════════════════════════════════════════════════════════════
    //  ANTI-PATTERN 1: Auto-commit per row (N transactions)
    //  vs CORRECT: Explicit transaction (1 transaction)
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// SLOW: Each Insert() auto-commits — creates 100 separate transactions.
    /// Each transaction: acquire shadow → journal write → B-tree mutate → commit → release.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("TxBatching")]
    public int SLOW_AutoCommit_PerRow()
    {
        using var writer = SharcWriter.Open(_dbPath);
        for (int i = 0; i < BatchSize; i++)
            writer.Insert("events", MakeEventRow(i));
        return BatchSize;
    }

    /// <summary>
    /// FAST: All 100 inserts share one transaction — 1 journal write, 1 commit.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("TxBatching")]
    public int FAST_ExplicitTransaction_Batch()
    {
        using var writer = SharcWriter.Open(_dbPath);
        using var tx = writer.BeginTransaction();
        for (int i = 0; i < BatchSize; i++)
            tx.Insert("events", MakeEventRow(i));
        tx.Commit();
        return BatchSize;
    }

    // ═══════════════════════════════════════════════════════════════
    //  ANTI-PATTERN 2: Open/close SharcWriter per write
    //  vs CORRECT: Reuse one SharcWriter
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// SLOW: Opens and closes SharcWriter for every single row.
    /// Each Open() parses the database header, allocates a page cache, and resolves schema.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("WriterReuse")]
    public int SLOW_OpenClose_PerRow()
    {
        for (int i = 0; i < BatchSize; i++)
        {
            using var writer = SharcWriter.Open(_dbPath);
            writer.Insert("events", MakeEventRow(i));
        }
        return BatchSize;
    }

    /// <summary>
    /// FAST: One SharcWriter, one transaction, all rows.
    /// Schema resolved once, shadow page source pooled.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("WriterReuse")]
    public int FAST_SingleWriter_Batch()
    {
        using var writer = SharcWriter.Open(_dbPath);
        using var tx = writer.BeginTransaction();
        for (int i = 0; i < BatchSize; i++)
            tx.Insert("events", MakeEventRow(i));
        tx.Commit();
        return BatchSize;
    }

    // ═══════════════════════════════════════════════════════════════
    //  ANTI-PATTERN 3: Ad-hoc Insert vs PreparedWriter
    //  (Both are correct; PreparedWriter is the fast path)
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// GOOD: Using BeginTransaction + tx.Insert.
    /// Table schema is resolved once, root page is cached.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("PreparedVsAdHoc")]
    public int GOOD_Transaction_Insert()
    {
        using var writer = SharcWriter.Open(_dbPath);
        using var tx = writer.BeginTransaction();
        for (int i = 0; i < BatchSize; i++)
            tx.Insert("events", MakeEventRow(i));
        tx.Commit();
        return BatchSize;
    }

    /// <summary>
    /// BETTER: Using InsertBatch — single method call, internal transaction.
    /// Avoids per-call overhead of TryGetTableInfo.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("PreparedVsAdHoc")]
    public int BETTER_InsertBatch()
    {
        using var writer = SharcWriter.Open(_dbPath);
        var rows = new ColumnValue[BatchSize][];
        for (int i = 0; i < BatchSize; i++)
            rows[i] = MakeEventRow(i);
        writer.InsertBatch("events", rows);
        return BatchSize;
    }

    // ═══════════════════════════════════════════════════════════════
    //  RECOMMENDED: WriteScope — RAII auto-commit, zero boilerplate
    //  Disposal is guaranteed, cannot forget 'using'.
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// BEST (Lambda): WriteScope guarantees disposal. Cannot leak scope.
    /// Auto-commits on success, rolls back on exception.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("WriteScope")]
    public int BEST_WriteScope_Lambda()
    {
        return SharcWriter.WriteScope(_dbPath, scope =>
        {
            for (int i = 0; i < BatchSize; i++)
                scope.Insert("events", MakeEventRow(i));
            return BatchSize;
        });
    }

    /// <summary>
    /// BEST (using): WriteScope with explicit 'using' for multi-batch
    /// ingestion with periodic Flush checkpoints.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("WriteScope")]
    public int BEST_WriteScope_Flush()
    {
        using var scope = SharcWriter.OpenScope(_dbPath);
        for (int i = 0; i < BatchSize / 2; i++)
            scope.Insert("events", MakeEventRow(i));
        scope.Flush(); // first batch committed to disk

        for (int i = BatchSize / 2; i < BatchSize; i++)
            scope.Insert("events", MakeEventRow(i));
        // second batch auto-commits on Dispose
        return BatchSize;
    }

    // ═══════════════════════════════════════════════════════════════
    //  Mixed: Update 100 rows — auto-commit vs batched
    // ═══════════════════════════════════════════════════════════════

    private static readonly ColumnValue[] UpdateRow =
    [
        ColumnValue.FromInt64(4, 9999),
        ColumnValue.FromInt64(1, 50),
        ColumnValue.FromInt64(4, 1700099999),
        ColumnValue.FromDouble(999.99),
    ];

    /// <summary>
    /// SLOW: 100 separate auto-commit Update calls.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("UpdateBatching")]
    public int SLOW_AutoCommit_Update100()
    {
        using var writer = SharcWriter.Open(_dbPath);
        int count = 0;
        for (long rowId = 2000; rowId < 2100; rowId++)
        {
            if (writer.Update("events", rowId, UpdateRow))
                count++;
        }
        return count;
    }

    /// <summary>
    /// FAST: 100 updates in one explicit transaction.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("UpdateBatching")]
    public int FAST_Transaction_Update100()
    {
        using var writer = SharcWriter.Open(_dbPath);
        int count = 0;
        using var tx = writer.BeginTransaction();
        for (long rowId = 2000; rowId < 2100; rowId++)
        {
            if (tx.Update("events", rowId, UpdateRow))
                count++;
        }
        tx.Commit();
        return count;
    }
}
