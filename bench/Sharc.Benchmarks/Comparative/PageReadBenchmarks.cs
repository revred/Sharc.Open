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
using Sharc.Core.Format;
using Sharc.Core.IO;

namespace Sharc.Benchmarks.Comparative;

/// <summary>
/// Compares page-level read operations:
///   Sharc: Direct span slice from in-memory buffer + BTreePageHeader.Parse (0 alloc for parse,
///          ushort[] only for cell pointers)
///   SQLite: Internal page cache accessed indirectly via SQL queries (interop + result boxing)
/// MemoryDiagnoser will show Sharc's per-page alloc is just the cell pointer array vs
/// SQLite's reader + command + result object allocations.
/// </summary>
[BenchmarkCategory("Comparative", "PageRead")]
[MemoryDiagnoser]
public class PageReadBenchmarks
{
    private byte[] _dbBytes = null!;
    private string _dbPath = null!;
    private SqliteConnection _conn = null!;
    private SqliteCommand _selectFirst = null!;
    private SqliteCommand _countRows = null!;
    private SqliteCommand _selectAll = null!;
    private SafeM2MPageSource _mmapSource = null!;
    private FilePageSource _fileSource = null!;
    private int _pageSize;
    private int _pageCount;

    [GlobalSetup]
    public void Setup()
    {
        var dir = Path.Combine(Path.GetTempPath(), "sharc_bench");
        Directory.CreateDirectory(dir);
        _dbPath = TestDatabaseGenerator.CreateCanonicalDatabase(dir);
        _dbBytes = File.ReadAllBytes(_dbPath);

        var header = DatabaseHeader.Parse(_dbBytes);
        _pageSize = header.PageSize;
        _pageCount = header.PageCount;

        _mmapSource = new SafeM2MPageSource(_dbPath);
        _fileSource = new FilePageSource(_dbPath);

        _conn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadOnly");
        _conn.Open();

        _selectFirst = _conn.CreateCommand();
        _selectFirst.CommandText = "SELECT id FROM users LIMIT 1";

        _countRows = _conn.CreateCommand();
        _countRows.CommandText = "SELECT COUNT(*) FROM users";

        _selectAll = _conn.CreateCommand();
        _selectAll.CommandText = "SELECT id, username, email, age, balance FROM users";
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        SqliteConnection.ClearAllPools();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _selectFirst?.Dispose();
        _countRows?.Dispose();
        _selectAll?.Dispose();
        _conn?.Dispose();
        _mmapSource?.Dispose();
        _fileSource?.Dispose();
    }

    // --- Sharc: span-based page access (zero-alloc parse, struct return) ---

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Sharc")]
    public BTreePageHeader Sharc_ReadPage1_ParseBTreeHeader()
    {
        // Page 1 b-tree header is at offset 100 (after the 100-byte database header)
        var pageSpan = _dbBytes.AsSpan(100, _pageSize - 100);
        return BTreePageHeader.Parse(pageSpan);
    }

    [Benchmark]
    [BenchmarkCategory("Sharc")]
    public BTreePageHeader Sharc_ReadPage2_ParseBTreeHeader()
    {
        var pageSpan = _dbBytes.AsSpan(_pageSize);
        return BTreePageHeader.Parse(pageSpan);
    }

    /// <summary>
    /// Sharc: parse + read cell pointers on page 1. Only allocation is ushort[cellCount].
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Sharc")]
    public ushort[] Sharc_ReadPage1_WithCellPointers()
    {
        var pageSpan = _dbBytes.AsSpan(100, _pageSize - 100);
        var btreeHeader = BTreePageHeader.Parse(pageSpan);
        return btreeHeader.ReadCellPointers(pageSpan);
    }

    /// <summary>
    /// Sharc: scan all pages, parse each b-tree header. Shows per-page cost.
    /// Allocation is 0 since BTreePageHeader is a struct (no cell pointer read here).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Sharc")]
    public int Sharc_ScanAllPages_ParseHeaders()
    {
        int totalCells = 0;
        // Page 1 starts at offset 100 (after db header)
        var page1Span = _dbBytes.AsSpan(100, _pageSize - 100);
        if (TryParseBTreeHeader(page1Span, out var page1Header))
            totalCells += page1Header.CellCount;

        for (int p = 1; p < _pageCount && (p + 1) * _pageSize <= _dbBytes.Length; p++)
        {
            var pageSpan = _dbBytes.AsSpan(p * _pageSize, _pageSize);
            if (TryParseBTreeHeader(pageSpan, out var hdr))
                totalCells += hdr.CellCount;
        }
        return totalCells;
    }

    [Benchmark]
    [BenchmarkCategory("Sharc")]
    public void Sharc_ReadAllPages_Transform()
    {
        var dest = new byte[_pageSize];
        for (int p = 0; p < _pageCount && (p + 1) * _pageSize <= _dbBytes.Length; p++)
        {
            int offset = p * _pageSize;
            var source = _dbBytes.AsSpan(offset, _pageSize);
            IdentityPageTransform.Instance.TransformRead(source, dest, (uint)(p + 1));
        }
    }

    // --- Sharc: memory-mapped page access (zero-copy from OS virtual memory) ---

    /// <summary>
    /// Sharc mmap: read page via IPageSource.GetPage (zero-copy span into mapped region).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Sharc", "Mmap")]
    public BTreePageHeader Sharc_Mmap_ReadPage1_ParseBTreeHeader()
    {
        var page1 = _mmapSource.GetPage(1);
        return BTreePageHeader.Parse(page1[100..]); // b-tree starts after 100-byte db header
    }

    [Benchmark]
    [BenchmarkCategory("Sharc", "Mmap")]
    public BTreePageHeader Sharc_Mmap_ReadPage2_ParseBTreeHeader()
    {
        var page2 = _mmapSource.GetPage(2);
        return BTreePageHeader.Parse(page2);
    }

    /// <summary>
    /// Sharc mmap: scan all pages via GetPage. Zero-copy span slices from mapped memory.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Sharc", "Mmap")]
    public int Sharc_Mmap_ScanAllPages_ParseHeaders()
    {
        int totalCells = 0;
        for (uint p = 1; p <= (uint)_mmapSource.PageCount; p++)
        {
            var page = _mmapSource.GetPage(p);
            var offset = p == 1 ? 100 : 0; // page 1 has 100-byte db header prefix
            if (TryParseBTreeHeader(page[offset..], out var hdr))
                totalCells += hdr.CellCount;
        }
        return totalCells;
    }

    // --- Sharc: FilePageSource (RandomAccess â€” one syscall per page read) ---

    /// <summary>
    /// Sharc file: read page via RandomAccess (one syscall into reusable buffer).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Sharc", "File")]
    public BTreePageHeader Sharc_File_ReadPage1_ParseBTreeHeader()
    {
        var page1 = _fileSource.GetPage(1);
        return BTreePageHeader.Parse(page1[100..]); // b-tree starts after 100-byte db header
    }

    [Benchmark]
    [BenchmarkCategory("Sharc", "File")]
    public BTreePageHeader Sharc_File_ReadPage2_ParseBTreeHeader()
    {
        var page2 = _fileSource.GetPage(2);
        return BTreePageHeader.Parse(page2);
    }

    /// <summary>
    /// Sharc file: scan all pages via RandomAccess. One syscall per page.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Sharc", "File")]
    public int Sharc_File_ScanAllPages_ParseHeaders()
    {
        int totalCells = 0;
        for (uint p = 1; p <= (uint)_fileSource.PageCount; p++)
        {
            var page = _fileSource.GetPage(p);
            var offset = p == 1 ? 100 : 0;
            if (TryParseBTreeHeader(page[offset..], out var hdr))
                totalCells += hdr.CellCount;
        }
        return totalCells;
    }

    private static bool TryParseBTreeHeader(ReadOnlySpan<byte> pageBytes, out BTreePageHeader header)
    {
        header = default;
        if (pageBytes.Length < SQLiteLayout.TableLeafHeaderSize)
            return false;

        byte pageType = pageBytes[0];
        if (pageType is not (0x02 or 0x05 or 0x0A or 0x0D))
            return false;

        header = BTreePageHeader.Parse(pageBytes);
        return true;
    }

    // --- SQLite: equivalent reads via SQL (interop + managed object allocations) ---

    [Benchmark]
    [BenchmarkCategory("SQLite")]
    public long SQLite_ReadFirstRow()
    {
        return (long)_selectFirst.ExecuteScalar()!;
    }

    [Benchmark]
    [BenchmarkCategory("SQLite")]
    public long SQLite_CountRows()
    {
        return (long)_countRows.ExecuteScalar()!;
    }

    /// <summary>
    /// SQLite: full table scan reading all rows. Shows reader + per-row allocation.
    /// This is the canonical "how much memory does a full scan cost?" benchmark.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("SQLite")]
    public long SQLite_FullTableScan()
    {
        long sum = 0;
        using var reader = _selectAll.ExecuteReader();
        while (reader.Read())
        {
            sum += reader.GetInt64(0);   // id
            _ = reader.GetString(1);      // username
            _ = reader.GetString(2);      // email
            sum += reader.GetInt32(3);    // age
            sum += (long)reader.GetDouble(4); // balance
        }
        return sum;
    }

    /// <summary>
    /// SQLite: scan only integer columns. Still allocates reader objects + interop buffers.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("SQLite")]
    public long SQLite_ScanIntegersOnly()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT id, age FROM users";
        long sum = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            sum += reader.GetInt64(0);
            sum += reader.GetInt32(1);
        }
        return sum;
    }
}
