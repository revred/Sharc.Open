// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Buffers.Binary;
using System.Text;
using Sharc.Core;
using Sharc.Core.BTree;
using Sharc.Core.Format;
using Sharc.Core.IO;
using Sharc.Core.Records;
using Xunit;

namespace Sharc.Tests.Write;

/// <summary>
/// Stress tests for <see cref="BTreeMutator"/> — pushes insert, delete, and update
/// operations to their limits with large row counts, wide rows, and interleaved mutations.
/// </summary>
public sealed class BTreeMutatorStressTests
{
    private const int PageSize = 4096;
    private const int UsableSize = PageSize;

    private static byte[] CreateMinimalDb()
    {
        var data = new byte[PageSize * 2];
        "SQLite format 3\0"u8.CopyTo(data);
        BinaryPrimitives.WriteUInt16BigEndian(data.AsSpan(16), PageSize);
        data[18] = 1; data[19] = 1;
        BinaryPrimitives.WriteUInt32BigEndian(data.AsSpan(28), 2);
        BinaryPrimitives.WriteUInt32BigEndian(data.AsSpan(40), 1);
        BinaryPrimitives.WriteUInt32BigEndian(data.AsSpan(44), 4);

        // sqlite_master entry for "test(id INTEGER, val TEXT)"
        string sql = "CREATE TABLE test(id INTEGER, val TEXT)";
        var sqlBytes = Encoding.UTF8.GetBytes(sql);
        var cols = new ColumnValue[5];
        cols[0] = ColumnValue.Text(2 * 5 + 13, "table"u8.ToArray());
        cols[1] = ColumnValue.Text(2 * 4 + 13, "test"u8.ToArray());
        cols[2] = ColumnValue.Text(2 * 4 + 13, "test"u8.ToArray());
        cols[3] = ColumnValue.FromInt64(1, 2);
        cols[4] = ColumnValue.Text(2 * sqlBytes.Length + 13, sqlBytes);

        int recordSize = RecordEncoder.ComputeEncodedSize(cols);
        Span<byte> recordBuf = stackalloc byte[recordSize];
        RecordEncoder.EncodeRecord(cols, recordBuf);
        int cellSize = CellBuilder.ComputeTableLeafCellSize(1, recordSize, UsableSize);
        Span<byte> cellBuf = stackalloc byte[cellSize];
        CellBuilder.BuildTableLeafCell(1, recordBuf, cellBuf, UsableSize);

        int pageHdrOff = 100;
        ushort cellContentOff = (ushort)(PageSize - cellSize);
        var masterHdr = new BTreePageHeader(BTreePageType.LeafTable, 0, 1, cellContentOff, 0, 0);
        BTreePageHeader.Write(data.AsSpan(pageHdrOff), masterHdr);
        BinaryPrimitives.WriteUInt16BigEndian(data.AsSpan(pageHdrOff + 8), cellContentOff);
        cellBuf.CopyTo(data.AsSpan(cellContentOff));

        int page2Off = PageSize;
        var tableHdr = new BTreePageHeader(BTreePageType.LeafTable, 0, 0, (ushort)UsableSize, 0, 0);
        BTreePageHeader.Write(data.AsSpan(page2Off), tableHdr);

        return data;
    }

    private static byte[] MakeRecord(long id, string value)
    {
        var cols = new ColumnValue[]
        {
            ColumnValue.FromInt64(1, id),
            ColumnValue.Text(2 * Encoding.UTF8.GetByteCount(value) + 13, Encoding.UTF8.GetBytes(value)),
        };
        int size = RecordEncoder.ComputeEncodedSize(cols);
        var buf = new byte[size];
        RecordEncoder.EncodeRecord(cols, buf);
        return buf;
    }

    private static int CountRows(IPageSource source, uint rootPage)
    {
        using var cursor = new BTreeCursor<IPageSource>(source, rootPage, UsableSize);
        int count = 0;
        while (cursor.MoveNext()) count++;
        return count;
    }

    [Fact]
    public void Insert500_AllRowsAccessible()
    {
        var data = CreateMinimalDb();
        var source = new MemoryPageSource(data);
        using var mutator = new BTreeMutator(source, UsableSize);

        uint root = 2;
        for (int i = 1; i <= 500; i++)
        {
            root = mutator.Insert(root, i, MakeRecord(i, $"val_{i:D5}"));
        }

        Assert.Equal(500, CountRows(source, root));
    }

    [Fact]
    public void Insert_ThenDeleteAll_TreeIsEmpty()
    {
        var data = CreateMinimalDb();
        var source = new MemoryPageSource(data);
        using var mutator = new BTreeMutator(source, UsableSize);

        uint root = 2;
        for (int i = 1; i <= 100; i++)
        {
            root = mutator.Insert(root, i, MakeRecord(i, $"val_{i}"));
        }
        Assert.Equal(100, CountRows(source, root));

        // Delete all rows in reverse order
        for (int i = 100; i >= 1; i--)
        {
            var (found, newRoot) = mutator.Delete(root, i);
            Assert.True(found);
            root = newRoot;
        }
        Assert.Equal(0, CountRows(source, root));
    }

    [Fact]
    public void Insert_ThenDeleteOdds_EvensRemain()
    {
        var data = CreateMinimalDb();
        var source = new MemoryPageSource(data);
        using var mutator = new BTreeMutator(source, UsableSize);

        uint root = 2;
        for (int i = 1; i <= 200; i++)
        {
            root = mutator.Insert(root, i, MakeRecord(i, $"val_{i}"));
        }

        // Delete odd rowids
        for (int i = 1; i <= 200; i += 2)
        {
            var (found, newRoot) = mutator.Delete(root, i);
            Assert.True(found);
            root = newRoot;
        }

        Assert.Equal(100, CountRows(source, root));

        // Verify all remaining rows have even rowids
        using var cursor = new BTreeCursor<MemoryPageSource>(source, root, UsableSize);
        while (cursor.MoveNext())
        {
            Assert.Equal(0, cursor.RowId % 2);
        }
    }

    [Fact]
    public void InterleavedInsertDelete_DataConsistent()
    {
        var data = CreateMinimalDb();
        var source = new MemoryPageSource(data);
        using var mutator = new BTreeMutator(source, UsableSize);

        uint root = 2;
        // Insert 1-100
        for (int i = 1; i <= 100; i++)
            root = mutator.Insert(root, i, MakeRecord(i, $"v{i}"));

        // Delete 1-50, then insert 101-150
        for (int i = 1; i <= 50; i++)
        {
            var (found, newRoot) = mutator.Delete(root, i);
            Assert.True(found);
            root = newRoot;
        }
        for (int i = 101; i <= 150; i++)
            root = mutator.Insert(root, i, MakeRecord(i, $"v{i}"));

        // Should have rows 51-150 = 100 rows
        Assert.Equal(100, CountRows(source, root));
    }

    [Fact]
    public void UpdateAllRows_ValuesChanged()
    {
        var data = CreateMinimalDb();
        var source = new MemoryPageSource(data);
        using var mutator = new BTreeMutator(source, UsableSize);

        uint root = 2;
        for (int i = 1; i <= 100; i++)
            root = mutator.Insert(root, i, MakeRecord(i, $"old_{i}"));

        // Update every row with new value
        for (int i = 1; i <= 100; i++)
        {
            var (found, newRoot) = mutator.Update(root, i, MakeRecord(i, $"new_{i}"));
            Assert.True(found);
            root = newRoot;
        }

        // Verify all values are updated
        var decoder = new RecordDecoder();
        var colBuf = new ColumnValue[2];
        using var cursor = new BTreeCursor<MemoryPageSource>(source, root, UsableSize);
        int count = 0;
        while (cursor.MoveNext())
        {
            decoder.DecodeRecord(cursor.Payload, colBuf);
            string val = colBuf[1].AsString();
            Assert.StartsWith("new_", val);
            count++;
        }
        Assert.Equal(100, count);
    }

    [Fact]
    public void WideRows_CauseOverflow_StillReadable()
    {
        var data = CreateMinimalDb();
        var source = new MemoryPageSource(data);
        using var mutator = new BTreeMutator(source, UsableSize);

        uint root = 2;
        // Insert rows with ~2KB values — forces overflow pages
        for (int i = 1; i <= 20; i++)
        {
            string bigVal = new string('X', 2000) + $"_{i}";
            root = mutator.Insert(root, i, MakeRecord(i, bigVal));
        }

        // Verify all rows readable with correct content
        var decoder = new RecordDecoder();
        var colBuf = new ColumnValue[2];
        using var cursor = new BTreeCursor<MemoryPageSource>(source, root, UsableSize);
        int count = 0;
        while (cursor.MoveNext())
        {
            decoder.DecodeRecord(cursor.Payload, colBuf);
            string val = colBuf[1].AsString();
            Assert.StartsWith(new string('X', 100), val);
            count++;
        }
        Assert.Equal(20, count);
    }

    [Fact]
    public void DeleteNonExistent_ReturnsFalse()
    {
        var data = CreateMinimalDb();
        var source = new MemoryPageSource(data);
        using var mutator = new BTreeMutator(source, UsableSize);

        uint root = 2;
        root = mutator.Insert(root, 1, MakeRecord(1, "only"));

        var (found, newRoot) = mutator.Delete(root, 999);
        Assert.False(found);
        Assert.Equal(1, CountRows(source, newRoot));
    }

    [Fact]
    public void UpdateNonExistent_ReturnsFalse()
    {
        var data = CreateMinimalDb();
        var source = new MemoryPageSource(data);
        using var mutator = new BTreeMutator(source, UsableSize);

        uint root = 2;
        root = mutator.Insert(root, 1, MakeRecord(1, "only"));

        var (found, newRoot) = mutator.Update(root, 999, MakeRecord(999, "sample"));
        Assert.False(found);

        // Original row unchanged
        var decoder = new RecordDecoder();
        var colBuf = new ColumnValue[2];
        using var cursor = new BTreeCursor<MemoryPageSource>(source, newRoot, UsableSize);
        Assert.True(cursor.MoveNext());
        decoder.DecodeRecord(cursor.Payload, colBuf);
        Assert.Equal("only", colBuf[1].AsString());
    }
}
