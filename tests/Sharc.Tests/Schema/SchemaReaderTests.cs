// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.


/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Licensed under the MIT License — free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using System.Text;
using Sharc.Core;
using Sharc.Core.Query;
using Sharc.Core.Schema;
using Xunit;

namespace Sharc.Tests.Schema;

/// <summary>
/// Unit tests for SchemaReader — parses sqlite_schema (page 1) into SharcSchema.
/// Uses fake IBTreeReader and IRecordDecoder to feed synthetic rows.
/// </summary>
public class SchemaReaderTests
{
    // ── Single table ──

    [Fact]
    public void ReadSchema_CachesResult_ReturnsSameInstance()
    {
        var rows = new[]
        {
            MakeSchemaRow("table", "users", "users", 2,
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        };

        var reader = CreateSchemaReader(rows);
        
        // First read - clears cache and populates it
        var firstSchema = reader.ReadSchema();
        
        // Second read - should return exactly the same object instance
        var secondSchema = reader.ReadSchema();
        
        Assert.Same(firstSchema, secondSchema);
        
        // Verify it actually has data
        Assert.Contains(firstSchema.Tables, t => t.Name == "users");
    }

    [Fact]
    public void ReadSchema_SingleTable_ParsesTableInfo()
    {
        var rows = new[]
        {
            MakeSchemaRow("table", "users", "users", 2,
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        };

        var reader = CreateSchemaReader(rows);
        var schema = reader.ReadSchema();

        var tables = schema.Tables.Where(t => t.Name != "sqlite_master").ToList();
        Assert.Single(tables);
        var table = tables[0];
        Assert.Equal("users", table.Name);
        Assert.Equal(2, table.RootPage);
        Assert.Contains("users", table.Sql);
        Assert.False(table.IsWithoutRowId);
        Assert.True(table.Columns.Count >= 1);
    }

    // ── WITHOUT ROWID detection ──

    [Fact]
    public void ReadSchema_WithoutRowId_DetectedCorrectly()
    {
        var rows = new[]
        {
            MakeSchemaRow("table", "kv", "kv", 3,
                "CREATE TABLE kv (key TEXT PRIMARY KEY, val BLOB) WITHOUT ROWID")
        };

        var reader = CreateSchemaReader(rows);
        var schema = reader.ReadSchema();

        var tables = schema.Tables.Where(t => t.Name != "sqlite_master").ToList();
        Assert.Single(tables);
        Assert.True(tables[0].IsWithoutRowId);
    }

    // ── Single index ──

    [Fact]
    public void ReadSchema_SingleIndex_ParsesIndexInfo()
    {
        var rows = new[]
        {
            MakeSchemaRow("index", "idx_name", "users", 4,
                "CREATE INDEX idx_name ON users (name)")
        };

        var reader = CreateSchemaReader(rows);
        var schema = reader.ReadSchema();

        Assert.Empty(schema.Tables.Where(t => t.Name != "sqlite_master"));
        Assert.Single(schema.Indexes);
        var index = schema.Indexes[0];
        Assert.Equal("idx_name", index.Name);
        Assert.Equal("users", index.TableName);
        Assert.Equal(4, index.RootPage);
        Assert.False(index.IsUnique);
    }

    // ── UNIQUE index ──

    [Fact]
    public void ReadSchema_UniqueIndex_IsUniqueFlagSet()
    {
        var rows = new[]
        {
            MakeSchemaRow("index", "idx_email", "users", 5,
                "CREATE UNIQUE INDEX idx_email ON users (email)")
        };

        var reader = CreateSchemaReader(rows);
        var schema = reader.ReadSchema();

        Assert.Single(schema.Indexes);
        Assert.True(schema.Indexes[0].IsUnique);
    }

    // ── View ──

    [Fact]
    public void ReadSchema_SingleView_ParsesViewInfo()
    {
        var rows = new[]
        {
            MakeSchemaRow("view", "active_users", "active_users", 0,
                "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1")
        };

        var reader = CreateSchemaReader(rows);
        var schema = reader.ReadSchema();

        Assert.Empty(schema.Tables.Where(t => t.Name != "sqlite_master"));
        Assert.Empty(schema.Indexes);
        Assert.Single(schema.Views);
        Assert.Equal("active_users", schema.Views[0].Name);
        Assert.Contains("SELECT", schema.Views[0].Sql);
    }

    // ── Mixed schema rows ──

    [Fact]
    public void ReadSchema_MixedTypes_AllParsedCorrectly()
    {
        var rows = new[]
        {
            MakeSchemaRow("table", "t1", "t1", 2, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)"),
            MakeSchemaRow("index", "i1", "t1", 3, "CREATE INDEX i1 ON t1 (id)"),
            MakeSchemaRow("view", "v1", "v1", 0, "CREATE VIEW v1 AS SELECT 1"),
            MakeSchemaRow("table", "t2", "t2", 4, "CREATE TABLE t2 (x TEXT)"),
        };

        var reader = CreateSchemaReader(rows);
        var schema = reader.ReadSchema();

        Assert.Equal(2, schema.Tables.Count(t => t.Name != "sqlite_master"));
        Assert.Single(schema.Indexes);
        Assert.Single(schema.Views);
    }

    // ── Short row (< 5 columns) is skipped ──

    [Fact]
    public void ReadSchema_ShortRow_Skipped()
    {
        // Row with only 3 columns should be ignored
        var shortRow = new ColumnValue[]
        {
            MakeText("table"),
            MakeText("bad"),
            MakeText("bad"),
        };

        var rows = new[]
        {
            shortRow,
            MakeSchemaRow("table", "good", "good", 2, "CREATE TABLE good (id INTEGER PRIMARY KEY)")
        };

        var reader = CreateSchemaReader(rows);
        var schema = reader.ReadSchema();

        var tables = schema.Tables.Where(t => t.Name != "sqlite_master").ToList();
        Assert.Single(tables);
        Assert.Equal("good", tables[0].Name);
    }

    // ── NULL sql for table is skipped ──

    [Fact]
    public void ReadSchema_NullSqlForTable_SkipsTable()
    {
        var rows = new[]
        {
            new ColumnValue[]
            {
                MakeText("table"),
                MakeText("internal_tbl"),
                MakeText("internal_tbl"),
                ColumnValue.FromInt64(1, 5),
                ColumnValue.Null()
            }
        };

        var reader = CreateSchemaReader(rows);
        var schema = reader.ReadSchema();

        // Table with null SQL is skipped
        Assert.Empty(schema.Tables.Where(t => t.Name != "sqlite_master"));
    }

    // ── Index with null sql still creates IndexInfo ──

    [Fact]
    public void ReadSchema_IndexWithNullSql_CreatesIndexWithEmptySql()
    {
        var rows = new[]
        {
            new ColumnValue[]
            {
                MakeText("index"),
                MakeText("autoindex_1"),
                MakeText("t1"),
                ColumnValue.FromInt64(1, 6),
                ColumnValue.Null()
            }
        };

        var reader = CreateSchemaReader(rows);
        var schema = reader.ReadSchema();

        Assert.Single(schema.Indexes);
        Assert.Equal("", schema.Indexes[0].Sql);
        Assert.False(schema.Indexes[0].IsUnique);
    }

    // ── Unknown type (e.g. "trigger") is silently ignored ──

    [Fact]
    public void ReadSchema_UnknownType_IgnoredSilently()
    {
        var rows = new[]
        {
            MakeSchemaRow("trigger", "trg1", "t1", 0, "CREATE TRIGGER trg1 ..."),
            MakeSchemaRow("table", "t1", "t1", 2, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
        };

        var reader = CreateSchemaReader(rows);
        var schema = reader.ReadSchema();

        var tables = schema.Tables.Where(t => t.Name != "sqlite_master").ToList();
        Assert.Single(tables);
        Assert.Empty(schema.Indexes);
        Assert.Empty(schema.Views);
    }

    // ── Helpers ──

    private static SchemaReader CreateSchemaReader(ColumnValue[][] rows)
    {
        var fakeCursor = new FakeBTreeCursor(rows);
        var fakeReader = new FakeBTreeReader(fakeCursor);
        var passThruDecoder = new PassThroughRecordDecoder(rows);
        return new SchemaReader(fakeReader, passThruDecoder);
    }

    private static ColumnValue[] MakeSchemaRow(string type, string name, string tblName,
        int rootPage, string sql) =>
    [
        MakeText(type),
        MakeText(name),
        MakeText(tblName),
        ColumnValue.FromInt64(1, rootPage),
        MakeText(sql),
    ];

    private static ColumnValue MakeText(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        long serialType = bytes.Length * 2 + 13; // text serial type formula
        return ColumnValue.Text(serialType, bytes);
    }

    // ── Fakes ──

    /// <summary>
    /// Fake cursor that yields pre-built payloads (we don't actually decode from raw bytes).
    /// The PassThroughRecordDecoder maps the payload position to the original ColumnValue[].
    /// </summary>
    private sealed class FakeBTreeCursor : IBTreeCursor
    {
        private readonly ColumnValue[][] _rows;
        private int _index = -1;
        private readonly byte[] _dummyPayload;

        public FakeBTreeCursor(ColumnValue[][] rows)
        {
            _rows = rows;
            _dummyPayload = new byte[8]; // dummy; the decoder ignores it
        }

        public bool MoveNext()
        {
            _index++;
            return _index < _rows.Length;
        }

        public void Reset() => _index = -1;

        public bool MoveLast()
        {
            if (_rows.Length == 0) return false;
            _index = _rows.Length - 1;
            return true;
        }

        public bool Seek(long rowId) => false;
        public long RowId => _index;
        public ReadOnlySpan<byte> Payload
        {
            get
            {
                // Encode the row index into the first 4 bytes so the decoder can look it up
                BitConverter.TryWriteBytes(_dummyPayload, _index);
                return _dummyPayload;
            }
        }
        public int PayloadSize => _dummyPayload.Length;
        public bool IsStale => false;
        public void Dispose() { }
    }

    private sealed class FakeBTreeReader : IBTreeReader
    {
        private readonly IBTreeCursor _cursor;
        public FakeBTreeReader(IBTreeCursor cursor) => _cursor = cursor;
        public IBTreeCursor CreateCursor(uint rootPage) => _cursor;
        public IIndexBTreeCursor CreateIndexCursor(uint rootPage) =>
            throw new NotSupportedException();
    }

    /// <summary>
    /// Decodes the "payload" by extracting the row index from the dummy bytes
    /// and returning the pre-built ColumnValue[] array.
    /// </summary>
    private sealed class PassThroughRecordDecoder : IRecordDecoder
    {
        public bool Matches(ReadOnlySpan<byte> payload, ResolvedFilter[] filters, long rowId, int rowidAliasOrdinal) => true;
        private readonly ColumnValue[][] _rows;
        public PassThroughRecordDecoder(ColumnValue[][] rows) => _rows = rows;

        public ColumnValue[] DecodeRecord(ReadOnlySpan<byte> payload)
        {
            int index = BitConverter.ToInt32(payload);
            return _rows[index];
        }

        public void DecodeRecord(ReadOnlySpan<byte> payload, ColumnValue[] destination)
        {
            var values = DecodeRecord(payload);
            values.AsSpan().CopyTo(destination);
        }
        public int GetColumnCount(ReadOnlySpan<byte> payload) => throw new NotSupportedException();
        public ColumnValue DecodeColumn(ReadOnlySpan<byte> payload, int columnIndex) =>
            throw new NotSupportedException();
        public int ReadSerialTypes(ReadOnlySpan<byte> payload, long[] serialTypes, out int bodyOffset)
        {
            bodyOffset = 0;
            throw new NotSupportedException();
        }

        public void DecodeRecord(ReadOnlySpan<byte> payload, ColumnValue[] destination, ReadOnlySpan<long> serialTypes, int bodyOffset)
        {
            DecodeRecord(payload, destination);
        }

        public ColumnValue DecodeColumn(ReadOnlySpan<byte> payload, int columnIndex, ReadOnlySpan<long> serialTypes, int bodyOffset)
        {
            return DecodeColumn(payload, columnIndex);
        }

        public int ReadSerialTypes(ReadOnlySpan<byte> payload, Span<long> serialTypes, out int bodyOffset)
        {
            bodyOffset = 0;
            throw new NotSupportedException();
        }

        public string DecodeStringDirect(ReadOnlySpan<byte> payload, int columnIndex, ReadOnlySpan<long> serialTypes, int bodyOffset) => throw new NotSupportedException();
        public long DecodeInt64Direct(ReadOnlySpan<byte> payload, int columnIndex, ReadOnlySpan<long> serialTypes, int bodyOffset) => throw new NotSupportedException();
        public double DecodeDoubleDirect(ReadOnlySpan<byte> payload, int columnIndex, ReadOnlySpan<long> serialTypes, int bodyOffset) => throw new NotSupportedException();
        public ColumnValue DecodeColumnAt(ReadOnlySpan<byte> payload, long serialType, int columnOffset) => throw new NotSupportedException();
        public long DecodeInt64At(ReadOnlySpan<byte> payload, long serialType, int columnOffset) => throw new NotSupportedException();
        public double DecodeDoubleAt(ReadOnlySpan<byte> payload, long serialType, int columnOffset) => throw new NotSupportedException();
        public string DecodeStringAt(ReadOnlySpan<byte> payload, long serialType, int columnOffset) => throw new NotSupportedException();
        public void ComputeColumnOffsets(ReadOnlySpan<long> serialTypes, int columnCount, int bodyOffset, Span<int> offsets) => throw new NotSupportedException();
        public bool TryDecodeIndexRecord(ReadOnlySpan<byte> payload, ColumnValue[] keys, int keyCount, out long trailingRowId) => throw new NotSupportedException();
    }
}