// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Buffers;

namespace Sharc.Core.Schema;

/// <summary>
/// Reads the sqlite_schema table (page 1) and builds a <see cref="SharcSchema"/>.
/// </summary>
internal sealed class SchemaReader
{
    /// <summary>
    /// Cached sqlite_master column definitions — shared across all SchemaReader instances
    /// to avoid allocating the same 5 ColumnInfo objects on every schema read.
    /// </summary>
    private static readonly IReadOnlyList<ColumnInfo> SqliteMasterColumns =
    [
        new() { Name = "type", DeclaredType = "TEXT", Ordinal = 0, IsPrimaryKey = false, IsNotNull = false },
        new() { Name = "name", DeclaredType = "TEXT", Ordinal = 1, IsPrimaryKey = false, IsNotNull = false },
        new() { Name = "tbl_name", DeclaredType = "TEXT", Ordinal = 2, IsPrimaryKey = false, IsNotNull = false },
        new() { Name = "rootpage", DeclaredType = "INTEGER", Ordinal = 3, IsPrimaryKey = false, IsNotNull = false },
        new() { Name = "sql", DeclaredType = "TEXT", Ordinal = 4, IsPrimaryKey = false, IsNotNull = false }
    ];

    private readonly IBTreeReader _bTreeReader;
    private readonly IRecordDecoder _recordDecoder;

    public SchemaReader(IBTreeReader bTreeReader, IRecordDecoder recordDecoder)
    {
        _bTreeReader = bTreeReader;
        _recordDecoder = recordDecoder;
    }

    private SharcSchema? _cachedSchema;

    /// <summary>
    /// Reads the sqlite_schema from page 1 and builds a full schema.
    /// </summary>
    /// <returns>The parsed database schema.</returns>
    public SharcSchema ReadSchema()
    {
        if (_cachedSchema != null) return _cachedSchema;

        var columnBuffer = ArrayPool<ColumnValue>.Shared.Rent(5);
        try
        {
            _cachedSchema = ReadSchemaCore(columnBuffer);
            return _cachedSchema;
        }
        finally
        {
            ArrayPool<ColumnValue>.Shared.Return(columnBuffer, clearArray: true);
        }
    }

    private SharcSchema ReadSchemaCore(ColumnValue[] _columnBuffer)
    {
        // Pre-size collections based on typical schema sizes to avoid List resizing.
        // sqlite_master entry is always first; most databases have 5-15 user tables.
        var tables = new List<TableInfo>(8)
        {
            new TableInfo
            {
                Name = "sqlite_master",
                RootPage = 1,
                Sql = "CREATE TABLE sqlite_master(type TEXT, name TEXT, tbl_name TEXT, rootpage INTEGER, sql TEXT)",
                Columns = SqliteMasterColumns,
                IsWithoutRowId = false,
                PhysicalColumnCount = 5
            }
        };
        var indexes = new List<IndexInfo>(8);
        var views = new List<ViewInfo>(2);

        // sqlite_schema is always rooted at page 1
        using var cursor = _bTreeReader.CreateCursor(1);

        while (cursor.MoveNext())
        {
            _recordDecoder.DecodeRecord(cursor.Payload, _columnBuffer);
            
            // sqlite_schema columns: type(0), name(1), tbl_name(2), rootpage(3), sql(4)
            if (_columnBuffer[0].IsNull) continue;
            
            string type = _columnBuffer[0].AsString();
            string name = _columnBuffer[1].IsNull ? "" : _columnBuffer[1].AsString();
            string tblName = _columnBuffer[2].IsNull ? "" : _columnBuffer[2].AsString();

            int rootPage = _columnBuffer[3].IsNull ? 0 : (int)_columnBuffer[3].AsInt64();
            string? sql = _columnBuffer[4].IsNull ? null : _columnBuffer[4].AsString();

            switch (type)
            {
                case "table":
                    if (sql != null)
                    {
                        var physicalColumns = SchemaParser.ParseTableColumns(sql.AsSpan());
                        var (logicalColumns, physicalCount) = SchemaParser.MergeColumnPairs(physicalColumns);
                        var tableInfo = new TableInfo
                        {
                            Name = name,
                            RootPage = rootPage,
                            Sql = sql,
                            Columns = logicalColumns,
                            IsWithoutRowId = sql.Contains("WITHOUT ROWID",
                                StringComparison.OrdinalIgnoreCase)
                        };
                        tableInfo.PhysicalColumnCount = physicalCount;
                        tables.Add(tableInfo);
                    }
                    break;

                case "index":
                    var indexColumns = sql != null
                        ? SchemaParser.ParseIndexColumns(sql.AsSpan())
                        : (IReadOnlyList<IndexColumnInfo>)[];
                    indexes.Add(new IndexInfo
                    {
                        Name = name,
                        TableName = tblName,
                        RootPage = rootPage,
                        Sql = sql ?? "",
                        IsUnique = sql != null && sql.Contains("UNIQUE",
                            StringComparison.OrdinalIgnoreCase),
                        Columns = indexColumns
                    });
                    break;

                case "view":
                    views.Add(new ViewInfo
                    {
                        Name = name,
                        Sql = sql ?? ""
                    });
                    break;
            }
        }

        // Link indexes to their tables to avoid runtime lookups/allocations
        foreach (var table in tables)
        {
            List<IndexInfo>? tableIndexes = null;
            for (int i = 0; i < indexes.Count; i++)
            {
                if (indexes[i].TableName.Equals(table.Name, StringComparison.OrdinalIgnoreCase))
                {
                    tableIndexes ??= new List<IndexInfo>();
                    tableIndexes.Add(indexes[i]);
                }
            }
            table.Indexes = tableIndexes ?? (IReadOnlyList<IndexInfo>)Array.Empty<IndexInfo>();
        }

        return new SharcSchema
        {
            Tables = tables,
            Indexes = indexes,
            Views = views
        };
    }
}