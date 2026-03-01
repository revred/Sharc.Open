// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Buffers;
using System.Linq;
using Sharc.Core;
using Sharc.Core.BTree;
using Sharc.Core.Format;
using Sharc.Core.Primitives;
using Sharc.Core.Records;
using Sharc.Core.Schema;

namespace Sharc;

/// <summary>
/// Handles Data Definition Language (DDL) execution.
/// </summary>
internal static class SharcSchemaWriter
{
    private const int SqliteMasterRootPage = 1;

    public static void Execute(SharcDatabase db, Transaction tx, string sql, Sharc.Core.Trust.AgentInfo? agent = null)
    {
        // Enforce Entitlements
        if (agent != null)
        {
            Sharc.Trust.EntitlementEnforcer.EnforceSchemaAdmin(agent);
        }

        // Normalize: remove comments, trim, etc? For now just trim.
        var span = sql.AsSpan().Trim();
        if (span.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
        {
            ExecuteCreateTable(db, tx, span, sql);
        }
        else if (span.StartsWith("CREATE UNIQUE INDEX", StringComparison.OrdinalIgnoreCase)
              || span.StartsWith("CREATE INDEX", StringComparison.OrdinalIgnoreCase))
        {
            ExecuteCreateIndex(db, tx, span, sql);
        }
        else if (span.StartsWith("CREATE VIEW", StringComparison.OrdinalIgnoreCase))
        {
            ExecuteCreateView(db, tx, span, sql);
        }
        else if (span.StartsWith("CREATE UNIQUE INDEX", StringComparison.OrdinalIgnoreCase)
              || span.StartsWith("CREATE INDEX", StringComparison.OrdinalIgnoreCase))
        {
            ExecuteCreateIndex(db, tx, span, sql);
        }
        else if (span.StartsWith("ALTER TABLE", StringComparison.OrdinalIgnoreCase))
        {
            ExecuteAlterTable(db, tx, span, sql);
        }
        else
        {
            throw new NotSupportedException($"Unsupported DDL statement: {sql}");
        }
    }

    private static void ExecuteCreateView(SharcDatabase db, Transaction tx, ReadOnlySpan<char> span, string originalSql)
    {
        // CREATE VIEW Name AS SELECT ...
        int pos = 11; // Length of "CREATE VIEW"
        SkipWhitespace(span, ref pos);
        string viewName = ReadIdentifier(span, ref pos);
        
        if (db.Schema.Tables.Any(t => t.Name.Equals(viewName, StringComparison.OrdinalIgnoreCase)))
            throw new InvalidOperationException($"Object '{viewName}' already exists.");

        // Insert into sqlite_master
        var mutator = tx.FetchMutator(db.UsablePageSize);
        long rowId = mutator.GetMaxRowId((uint)SqliteMasterRootPage) + 1;

        var record = new ColumnValue[]
        {
            CreateTextValue("view"),
            CreateTextValue(viewName),
            CreateTextValue(viewName),
            CreateIntValue(0), // Views have rootpage 0
            CreateTextValue(originalSql)
        };

        WriteMasterRecord(db, tx, record, rowId, isInsert: true);
    }

    private static void ExecuteCreateTable(SharcDatabase db, Transaction tx, ReadOnlySpan<char> span, string originalSql)
    {
        // Format: CREATE TABLE [IF NOT EXISTS] Name ( ... )
        int pos = 12; // Length of "CREATE TABLE"
        SkipWhitespace(span, ref pos);

        bool ifNotExists = false;
        if (StartsWith(span.Slice(pos), "IF NOT EXISTS"))
        {
            ifNotExists = true;
            pos += 13;
            SkipWhitespace(span, ref pos);
        }

        string tableName = ReadIdentifier(span, ref pos);
        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentException("Invalid table name in CREATE TABLE statement.");

        // Check if table exists
        if (db.Schema.Tables.Any(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase)))
        {
            if (ifNotExists) return;
            throw new InvalidOperationException($"Table '{tableName}' already exists.");
        }

        // Validate body
        SkipWhitespace(span, ref pos);
        if (pos >= span.Length || span[pos] != '(')
            throw new ArgumentException("Expected '(' after table name.");

        var body = span.Slice(pos);
        var columns = SchemaParser.ParseTableColumns(body);
        if (columns.Count == 0)
            throw new ArgumentException("Table must have at least one column.");

        // Allocate Root Page
        // Usable page size = Header.UsablePageSize. 
        // Note: New root page must be initialized as Leaf Table (0x0D). 
        // BTreeMutator.AllocateNewPage does exactly that.
        uint rootPage = tx.AllocateTableRoot(db.UsablePageSize);

        // Insert into sqlite_master
        long rowId = tx.FetchMutator(db.UsablePageSize).GetMaxRowId((uint)SqliteMasterRootPage) + 1;

        var record = new ColumnValue[]
        {
            CreateTextValue("table"),
            CreateTextValue(tableName),
            CreateTextValue(tableName),
            CreateIntValue(rootPage),
            CreateTextValue(originalSql)
        };

        WriteMasterRecord(db, tx, record, rowId, isInsert: true);
    }

    private static void ExecuteCreateIndex(SharcDatabase db, Transaction tx, ReadOnlySpan<char> span, string originalSql)
    {
        // Format: CREATE [UNIQUE] INDEX [IF NOT EXISTS] Name ON TableName (columns...)
        int pos = 6; // Skip "CREATE"
        SkipWhitespace(span, ref pos);

        // isUnique is used only for parsing — uniqueness is preserved in the original SQL
        // text stored in sqlite_master and detected by SchemaReader.
        if (StartsWith(span.Slice(pos), "UNIQUE"))
        {
            pos += 6;
            SkipWhitespace(span, ref pos);
        }

        // Skip "INDEX"
        if (!StartsWith(span.Slice(pos), "INDEX"))
            throw new ArgumentException("Expected INDEX keyword.");
        pos += 5;
        SkipWhitespace(span, ref pos);

        bool ifNotExists = false;
        if (StartsWith(span.Slice(pos), "IF NOT EXISTS"))
        {
            ifNotExists = true;
            pos += 13;
            SkipWhitespace(span, ref pos);
        }

        string indexName = ReadIdentifier(span, ref pos);
        if (string.IsNullOrEmpty(indexName))
            throw new ArgumentException("Invalid index name in CREATE INDEX statement.");
        SkipWhitespace(span, ref pos);

        // Skip "ON"
        if (!StartsWith(span.Slice(pos), "ON"))
            throw new ArgumentException("Expected ON keyword in CREATE INDEX statement.");
        pos += 2;
        SkipWhitespace(span, ref pos);

        string tableName = ReadIdentifier(span, ref pos);

        // Validate target table exists
        var table = db.Schema.Tables.FirstOrDefault(
            t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase))
            ?? throw new KeyNotFoundException($"Table '{tableName}' not found.");

        // Check if index already exists
        if (db.Schema.Indexes.Any(
            i => i.Name.Equals(indexName, StringComparison.OrdinalIgnoreCase)))
        {
            if (ifNotExists) return;
            throw new InvalidOperationException($"Index '{indexName}' already exists.");
        }

        // Validate columns exist in target table
        var indexColumns = SchemaParser.ParseIndexColumns(originalSql.AsSpan());
        foreach (var col in indexColumns)
        {
            if (table.GetColumnOrdinal(col.Name) < 0)
                throw new ArgumentException(
                    $"Column '{col.Name}' does not exist in table '{tableName}'.");
        }

        // Allocate index root page (LeafIndex = 0x0A)
        uint rootPage = tx.AllocateIndexRoot(db.UsablePageSize);

        // Populate the index B-tree from existing table rows (no-op for empty tables)
        var columnOrdinals = new int[indexColumns.Count];
        for (int i = 0; i < indexColumns.Count; i++)
            columnOrdinals[i] = table.GetColumnOrdinal(indexColumns[i].Name);

        var mutator = tx.FetchMutator(db.UsablePageSize);
        using var populator = new Core.BTree.IndexBTreePopulator(
            mutator, db.UsablePageSize, (Core.IWritablePageSource)tx.PageSource);
        populator.PopulateIndex(rootPage, (uint)table.RootPage, columnOrdinals,
            db.BTreeReader, db.RecordDecoder);

        // Insert into sqlite_master
        long rowId = tx.FetchMutator(db.UsablePageSize).GetMaxRowId((uint)SqliteMasterRootPage) + 1;

        var record = new ColumnValue[]
        {
            CreateTextValue("index"),
            CreateTextValue(indexName),
            CreateTextValue(tableName),
            CreateIntValue(rootPage),
            CreateTextValue(originalSql)
        };

        WriteMasterRecord(db, tx, record, rowId, isInsert: true);
    }

    private static void ExecuteAlterTable(SharcDatabase db, Transaction tx, ReadOnlySpan<char> span, string originalSql)
    {
        // ALTER TABLE Name ...
        int pos = 11; // Length of "ALTER TABLE"
        SkipWhitespace(span, ref pos);
        string tableName = ReadIdentifier(span, ref pos);
        var table = db.Schema.Tables.FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase)) 
            ?? throw new KeyNotFoundException($"Table '{tableName}' not found.");

        SkipWhitespace(span, ref pos);
        var remainder = span.Slice(pos);

        if (StartsWith(remainder, "ADD COLUMN"))
        {
            ExecuteAddColumn(db, tx, table, remainder.Slice(10), originalSql);
        }
        else if (StartsWith(remainder, "RENAME TO"))
        {
            ExecuteRenameTable(db, tx, table, remainder.Slice(9));
        }
        else
        {
            throw new NotSupportedException("Only ADD COLUMN and RENAME TO are supported.");
        }
    }

    private static void ExecuteAddColumn(SharcDatabase db, Transaction tx, TableInfo table, ReadOnlySpan<char> colDef, string fullOriginalSql)
    {
        // colDef is like " Age INTEGER DEFAULT 0'"
        // Validate by wrapping in parenthesis and parsing
        // We need to append this to the ORIGINAL SQL of the table.
        // And update sqlite_master.
        
        // 1. Validate
        // Hack: Construct "CREATE TABLE T ( " + colDef + " )" to parse just the column.
        // Actually SchemaParser.ParseTableColumns works on just "( colDef )".
        string dummySql = string.Concat("(", colDef.ToString(), ")");
        var newCols = SchemaParser.ParseTableColumns(dummySql.AsSpan());
        if (newCols.Count != 1)
            throw new ArgumentException("Invalid column definition or multiple columns not supported.");

        // 2. Find row in sqlite_master
        long masterRowId = FindMasterRowId(db, tx, table.Name);
        
        // 3. Read current SQL
        // We could read it from db.Schema but it might not be the exact strong text?
        // db.Schema loads it from sqlite_master, so it should be fine.
        // Or we assume the schema is up to date.
        // But to be safe, let's read the record from sqlite_master to get exact text.
        var masterRecord = ReadMasterRecord(db, tx, masterRowId);
        string currentSql = masterRecord[4].AsString();

        // 4. Modify SQL
        // Naive string manipulation: insert before last ')'
        int lastParen = currentSql.LastIndexOf(')');
        if (lastParen < 0) throw new InvalidOperationException("Corrupt table SQL definition.");
        
        // Check if we need a comma
        // If the body is empty "()" -> no comma.
        // Normally it's "(Col1 ...)" -> need comma.
        
        string newSql = string.Concat(currentSql.AsSpan(0, lastParen), ", ", colDef.Trim(), ")");

        // 5. Update sqlite_master
        UpdateMasterRecord(db, tx, masterRowId, masterRecord, 4, newSql);
    }

    private static void ExecuteRenameTable(SharcDatabase db, Transaction tx, TableInfo table, ReadOnlySpan<char> newNameSpan)
    {
        int pos = 0;
        SkipWhitespace(newNameSpan, ref pos);
        string newName = ReadIdentifier(newNameSpan, ref pos);
        if (string.IsNullOrEmpty(newName)) throw new ArgumentException("Invalid new table name.");

        if (db.Schema.Tables.Any(t => t.Name.Equals(newName, StringComparison.OrdinalIgnoreCase)))
            throw new InvalidOperationException($"Table '{newName}' already exists.");


        long masterRowId = FindMasterRowId(db, tx, table.Name);
        var masterRecord = ReadMasterRecord(db, tx, masterRowId);
        string currentSql = masterRecord[4].AsString();

        // Update SQL: replace table name.
        // "CREATE TABLE OldName ..." -> "CREATE TABLE NewName ..."
        // Using strict replace might be dangerous if "OldName" appears in column definitions.
        // Regex or specialized replace needed.
        // Or just replace the first occurrence after CREATE TABLE.
        int createIdx = currentSql.IndexOf("CREATE TABLE", StringComparison.OrdinalIgnoreCase);
        if (createIdx < 0) throw new InvalidOperationException("Invalid CREATE statement in sqlite_master.");
        
        int nameStart = createIdx + 12;
        while (nameStart < currentSql.Length && char.IsWhiteSpace(currentSql[nameStart])) nameStart++;
        
        // The existing name length... we know it's table.Name.
        // Verify it matches
        string sqlRemaining = currentSql.Substring(nameStart);
        if (!sqlRemaining.StartsWith(table.Name, StringComparison.OrdinalIgnoreCase) && 
            !sqlRemaining.StartsWith($"\"{table.Name}\"", StringComparison.OrdinalIgnoreCase) && 
            !sqlRemaining.StartsWith($"[{table.Name}]", StringComparison.OrdinalIgnoreCase))
        {
             // It might be quoted or different case.
             // This is tricky.
        }

        // Simpler approach: Reconstruct SQL? No, we drop constraints etc.
        // Best approach: Parse the name out of SQL and replace that range.
        // Let's assume standard formatting for now or minimal robust replacement.
        // "CREATE TABLE " + newName + " (" + columns + ")"
        // Find first (
        int openParen = currentSql.IndexOf('(');
        string newSql = string.Concat("CREATE TABLE ", newName, " ", currentSql.AsSpan(openParen));

        // Update Record
        // col 1: name
        // col 2: tbl_name
        // col 4: sql
        masterRecord[1] = CreateTextValue(newName);
        masterRecord[2] = CreateTextValue(newName);
        masterRecord[4] = CreateTextValue(newSql);

        WriteMasterRecord(db, tx, masterRecord, masterRowId, isInsert: false);
    }

    // --- Helpers ---

    private static long FindMasterRowId(SharcDatabase db, Transaction tx, string tableName)
    {
        using var cursor = db.BTreeReader.CreateCursor((uint)SqliteMasterRootPage);
        while (cursor.MoveNext())
        {
             var record = db.RecordDecoder.DecodeRecord(cursor.Payload);
             // Col 0 = type ("table", "index", etc)
             // Col 1 = name
             if (record.Length > 1 
                 && record[0].AsString().Equals("table", StringComparison.OrdinalIgnoreCase)
                 && record[1].AsString().Equals(tableName, StringComparison.OrdinalIgnoreCase))
             {
                 return cursor.RowId;
             }
        }
        throw new KeyNotFoundException($"Entry for '{tableName}' not found in sqlite_master.");
    }

    private static ColumnValue[] ReadMasterRecord(SharcDatabase db, Transaction tx, long rowId)
    {
         var cursor = db.BTreeReader.CreateCursor((uint)SqliteMasterRootPage);
         if (cursor.Seek(rowId))
         {
             return db.RecordDecoder.DecodeRecord(cursor.Payload);
         }
         throw new KeyNotFoundException($"Row {rowId} not found in sqlite_master.");
    }

    private static void UpdateMasterRecord(SharcDatabase db, Transaction tx, long rowId, ColumnValue[] record, int colIndex, string newValue)
    {
        record[colIndex] = CreateTextValue(newValue);
        WriteMasterRecord(db, tx, record, rowId, isInsert: false);
    }

    private static ColumnValue CreateTextValue(string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        long serialType = 2L * bytes.Length + 13;
        return ColumnValue.Text(serialType, bytes);
    }

    private static ColumnValue CreateIntValue(long value)
    {
        if (value == 0) return ColumnValue.FromInt64(8, 0);
        if (value == 1) return ColumnValue.FromInt64(9, 1);
        // Simplified: use 64-bit int for all others for now
        return ColumnValue.FromInt64(6, value);
    }

    private static void WriteMasterRecord(SharcDatabase db, Transaction tx, ColumnValue[] record, long rowId, bool isInsert)
    {
        var mutator = tx.FetchMutator(db.UsablePageSize);
        int size = RecordEncoder.ComputeEncodedSize(record);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            RecordEncoder.EncodeRecord(record, buffer);
            if (isInsert)
                mutator.Insert((uint)SqliteMasterRootPage, rowId, buffer.AsSpan(0, size));
            else
                mutator.Update((uint)SqliteMasterRootPage, rowId, buffer.AsSpan(0, size));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        
        tx.SetSchemaCookie(db.Header.SchemaCookie + 1);
        db.InvalidateSchema();
    }

    // --- Parser Helpers (simplified) ---
    private static void SkipWhitespace(ReadOnlySpan<char> s, ref int pos)
    {
        while (pos < s.Length && char.IsWhiteSpace(s[pos])) pos++;
    }

    private static string ReadIdentifier(ReadOnlySpan<char> s, ref int pos)
    {
        SkipWhitespace(s, ref pos);
        if (pos >= s.Length) return "";
        if (s[pos] is '\"' or '[' or '`')
        {
            char endChar = s[pos] == '[' ? ']' : s[pos];
            int start = ++pos;
            while (pos < s.Length && s[pos] != endChar) pos++;
            string id = s.Slice(start, pos - start).ToString();
            if (pos < s.Length) pos++; 
            return id;
        }
        int idS = pos;
        while (pos < s.Length && (char.IsLetterOrDigit(s[pos]) || s[pos] == '_')) pos++;
        return s.Slice(idS, pos - idS).ToString();
    }

    private static bool StartsWith(ReadOnlySpan<char> s, string value) =>
        s.StartsWith(value.AsSpan(), StringComparison.OrdinalIgnoreCase);
}
