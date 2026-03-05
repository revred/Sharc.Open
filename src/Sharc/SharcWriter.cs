// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Buffers.Binary;
using System.Text;
using Sharc.Core;
using Sharc.Core.BTree;
using Sharc.Core.Format;
using Sharc.Core.IO;
using Sharc.Core.Primitives;
using Sharc.Core.Records;
using Sharc.Core.Schema;
using Sharc.Core.Trust;

namespace Sharc;

/// <summary>
/// Public API for writing to a Sharc database.
/// Wraps <see cref="SharcDatabase"/> with typed write operations.
/// </summary>
public sealed class SharcWriter : IDisposable
{
    private readonly SharcDatabase _db;
    private readonly bool _ownsDb;
    private readonly Dictionary<string, uint> _tableRootCache = new(StringComparer.OrdinalIgnoreCase);
    private ShadowPageSource? _cachedShadow;
    private bool _disposed;

    /// <summary>Number of cached table root pages. Exposed for test observability.</summary>
    internal int TableRootCacheCount => _tableRootCache.Count;

    /// <summary>
    /// Opens a database for reading and writing.
    /// </summary>
    public static SharcWriter Open(string path)
    {
        // Write-appropriate cache: BTreeMutator._pageCache handles intra-transaction pages;
        // LRU only serves cross-transaction reads for schema + root pages.
        // 16 pages × 4096 = 64 KB maximum, demand-grown from 0.
        var db = SharcDatabase.Open(path, new SharcOpenOptions { Writable = true, PageCacheSize = 16 });
        return new SharcWriter(db, ownsDb: true);
    }

    /// <summary>
    /// Wraps an existing <see cref="SharcDatabase"/> for write operations.
    /// The caller retains ownership of the database.
    /// </summary>
    public static SharcWriter From(SharcDatabase db)
    {
        return new SharcWriter(db, ownsDb: false);
    }

    // ─────────────────────────────────────────────────────────────
    //  WriteScope APIs — disposal-safe write operations
    // ─────────────────────────────────────────────────────────────

    /// <summary>
    /// <b>Recommended API.</b> Executes a write action within a guaranteed-disposal scope.
    /// The scope auto-commits on success and rolls back on exception.
    /// Disposal is automatic — the caller cannot forget <c>using</c>.
    /// <code>
    /// SharcWriter.WriteScope("data.arc", scope =>
    /// {
    ///     scope.Insert("events", values1);
    ///     scope.Insert("events", values2);
    /// });
    /// // auto-committed — no way to leak the scope
    /// </code>
    /// </summary>
    public static void WriteScope(string path, Action<WriteScope> action)
    {
        using var scope = OpenScope(path);
        action(scope);
    }

    /// <summary>
    /// <b>Recommended API.</b> Executes a write function within a guaranteed-disposal scope
    /// and returns a result.
    /// <code>
    /// long id = SharcWriter.WriteScope("data.arc", scope =>
    /// {
    ///     return scope.Insert("events", values);
    /// });
    /// </code>
    /// </summary>
    public static TResult WriteScope<TResult>(string path, Func<WriteScope, TResult> action)
    {
        using var scope = OpenScope(path);
        return action(scope);
    }

    /// <summary>
    /// Opens a database and returns an RAII-style <see cref="Sharc.WriteScope"/> that
    /// auto-commits on Dispose. Prefer <see cref="WriteScope(string, Action{WriteScope})"/>
    /// which guarantees disposal. Use this only when you need the scope to span
    /// multiple statements or control Flush/Finalize timing.
    /// <para><b>Important:</b> Always use a <c>using</c> statement or <c>using var</c>
    /// declaration. Without <c>using</c>, pending writes are lost and the file lock
    /// leaks until GC finalization (which fires a <c>Debug.Fail</c> to catch the bug).</para>
    /// <code>
    /// using var scope = SharcWriter.OpenScope("data.arc");
    /// scope.Insert("events", values);
    /// // auto-commits when scope is disposed
    /// </code>
    /// </summary>
    public static WriteScope OpenScope(string path)
    {
        var db = SharcDatabase.Open(path, new SharcOpenOptions { Writable = true, PageCacheSize = 16 });
        return new WriteScope(db, ownsDb: true);
    }

    /// <summary>
    /// Creates a <see cref="Sharc.WriteScope"/> over an existing database.
    /// The caller retains ownership of the database.
    /// <para><b>Important:</b> Always use a <c>using</c> statement or <c>using var</c>
    /// declaration.</para>
    /// </summary>
    public static WriteScope Scope(SharcDatabase db)
    {
        return new WriteScope(db, ownsDb: false);
    }

    private SharcWriter(SharcDatabase db, bool ownsDb)
    {
        _db = db;
        _ownsDb = ownsDb;
    }

    /// <summary>
    /// Begins an auto-commit transaction using a pooled ShadowPageSource.
    /// The shadow is created on first use, then Reset and reused across transactions.
    /// A fresh BTreeMutator is always created per-transaction to avoid stale freelist state.
    /// </summary>
    private Transaction BeginAutoCommitTransaction()
    {
        if (_cachedShadow == null)
            return _db.BeginTransaction();

        return _db.BeginPooledTransaction(_cachedShadow);
    }

    /// <summary>
    /// Captures the ShadowPageSource from a completed auto-commit transaction for reuse.
    /// </summary>
    private void CapturePooledShadow(Transaction tx)
    {
        _cachedShadow ??= tx.GetShadowSource();
    }

    /// <summary>
    /// Gets the underlying database for read operations.
    /// </summary>
    public SharcDatabase Database => _db;

    /// <summary>
    /// Inserts a single record into the given table. Auto-commits.
    /// Returns the assigned rowid.
    /// </summary>
    public long Insert(string tableName, params ColumnValue[] values)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var tx = BeginAutoCommitTransaction();
        long rowId = InsertCore(tx, tableName, values, TryGetTableInfo(tableName), _tableRootCache);
        CapturePooledShadow(tx);
        tx.Commit();
        return rowId;
    }

    /// <summary>
    /// Inserts a single record with agent write-scope enforcement. Auto-commits.
    /// Throws <see cref="UnauthorizedAccessException"/> if the agent's WriteScope denies access.
    /// </summary>
    public long Insert(AgentInfo agent, string tableName, params ColumnValue[] values)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tableInfo = TryGetTableInfo(tableName);
        Trust.EntitlementEnforcer.EnforceWrite(agent, tableName, GetColumnNames(tableInfo, values.Length));

        using var tx = BeginAutoCommitTransaction();
        long rowId = InsertCore(tx, tableName, values, tableInfo, _tableRootCache);
        CapturePooledShadow(tx);
        tx.Commit();
        return rowId;
    }

    /// <summary>
    /// Inserts multiple records in a single transaction.
    /// Returns the assigned rowids.
    /// </summary>
    public long[] InsertBatch(string tableName, IEnumerable<ColumnValue[]> records)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var tableInfo = TryGetTableInfo(tableName);
        var rowIds = records is ICollection<ColumnValue[]> coll ? new List<long>(coll.Count) : new List<long>();
        using var tx = BeginAutoCommitTransaction();
        foreach (var values in records)
        {
            rowIds.Add(InsertCore(tx, tableName, values, tableInfo, _tableRootCache));
        }
        CapturePooledShadow(tx);
        tx.Commit();
        return rowIds.ToArray();
    }

    /// <summary>
    /// Inserts multiple records with agent write-scope enforcement.
    /// Throws <see cref="UnauthorizedAccessException"/> if the agent's WriteScope denies access.
    /// </summary>
    public long[] InsertBatch(AgentInfo agent, string tableName, IEnumerable<ColumnValue[]> records)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tableInfo = TryGetTableInfo(tableName);
        
        // Peek first record to get column count for enforcement
        int colCount = records is IReadOnlyList<ColumnValue[]> list && list.Count > 0 ? list[0].Length : 0;
        Trust.EntitlementEnforcer.EnforceWrite(agent, tableName, GetColumnNames(tableInfo, colCount));

        var rowIds = records is ICollection<ColumnValue[]> coll ? new List<long>(coll.Count) : new List<long>();
        using var tx = BeginAutoCommitTransaction();
        foreach (var values in records)
        {
            rowIds.Add(InsertCore(tx, tableName, values, tableInfo, _tableRootCache));
        }
        CapturePooledShadow(tx);
        tx.Commit();
        return rowIds.ToArray();
    }

    /// <summary>
    /// Deletes a single record by rowid. Auto-commits.
    /// Returns true if the row existed and was removed.
    /// </summary>
    public bool Delete(string tableName, long rowId)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var tx = BeginAutoCommitTransaction();
        bool found = DeleteCore(tx, tableName, rowId, _tableRootCache);
        CapturePooledShadow(tx);
        tx.Commit();
        return found;
    }

    /// <summary>
    /// Deletes a single record with agent write-scope enforcement. Auto-commits.
    /// Throws <see cref="UnauthorizedAccessException"/> if the agent's WriteScope denies access.
    /// </summary>
    public bool Delete(AgentInfo agent, string tableName, long rowId)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        Trust.EntitlementEnforcer.EnforceWrite(agent, tableName, null); // Delete usually requires all-columns or just table write

        using var tx = BeginAutoCommitTransaction();
        bool found = DeleteCore(tx, tableName, rowId, _tableRootCache);
        CapturePooledShadow(tx);
        tx.Commit();
        return found;
    }

    /// <summary>
    /// Updates a single record by rowid with new column values. Auto-commits.
    /// Returns true if the row existed and was updated.
    /// </summary>
    public bool Update(string tableName, long rowId, params ColumnValue[] values)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var tx = BeginAutoCommitTransaction();
        bool found = UpdateCore(tx, tableName, rowId, values, TryGetTableInfo(tableName), _tableRootCache);
        CapturePooledShadow(tx);
        tx.Commit();
        return found;
    }

    /// <summary>
    /// Updates a single record with agent write-scope enforcement. Auto-commits.
    /// Throws <see cref="UnauthorizedAccessException"/> if the agent's WriteScope denies access.
    /// </summary>
    public bool Update(AgentInfo agent, string tableName, long rowId, params ColumnValue[] values)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tableInfo = TryGetTableInfo(tableName);
        Trust.EntitlementEnforcer.EnforceWrite(agent, tableName, GetColumnNames(tableInfo, values.Length));

        using var tx = BeginAutoCommitTransaction();
        bool found = UpdateCore(tx, tableName, rowId, values, tableInfo, _tableRootCache);
        CapturePooledShadow(tx);
        tx.Commit();
        return found;
    }

    /// <summary>
    /// Inserts or updates a record by rowid. If the row exists, it is updated;
    /// if not, it is inserted with the specified rowid. Auto-commits.
    /// Returns the rowid.
    /// </summary>
    public long Upsert(string tableName, long rowId, params ColumnValue[] values)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var tableInfo = TryGetTableInfo(tableName);
        using var tx = BeginAutoCommitTransaction();

        bool updated = UpdateCore(tx, tableName, rowId, values, tableInfo, _tableRootCache);
        if (!updated)
            InsertCoreAtRowId(tx, tableName, rowId, values, tableInfo, _tableRootCache);

        CapturePooledShadow(tx);
        tx.Commit();
        return rowId;
    }

    /// <summary>
    /// Deletes all rows matching the given filter predicate. Auto-commits.
    /// Returns the number of rows deleted.
    /// </summary>
    public int DeleteWhere(string tableName, IFilterStar filter)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Phase 1: Collect matching rowids via JitQuery (read-only scan)
        var rowIds = new List<long>();
        using (var jit = _db.Jit(tableName))
        {
            jit.Where(filter);
            using var reader = jit.Query();
            while (reader.Read())
                rowIds.Add(reader.RowId);
        }

        if (rowIds.Count == 0) return 0;

        // Phase 2: Delete in reverse rowid order to avoid B-tree rebalancing interference
        rowIds.Sort();
        rowIds.Reverse();

        using var tx = BeginAutoCommitTransaction();
        int deleted = 0;
        foreach (long rowId in rowIds)
        {
            if (DeleteCore(tx, tableName, rowId, _tableRootCache))
                deleted++;
        }
        CapturePooledShadow(tx);
        tx.Commit();
        return deleted;
    }

    /// <summary>
    /// Creates a <see cref="PreparedWriter"/> for zero-overhead repeated writes to the given table.
    /// Schema and root page are resolved once at creation time.
    /// </summary>
    /// <param name="tableName">The table to write to.</param>
    /// <returns>A reusable <see cref="PreparedWriter"/> bound to the table.</returns>
    public PreparedWriter PrepareWriter(string tableName)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tableInfo = TryGetTableInfo(tableName);
        return new PreparedWriter(_db, tableName, tableInfo, _tableRootCache);
    }

    /// <summary>
    /// Begins an explicit write transaction.
    /// </summary>
    public SharcWriteTransaction BeginTransaction()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tx = _db.BeginTransaction();
        return new SharcWriteTransaction(_db, tx, _tableRootCache);
    }

    /// <summary>
    /// Begins an explicit write transaction bound to an agent for entitlement enforcement.
    /// </summary>
    public SharcWriteTransaction BeginTransaction(AgentInfo agent)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tx = _db.BeginTransaction();
        return new SharcWriteTransaction(_db, tx, _tableRootCache, agent);
    }

    /// <summary>
    /// Core insert: encode record → insert into B-tree via the transaction's shadow source.
    /// For tables with an INTEGER PRIMARY KEY column, the caller's explicit value is used
    /// as the B-tree rowid and NULL is stored at that position in the record (matching SQLite).
    /// </summary>
    internal static long InsertCore(Transaction tx, string tableName, ColumnValue[] values,
        TableInfo? tableInfo = null, Dictionary<string, uint>? rootCache = null)
    {
        var shadow = tx.GetShadowSource();
        int pageSize = shadow.PageSize;
        int usableSize = pageSize; // Reserved bytes are zero for all Sharc-created databases

        // Expand merged GUID columns to physical hi/lo Int64 pairs
        if (tableInfo != null)
            values = ExpandMergedColumns(values, tableInfo);

        uint rootPage = FindTableRootPageCached(shadow, tableName, usableSize, rootCache);
        if (rootPage == 0)
            throw new InvalidOperationException($"Table '{tableName}' not found.");

        // Snapshot values before IPK nullification — needed for index maintenance
        ColumnValue[]? indexValues = tableInfo?.Indexes.Count > 0
            ? (ColumnValue[])values.Clone()
            : null;

        // Detect INTEGER PRIMARY KEY (rowid alias):
        // Use caller's explicit value as rowid; store NULL at IPK position in record.
        int ipkOrdinal = FindIpkOrdinal(tableInfo, values.Length);
        long rowId;
        if (ipkOrdinal >= 0 && !values[ipkOrdinal].IsNull)
        {
            rowId = values[ipkOrdinal].AsInt64();
            values[ipkOrdinal] = ColumnValue.Null();
        }
        else
        {
            rowId = tx.FetchMutator(usableSize).GetMaxRowId(rootPage) + 1;
            if (ipkOrdinal >= 0)
                values[ipkOrdinal] = ColumnValue.Null();
        }

        // Restore IPK value in the index snapshot so indexed columns see the actual value
        if (indexValues != null && ipkOrdinal >= 0)
            indexValues[ipkOrdinal] = ColumnValue.FromInt64(6, rowId);

        // Encode the record
        int encodedSize = RecordEncoder.ComputeEncodedSize(values);
        Span<byte> recordBuf = encodedSize <= 512 ? stackalloc byte[encodedSize] : new byte[encodedSize];
        RecordEncoder.EncodeRecord(values, recordBuf);

        // Insert into B-tree
        var mutator = tx.FetchMutator(usableSize);
        uint newRoot = mutator.Insert(rootPage, rowId, recordBuf);
        tx.TrackRowMutation(tableName, rowId);

        if (newRoot != rootPage)
        {
            if (rootCache != null) rootCache[tableName] = newRoot;
            UpdateTableRootPage(shadow, tableName, newRoot, usableSize);
        }

        // Maintain secondary indexes
        if (indexValues != null)
            MaintainIndexesOnInsert(tx, tableName, rowId, indexValues, tableInfo, usableSize, rootCache);

        return rowId;
    }

    /// <summary>
    /// Core insert at a specific rowid (for upsert fallback when Update returns false).
    /// </summary>
    internal static long InsertCoreAtRowId(Transaction tx, string tableName, long rowId,
        ColumnValue[] values, TableInfo? tableInfo = null, Dictionary<string, uint>? rootCache = null)
    {
        var shadow = tx.GetShadowSource();
        int usableSize = shadow.PageSize;

        if (tableInfo != null)
            values = ExpandMergedColumns(values, tableInfo);

        // Snapshot values before IPK nullification for index maintenance
        ColumnValue[]? indexValues = tableInfo?.Indexes.Count > 0
            ? (ColumnValue[])values.Clone()
            : null;

        // Nullify IPK column — rowid is already passed explicitly
        int ipkOrdinal = FindIpkOrdinal(tableInfo, values.Length);
        if (ipkOrdinal >= 0)
            values[ipkOrdinal] = ColumnValue.Null();

        if (indexValues != null && ipkOrdinal >= 0)
            indexValues[ipkOrdinal] = ColumnValue.FromInt64(6, rowId);

        uint rootPage = FindTableRootPageCached(shadow, tableName, usableSize, rootCache);
        if (rootPage == 0)
            throw new InvalidOperationException($"Table '{tableName}' not found.");

        int encodedSize = RecordEncoder.ComputeEncodedSize(values);
        Span<byte> recordBuf = encodedSize <= 512 ? stackalloc byte[encodedSize] : new byte[encodedSize];
        RecordEncoder.EncodeRecord(values, recordBuf);

        var mutator = tx.FetchMutator(usableSize);
        uint newRoot = mutator.Insert(rootPage, rowId, recordBuf);
        tx.TrackRowMutation(tableName, rowId);

        if (newRoot != rootPage)
        {
            if (rootCache != null) rootCache[tableName] = newRoot;
            UpdateTableRootPage(shadow, tableName, newRoot, usableSize);
        }

        // Maintain secondary indexes
        if (indexValues != null)
            MaintainIndexesOnInsert(tx, tableName, rowId, indexValues, tableInfo, usableSize, rootCache);

        return rowId;
    }

    /// <summary>
    /// Core delete: find table root → mutator.Delete → update root if changed.
    /// Maintains secondary indexes if tableInfo is provided.
    /// </summary>
    internal static bool DeleteCore(Transaction tx, string tableName, long rowId,
        Dictionary<string, uint>? rootCache = null, TableInfo? tableInfo = null)
    {
        var shadow = tx.GetShadowSource();
        int usableSize = shadow.PageSize;

        uint rootPage = FindTableRootPageCached(shadow, tableName, usableSize, rootCache);
        if (rootPage == 0)
            throw new InvalidOperationException($"Table '{tableName}' not found.");

        // Read old values before delete for index maintenance
        ColumnValue[]? oldValues = null;
        if (tableInfo?.Indexes.Count > 0)
        {
            oldValues = ReadOldValues(shadow, rootPage, rowId, usableSize, tableInfo.Columns.Count);
        }

        var mutator = tx.FetchMutator(usableSize);
        var (found, newRoot) = mutator.Delete(rootPage, rowId);

        if (found)
            tx.TrackRowMutation(tableName, rowId);

        // Remove old entries from secondary indexes
        if (found && oldValues != null)
            MaintainIndexesOnDelete(tx, tableName, rowId, oldValues, tableInfo, usableSize, rootCache);

        if (found && newRoot != rootPage)
        {
            if (rootCache != null) rootCache[tableName] = newRoot;
            UpdateTableRootPage(shadow, tableName, newRoot, usableSize);
        }

        return found;
    }

    /// <summary>
    /// Core update: find table root → encode record → mutator.Update → update root if changed.
    /// For tables with an INTEGER PRIMARY KEY column, NULL is stored at that position
    /// in the record (the real value is the B-tree rowid, matching SQLite).
    /// </summary>
    internal static bool UpdateCore(Transaction tx, string tableName, long rowId, ColumnValue[] values,
        TableInfo? tableInfo = null, Dictionary<string, uint>? rootCache = null)
    {
        var shadow = tx.GetShadowSource();
        int usableSize = shadow.PageSize;

        // Expand merged GUID columns to physical hi/lo Int64 pairs
        if (tableInfo != null)
            values = ExpandMergedColumns(values, tableInfo);

        // Snapshot new values before IPK nullification — needed for index maintenance
        ColumnValue[]? newIndexValues = tableInfo?.Indexes.Count > 0
            ? (ColumnValue[])values.Clone()
            : null;

        // Nullify IPK column — rowid is already passed explicitly
        int ipkOrdinal = FindIpkOrdinal(tableInfo, values.Length);
        if (ipkOrdinal >= 0)
            values[ipkOrdinal] = ColumnValue.Null();

        // Restore IPK value in the index snapshot
        if (newIndexValues != null && ipkOrdinal >= 0)
            newIndexValues[ipkOrdinal] = ColumnValue.FromInt64(6, rowId);

        uint rootPage = FindTableRootPageCached(shadow, tableName, usableSize, rootCache);
        if (rootPage == 0)
            throw new InvalidOperationException($"Table '{tableName}' not found.");

        // Read old values before update for index maintenance
        ColumnValue[]? oldValues = null;
        if (newIndexValues != null)
        {
            oldValues = ReadOldValues(shadow, rootPage, rowId, usableSize, tableInfo!.Columns.Count);
            // Restore IPK in old values too
            if (oldValues != null && ipkOrdinal >= 0)
                oldValues[ipkOrdinal] = ColumnValue.FromInt64(6, rowId);
        }

        int encodedSize = RecordEncoder.ComputeEncodedSize(values);
        Span<byte> recordBuf = encodedSize <= 512 ? stackalloc byte[encodedSize] : new byte[encodedSize];
        RecordEncoder.EncodeRecord(values, recordBuf);

        var mutator = tx.FetchMutator(usableSize);
        var (found, newRoot) = mutator.Update(rootPage, rowId, recordBuf);

        if (found)
            tx.TrackRowMutation(tableName, rowId);

        // Update secondary indexes: delete old entries, insert new entries
        if (found && oldValues != null && newIndexValues != null)
        {
            MaintainIndexesOnDelete(tx, tableName, rowId, oldValues, tableInfo, usableSize, rootCache);
            MaintainIndexesOnInsert(tx, tableName, rowId, newIndexValues, tableInfo, usableSize, rootCache);
        }

        if (found && newRoot != rootPage)
        {
            if (rootCache != null) rootCache[tableName] = newRoot;
            UpdateTableRootPage(shadow, tableName, newRoot, usableSize);
        }

        return found;
    }

    /// <summary>
    /// Looks up a table's metadata from the database schema.
    /// Returns null if the table is not found (caller falls back to schema-less path).
    /// </summary>
    private TableInfo? TryGetTableInfo(string tableName)
    {
        var tables = _db.Schema.Tables;
        for (int i = 0; i < tables.Count; i++)
        {
            if (tables[i].Name.Equals(tableName, StringComparison.OrdinalIgnoreCase))
                return tables[i];
        }
        return null;
    }

    private static string[]? GetColumnNames(TableInfo? table, int valueCount)
    {
        if (table == null || valueCount == 0) return null;
        var cols = new string[Math.Min(valueCount, table.Columns.Count)];
        for (int i = 0; i < cols.Length; i++)
            cols[i] = table.Columns[i].Name;
        return cols;
    }

    /// <summary>
    /// Finds the ordinal of the INTEGER PRIMARY KEY column (rowid alias), or -1 if none.
    /// SQLite treats INTEGER PRIMARY KEY as an alias for the B-tree rowid: the caller's
    /// explicit value becomes the rowid and NULL is stored at that position in the record.
    /// Only returns a valid ordinal when the caller provides values for ALL columns
    /// (including the IPK column). When the caller omits the IPK column (passes N-1
    /// values for an N-column table), returns -1 to fall back to auto-rowid.
    /// </summary>
    private static int FindIpkOrdinal(TableInfo? tableInfo, int valueCount)
    {
        if (tableInfo == null) return -1;
        var columns = tableInfo.Columns;
        // Only apply IPK handling when the caller passes all columns.
        // If they pass fewer values, they're omitting the IPK column (auto-rowid).
        if (valueCount < columns.Count) return -1;
        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i].IsPrimaryKey &&
                columns[i].DeclaredType.Equals("INTEGER", StringComparison.OrdinalIgnoreCase))
                return columns[i].Ordinal;
        }
        return -1;
    }

    /// <summary>
    /// Checks the cache first, then falls back to scanning sqlite_master.
    /// </summary>
    private static uint FindTableRootPageCached(IPageSource source, string tableName,
        int usableSize, Dictionary<string, uint>? cache)
    {
        if (cache != null && cache.TryGetValue(tableName, out uint cached))
            return cached;

        uint rootPage = FindTableRootPage(source, tableName, usableSize);
        if (cache != null && rootPage != 0)
            cache[tableName] = rootPage;

        return rootPage;
    }

    // Reusable scratch buffer and decoder for FindTableRootPage — avoids per-call allocations.
    // ThreadStatic ensures thread safety without locking.
    [ThreadStatic] private static ColumnValue[]? t_schemaColumnBuffer;
    [ThreadStatic] private static RecordDecoder? t_schemaDecoder;

    /// <summary>
    /// Finds the root page of a table by scanning sqlite_master (page 1).
    /// </summary>
    private static uint FindTableRootPage(IPageSource source, string tableName, int usableSize)
    {
        using var cursor = new BTreeCursor<IPageSource>(source, 1, usableSize);
        var columnBuffer = t_schemaColumnBuffer ??= new ColumnValue[5];
        var decoder = t_schemaDecoder ??= new RecordDecoder();

        while (cursor.MoveNext())
        {
            decoder.DecodeRecord(cursor.Payload, columnBuffer);
            if (columnBuffer[0].IsNull) continue;

            string type = columnBuffer[0].AsString();
            if (type != "table") continue;

            string name = columnBuffer[1].IsNull ? "" : columnBuffer[1].AsString();
            if (string.Equals(name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                return columnBuffer[3].IsNull ? 0u : (uint)columnBuffer[3].AsInt64();
            }
        }

        return 0;
    }

    /// <summary>
    /// Updates the root page of a table in sqlite_master after a root split.
    /// Scans page 1 to find the matching sqlite_master row, re-encodes the record
    /// with the new rootpage value, and updates it via BTreeMutator.
    /// </summary>
    private static void UpdateTableRootPage(IWritablePageSource source, string tableName,
        uint newRootPage, int usableSize)
    {
        using var cursor = new BTreeCursor<IWritablePageSource>(source, 1, usableSize);
        var columnBuffer = new ColumnValue[5];
        var decoder = new RecordDecoder();

        while (cursor.MoveNext())
        {
            decoder.DecodeRecord(cursor.Payload, columnBuffer);
            if (columnBuffer[0].IsNull) continue;

            string type = columnBuffer[0].AsString();
            if (type != "table" && type != "index") continue;

            string name = columnBuffer[1].IsNull ? "" : columnBuffer[1].AsString();
            if (!string.Equals(name, tableName, StringComparison.OrdinalIgnoreCase))
                continue;

            // Found the matching sqlite_master entry — re-encode with new rootpage
            long rowId = cursor.RowId;

            // Preserve all columns except rootpage (column 3)
            var record = new ColumnValue[5];
            record[0] = columnBuffer[0];
            record[1] = columnBuffer[1];
            record[2] = columnBuffer[2];
            record[3] = ColumnValue.FromInt64(1, (long)newRootPage);
            record[4] = columnBuffer[4];

            int size = RecordEncoder.ComputeEncodedSize(record);
            Span<byte> buf = size <= 512 ? stackalloc byte[size] : new byte[size];
            RecordEncoder.EncodeRecord(record, buf);

            // Must dispose cursor before mutating the same B-tree it's reading
            cursor.Dispose();

            using var mutator = new BTreeMutator(source, usableSize);
            mutator.Update(1, rowId, buf);
            return;
        }
    }

    // ── Secondary Index Maintenance ──────────────────────────────

    /// <summary>
    /// Inserts entries into all secondary indexes on the given table for the specified row.
    /// Called after a row is inserted into the table B-tree.
    /// </summary>
    private static void MaintainIndexesOnInsert(Transaction tx, string tableName,
        long rowId, ColumnValue[] values, TableInfo? tableInfo, int usableSize,
        Dictionary<string, uint>? rootCache)
    {
        if (tableInfo == null) return;
        var indexes = tableInfo.Indexes;
        if (indexes.Count == 0) return;

        var shadow = tx.GetShadowSource();
        var indexMutator = tx.FetchIndexMutator(usableSize);

        foreach (var index in indexes)
        {
            var indexRecord = BuildIndexRecord(values, rowId, index, tableInfo);
            int encodedSize = RecordEncoder.ComputeEncodedSize(indexRecord);
            byte[] recordBuf = new byte[encodedSize];
            RecordEncoder.EncodeRecord(indexRecord, recordBuf);

            uint indexRoot = FindIndexRootPageCached(shadow, index.Name, usableSize, rootCache);
            if (indexRoot == 0) continue;

            uint newRoot = indexMutator.Insert(indexRoot, recordBuf);
            if (newRoot != indexRoot)
            {
                if (rootCache != null) rootCache[index.Name] = newRoot;
                UpdateTableRootPage(shadow, index.Name, newRoot, usableSize);
            }
        }
    }

    /// <summary>
    /// Deletes entries from all secondary indexes on the given table for the specified row.
    /// The <paramref name="oldValues"/> must be the OLD values being removed.
    /// </summary>
    private static void MaintainIndexesOnDelete(Transaction tx, string tableName,
        long rowId, ColumnValue[] oldValues, TableInfo? tableInfo, int usableSize,
        Dictionary<string, uint>? rootCache)
    {
        if (tableInfo == null) return;
        var indexes = tableInfo.Indexes;
        if (indexes.Count == 0) return;

        var shadow = tx.GetShadowSource();
        var indexMutator = tx.FetchIndexMutator(usableSize);

        foreach (var index in indexes)
        {
            var indexRecord = BuildIndexRecord(oldValues, rowId, index, tableInfo);
            int encodedSize = RecordEncoder.ComputeEncodedSize(indexRecord);
            byte[] recordBuf = new byte[encodedSize];
            RecordEncoder.EncodeRecord(indexRecord, recordBuf);

            uint indexRoot = FindIndexRootPageCached(shadow, index.Name, usableSize, rootCache);
            if (indexRoot == 0) continue;

            var (found, newRoot) = indexMutator.Delete(indexRoot, recordBuf);
            if (found && newRoot != indexRoot)
            {
                if (rootCache != null) rootCache[index.Name] = newRoot;
                UpdateTableRootPage(shadow, index.Name, newRoot, usableSize);
            }
        }
    }

    /// <summary>
    /// Reads the old record values for a row from the table B-tree.
    /// Used by UpdateCore and DeleteCore to build old index entries for removal.
    /// </summary>
    private static ColumnValue[]? ReadOldValues(IPageSource source, uint tableRoot,
        long rowId, int usableSize, int columnCount)
    {
        using var cursor = new BTreeCursor<IPageSource>(source, tableRoot, usableSize);
        if (!cursor.Seek(rowId)) return null;

        var decoder = new RecordDecoder();
        var values = new ColumnValue[columnCount];
        decoder.DecodeRecord(cursor.Payload, values);
        return values;
    }

    /// <summary>
    /// Builds a SQLite index record: [indexed column values..., table rowid].
    /// The rowid is always the last column in the index record.
    /// </summary>
    private static ColumnValue[] BuildIndexRecord(ColumnValue[] rowValues, long rowId,
        IndexInfo index, TableInfo table)
    {
        var cols = index.Columns;
        var record = new ColumnValue[cols.Count + 1];

        for (int i = 0; i < cols.Count; i++)
        {
            // Find the column ordinal in the table
            int ordinal = -1;
            for (int j = 0; j < table.Columns.Count; j++)
            {
                if (table.Columns[j].Name.Equals(cols[i].Name, StringComparison.OrdinalIgnoreCase))
                {
                    ordinal = j;
                    break;
                }
            }

            record[i] = ordinal >= 0 && ordinal < rowValues.Length
                ? rowValues[ordinal]
                : ColumnValue.Null();
        }

        // Last column is the table rowid
        record[cols.Count] = ColumnValue.FromInt64(6, rowId);
        return record;
    }

    /// <summary>
    /// Finds an index root page by name, with cache.
    /// Reuses the same root-page scanning logic as tables (both are in sqlite_master).
    /// </summary>
    private static uint FindIndexRootPageCached(IPageSource source, string indexName,
        int usableSize, Dictionary<string, uint>? cache)
    {
        if (cache != null && cache.TryGetValue(indexName, out uint cached))
            return cached;

        uint rootPage = FindIndexRootPage(source, indexName, usableSize);
        if (cache != null && rootPage != 0)
            cache[indexName] = rootPage;

        return rootPage;
    }

    /// <summary>
    /// Finds the root page of an index by scanning sqlite_master.
    /// </summary>
    private static uint FindIndexRootPage(IPageSource source, string indexName, int usableSize)
    {
        using var cursor = new BTreeCursor<IPageSource>(source, 1, usableSize);
        var columnBuffer = new ColumnValue[5];
        var decoder = new RecordDecoder();

        while (cursor.MoveNext())
        {
            decoder.DecodeRecord(cursor.Payload, columnBuffer);
            if (columnBuffer[0].IsNull) continue;

            string type = columnBuffer[0].AsString();
            if (type != "index") continue;

            string name = columnBuffer[1].IsNull ? "" : columnBuffer[1].AsString();
            if (string.Equals(name, indexName, StringComparison.OrdinalIgnoreCase))
            {
                return columnBuffer[3].IsNull ? 0u : (uint)columnBuffer[3].AsInt64();
            }
        }

        return 0;
    }

    /// <summary>
    /// Expands logical column values into physical column values for tables with merged columns.
    /// FIX128 values at merged column positions are split into hi/lo Int64 pairs.
    /// Returns the same array unchanged if the table has no merged columns (fast path).
    /// </summary>
    internal static ColumnValue[] ExpandMergedColumns(ColumnValue[] logical, TableInfo table)
    {
        if (!table.HasMergedColumns) return logical;

        var physical = new ColumnValue[table.PhysicalColumnCount];
        var columns = table.Columns;

        for (int i = 0; i < columns.Count && i < logical.Length; i++)
        {
            var col = columns[i];
            if (col.IsMergedFix128Column)
            {
                var mergedOrdinals = col.MergedPhysicalOrdinals!;
                if (logical[i].IsNull)
                {
                    physical[mergedOrdinals[0]] = ColumnValue.Null();
                    physical[mergedOrdinals[1]] = ColumnValue.Null();
                }
                else
                {
                    var (hi, lo) = SplitFix128ForMerge(logical[i], col);
                    physical[mergedOrdinals[0]] = hi;
                    physical[mergedOrdinals[1]] = lo;
                }
            }
            else
            {
                int physOrd = col.MergedPhysicalOrdinals?[0] ?? col.Ordinal;
                physical[physOrd] = logical[i];
            }
        }

        return physical;
    }

    /// <summary>
    /// Splits a logical 128-bit value into hi/lo Int64 physical values for merged columns.
    /// Supports GUID and 16-byte BLOB payloads (including DecimalCodec output).
    /// </summary>
    private static (ColumnValue Hi, ColumnValue Lo) SplitFix128ForMerge(ColumnValue value, ColumnInfo column)
    {
        if (column.IsGuidColumn)
        {
            if (value.StorageClass == ColumnStorageClass.UniqueId)
                return ColumnValue.SplitGuidForMerge(value.AsGuid());

            if (value.StorageClass == ColumnStorageClass.Blob)
            {
                var bytes = value.AsBytes().Span;
                if (bytes.Length == 16)
                {
                    long hi = BinaryPrimitives.ReadInt64BigEndian(bytes[..8]);
                    long lo = BinaryPrimitives.ReadInt64BigEndian(bytes.Slice(8, 8));
                    return (ColumnValue.FromInt64(6, hi), ColumnValue.FromInt64(6, lo));
                }
            }

            throw new InvalidOperationException(
                $"Merged GUID column '{column.Name}' requires GUID/UUID or a 16-byte payload.");
        }

        if (column.IsDecimalColumn)
        {
            if (value.StorageClass == ColumnStorageClass.Blob)
            {
                var bytes = value.AsBytes().Span;
                if (bytes.Length == DecimalCodec.ByteCount && DecimalCodec.TryDecode(bytes, out _))
                {
                    long hi = BinaryPrimitives.ReadInt64BigEndian(bytes[..8]);
                    long lo = BinaryPrimitives.ReadInt64BigEndian(bytes.Slice(8, 8));
                    return (ColumnValue.FromInt64(6, hi), ColumnValue.FromInt64(6, lo));
                }
            }

            throw new InvalidOperationException(
                $"Merged decimal column '{column.Name}' requires a valid decimal FIX128 payload.");
        }

        throw new InvalidOperationException(
            $"Merged FIX128 column '{column.Name}' must be declared as GUID/UUID or FIX128.");
    }

    /// <summary>
    /// Inserts multiple records with auto-commit every <paramref name="commitInterval"/> rows.
    /// Bounds memory usage to approximately <paramref name="commitInterval"/> rows worth of dirty pages.
    /// Returns all assigned rowids.
    /// </summary>
    /// <param name="tableName">Target table name.</param>
    /// <param name="records">Records to insert.</param>
    /// <param name="commitInterval">Number of rows per transaction commit. Must be greater than zero.</param>
    public long[] InsertBatch(string tableName, IEnumerable<ColumnValue[]> records, int commitInterval)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(tableName);
        ArgumentNullException.ThrowIfNull(records);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(commitInterval, 0);

        var tableInfo = TryGetTableInfo(tableName);
        var rowIds = records is ICollection<ColumnValue[]> coll ? new List<long>(coll.Count) : new List<long>();
        int count = 0;
        Transaction? tx = null;

        try
        {
            foreach (var values in records)
            {
                tx ??= BeginAutoCommitTransaction();
                rowIds.Add(InsertCore(tx, tableName, values, tableInfo, _tableRootCache));
                count++;

                if (count >= commitInterval)
                {
                    CapturePooledShadow(tx);
                    tx.Commit();
                    tx.Dispose();
                    tx = null;
                    count = 0;
                }
            }

            // Commit remaining rows
            if (tx != null && count > 0)
            {
                CapturePooledShadow(tx);
                tx.Commit();
            }
        }
        finally
        {
            tx?.Dispose();
        }

        return rowIds.ToArray();
    }

    /// <summary>
    /// Returns the maximum rowid in the given table, or 0 if the table is empty.
    /// This is an O(log N) operation that descends to the rightmost B-tree leaf.
    /// Useful for resumable ingestion: after a restart, resume inserting from <c>GetMaxRowId(table) + 1</c>.
    /// </summary>
    public long GetMaxRowId(string tableName)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(tableName);

        var source = _db.PageSource;
        int usableSize = _db.UsablePageSize;
        uint rootPage = FindTableRootPageCached(source, tableName, usableSize, _tableRootCache);
        if (rootPage == 0)
            throw new InvalidOperationException($"Table '{tableName}' not found.");

        return GetMaxRowIdFromBTree(source, rootPage, usableSize);
    }

    /// <summary>
    /// Reads the max rowid by descending to the rightmost leaf of the B-tree.
    /// </summary>
    private static long GetMaxRowIdFromBTree(IPageSource source, uint rootPage, int usableSize)
    {
        Span<byte> pageBuf = stackalloc byte[source.PageSize];
        uint pageNum = rootPage;

        while (true)
        {
            source.ReadPage(pageNum, pageBuf);
            int hdrOff = pageNum == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
            var hdr = BTreePageHeader.Parse(pageBuf[hdrOff..]);

            if (hdr.IsLeaf)
            {
                if (hdr.CellCount == 0) return 0;
                int cellPtr = hdr.GetCellPointer(pageBuf[hdrOff..], hdr.CellCount - 1);
                CellParser.ParseTableLeafCell(pageBuf[cellPtr..], out _, out long maxRowId);
                return maxRowId;
            }

            // Interior page: descend to rightmost child
            pageNum = hdr.RightChildPage;
        }
    }

    /// <summary>
    /// Compacts the database by rebuilding all tables, removing fragmentation and clearing the freelist.
    /// After vacuum, the database file size reflects only the pages actually in use.
    /// </summary>
    public void Vacuum()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var schema = _db.Schema;
        int pageSize = _db.Header.PageSize;
        int usableSize = _db.UsablePageSize;

        // 1. Read all table data (raw record bytes + count)
        int userTableCount = Math.Max(schema.Tables.Count - 1, 0);
        var tableRows = new List<(TableInfo Table, List<byte[]> Records)>(userTableCount);
        foreach (var table in schema.Tables)
        {
            if (table.Name.Equals("sqlite_master", StringComparison.OrdinalIgnoreCase)) continue;
            var records = new List<byte[]>();
            using var cursor = new BTreeCursor<IPageSource>(_db.PageSource, (uint)table.RootPage, usableSize);
            while (cursor.MoveNext())
            {
                records.Add(cursor.Payload.ToArray());
            }
            tableRows.Add((table, records));
        }

        // 2. Build fresh database with same schema
        int schemaPageCount = 1 + tableRows.Count; // page 1 (schema) + one page per table
        var freshData = new byte[pageSize * schemaPageCount];

        // Build header
        var oldHeader = _db.Header;
        var newHeader = new DatabaseHeader(
            pageSize, oldHeader.WriteVersion, oldHeader.ReadVersion,
            oldHeader.ReservedBytesPerPage, oldHeader.ChangeCounter + 1,
            schemaPageCount, 0, 0, // freelist cleared
            oldHeader.SchemaCookie, oldHeader.SchemaFormat,
            oldHeader.TextEncoding, oldHeader.UserVersion,
            oldHeader.ApplicationId, oldHeader.SqliteVersionNumber);
        DatabaseHeader.Write(freshData, newHeader);

        // Build sqlite_master entries (schema page on page 1)
        var schemaCells = new List<(int Size, byte[] Cell)>(tableRows.Count);
        var tableTypeBytes = "table"u8.ToArray(); // cached — same for every entry
        var schemaCols = new ColumnValue[5]; // reuse across iterations
        for (int i = 0; i < tableRows.Count; i++)
        {
            var table = tableRows[i].Table;
            uint rootPage = (uint)(i + 2); // tables start at page 2
            var sqlBytes = Encoding.UTF8.GetBytes(table.Sql);
            var nameBytes = Encoding.UTF8.GetBytes(table.Name);

            schemaCols[0] = ColumnValue.Text(2 * 5 + 13, tableTypeBytes);
            schemaCols[1] = ColumnValue.Text(2 * nameBytes.Length + 13, nameBytes);
            schemaCols[2] = ColumnValue.Text(2 * nameBytes.Length + 13, nameBytes);
            schemaCols[3] = ColumnValue.FromInt64(1, (long)rootPage);
            schemaCols[4] = ColumnValue.Text(2 * sqlBytes.Length + 13, sqlBytes);

            int recSize = RecordEncoder.ComputeEncodedSize(schemaCols);
            var recBuf = new byte[recSize];
            RecordEncoder.EncodeRecord(schemaCols, recBuf);

            long rowId = i + 1;
            int cellSize = CellBuilder.ComputeTableLeafCellSize(rowId, recSize, usableSize);
            var cellBuf = new byte[cellSize];
            CellBuilder.BuildTableLeafCell(rowId, recBuf, cellBuf, usableSize);
            schemaCells.Add((cellSize, cellBuf));
        }

        // Write schema cells to page 1 (after the 100-byte database header)
        int hdrOff = SQLiteLayout.DatabaseHeaderSize; // 100
        ushort cellContentOffset = (ushort)pageSize;
        for (int i = 0; i < schemaCells.Count; i++)
        {
            var (size, cell) = schemaCells[i];
            cellContentOffset -= (ushort)size;
            cell.CopyTo(freshData.AsSpan(cellContentOffset));
            // Write cell pointer
            int ptrOff = hdrOff + SQLiteLayout.TableLeafHeaderSize + i * SQLiteLayout.CellPointerSize;
            BinaryPrimitives.WriteUInt16BigEndian(freshData.AsSpan(ptrOff), cellContentOffset);
        }

        var schemaHdr = new BTreePageHeader(
            BTreePageType.LeafTable, 0, (ushort)schemaCells.Count, cellContentOffset, 0, 0);
        BTreePageHeader.Write(freshData.AsSpan(hdrOff), schemaHdr);

        // Write empty table pages
        for (int i = 0; i < tableRows.Count; i++)
        {
            int pageOff = pageSize * (i + 1);
            var tableHdr = new BTreePageHeader(BTreePageType.LeafTable, 0, 0, (ushort)pageSize, 0, 0);
            BTreePageHeader.Write(freshData.AsSpan(pageOff), tableHdr);
        }

        // 3. Open fresh database and re-insert all rows
        using var freshDb = SharcDatabase.OpenMemory(freshData);
        using var freshWriter = SharcWriter.From(freshDb);

        for (int t = 0; t < tableRows.Count; t++)
        {
            var (table, records) = tableRows[t];
            if (records.Count == 0) continue;

            uint rootPage = (uint)(t + 2);
            using var tx = freshDb.BeginTransaction();
            var mutator = tx.FetchMutator(usableSize);

            for (int r = 0; r < records.Count; r++)
            {
                long rowId = r + 1;
                rootPage = mutator.Insert(rootPage, rowId, records[r]);
            }

            // Update root page in schema if it changed
            if (rootPage != (uint)(t + 2))
            {
                UpdateTableRootPage(tx.GetShadowSource(), table.Name, rootPage, usableSize);
            }

            tx.Commit();
        }

        // 4. Copy all pages from fresh database back to original source
        using var vacuumTx = _db.BeginTransaction();
        var shadow = vacuumTx.GetShadowSource();

        int freshPageCount = freshDb.PageSource.PageCount;
        Span<byte> pageBuf = stackalloc byte[pageSize];

        for (uint p = 1; p <= (uint)freshPageCount; p++)
        {
            freshDb.PageSource.ReadPage(p, pageBuf);
            shadow.WritePage(p, pageBuf);
        }

        // Update header on the vacuumed database
        // Re-read page 1 from shadow, update header with correct page count and cleared freelist
        shadow.ReadPage(1, pageBuf);
        var vacuumHeader = DatabaseHeader.Parse(pageBuf);
        var finalHeader = new DatabaseHeader(
            vacuumHeader.PageSize, vacuumHeader.WriteVersion, vacuumHeader.ReadVersion,
            vacuumHeader.ReservedBytesPerPage, vacuumHeader.ChangeCounter,
            freshPageCount, 0, 0, // freelist cleared
            vacuumHeader.SchemaCookie, vacuumHeader.SchemaFormat,
            vacuumHeader.TextEncoding, vacuumHeader.UserVersion,
            vacuumHeader.ApplicationId, vacuumHeader.SqliteVersionNumber);
        DatabaseHeader.Write(pageBuf, finalHeader);
        shadow.WritePage(1, pageBuf);

        vacuumTx.Commit();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _cachedShadow?.Dispose();
        _cachedShadow = null;
        if (_ownsDb) _db.Dispose();
    }
}
