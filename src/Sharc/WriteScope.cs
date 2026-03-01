// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Diagnostics;
using Sharc.Core;
using Sharc.Core.Schema;

namespace Sharc;

/// <summary>
/// RAII-style write scope that auto-commits on Dispose.
/// This is the recommended way to perform write operations — no explicit
/// BeginTransaction/Commit boilerplate required.
///
/// <para><b>IMPORTANT: Always use a <c>using</c> statement or <c>using var</c> declaration.</b>
/// Unlike C++ RAII, C# does not call Dispose() on garbage collection.
/// Without <c>using</c>, pending writes are rolled back and the file lock
/// is released only when the GC finalizer runs — which may be arbitrarily late
/// or never (on process exit). A <c>Debug.Fail</c> is triggered in the
/// finalizer to catch this bug during development.</para>
///
/// <para>Usage:
/// <code>
/// using var scope = SharcWriter.OpenScope("data.arc");
/// scope.Insert("events", values1);
/// scope.Insert("events", values2);
/// scope.Update("events", 5, newValues);
/// // auto-commits when scope is disposed
/// </code>
/// </para>
///
/// <para>Call <see cref="Flush"/> to commit pending operations to disk mid-scope
/// without ending the scope:
/// <code>
/// using var scope = SharcWriter.OpenScope("data.arc");
/// for (int batch = 0; batch &lt; 10; batch++)
/// {
///     for (int i = 0; i &lt; 1000; i++)
///         scope.Insert("events", MakeRow(i));
///     scope.Flush(); // commits this batch, starts a new transaction
/// }
/// // final batch auto-commits on Dispose
/// </code>
/// </para>
///
/// <para>On exception, the scope auto-rolls back uncommitted operations.
/// Previously flushed batches remain committed.</para>
/// </summary>
public sealed class WriteScope : IDisposable
{
    private readonly SharcDatabase _db;
    private readonly Dictionary<string, uint> _rootCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly bool _ownsDb;
    private Transaction _tx;
    private bool _finalized;
    private bool _disposed;
    private int _pendingOps;
    private int _totalOps;
    private int _flushCount;

    internal WriteScope(SharcDatabase db, bool ownsDb)
    {
        _db = db;
        _ownsDb = ownsDb;
        _tx = db.BeginTransaction();
    }

    /// <summary>
    /// GC safety net. If Dispose() was not called deterministically (missing <c>using</c>),
    /// this finalizer rolls back pending writes and releases the file lock.
    /// Pending operations are lost — this is a bug in the calling code, not a feature.
    /// In DEBUG builds, <c>Debug.Fail</c> fires to make the bug obvious.
    /// </summary>
    ~WriteScope()
    {
        Debug.Fail(
            $"WriteScope was not disposed deterministically. " +
            $"{_pendingOps} pending operation(s) will be rolled back. " +
            $"Always use 'using var scope = SharcWriter.OpenScope(...)' to ensure auto-commit.");
        DisposeCore(fromFinalizer: true);
    }

    /// <summary>Number of write operations pending (since last Flush/Dispose).</summary>
    public int PendingOperations => _pendingOps;

    /// <summary>Total write operations across all flushes.</summary>
    public int TotalOperations => _totalOps;

    /// <summary>Number of times Flush has been called.</summary>
    public int FlushCount => _flushCount;

    /// <summary>
    /// Inserts a record. Returns the assigned rowid.
    /// </summary>
    public long Insert(string tableName, params ColumnValue[] values)
    {
        EnsureActive();
        var tableInfo = TryGetTableInfo(tableName);
        long rowId = SharcWriter.InsertCore(_tx, tableName, values, tableInfo, _rootCache);
        _pendingOps++;
        _totalOps++;
        return rowId;
    }

    /// <summary>
    /// Deletes a record by rowid. Returns true if the row existed.
    /// </summary>
    public bool Delete(string tableName, long rowId)
    {
        EnsureActive();
        bool found = SharcWriter.DeleteCore(_tx, tableName, rowId, _rootCache);
        _pendingOps++;
        _totalOps++;
        return found;
    }

    /// <summary>
    /// Updates a record by rowid. Returns true if the row existed.
    /// </summary>
    public bool Update(string tableName, long rowId, params ColumnValue[] values)
    {
        EnsureActive();
        var tableInfo = TryGetTableInfo(tableName);
        bool found = SharcWriter.UpdateCore(_tx, tableName, rowId, values, tableInfo, _rootCache);
        _pendingOps++;
        _totalOps++;
        return found;
    }

    /// <summary>
    /// Inserts or updates a record by rowid.
    /// </summary>
    public long Upsert(string tableName, long rowId, params ColumnValue[] values)
    {
        EnsureActive();
        var tableInfo = TryGetTableInfo(tableName);
        bool updated = SharcWriter.UpdateCore(_tx, tableName, rowId, values, tableInfo, _rootCache);
        if (!updated)
            SharcWriter.InsertCoreAtRowId(_tx, tableName, rowId, values, tableInfo, _rootCache);
        _pendingOps++;
        _totalOps++;
        return rowId;
    }

    /// <summary>
    /// Executes a DDL statement (CREATE TABLE, CREATE INDEX, etc.).
    /// </summary>
    public void Execute(string sql)
    {
        EnsureActive();
        SharcSchemaWriter.Execute(_db, _tx, sql);
        _pendingOps++;
        _totalOps++;
    }

    /// <summary>
    /// Commits all pending operations to disk and starts a new transaction.
    /// The scope remains open for further writes. Previously flushed data
    /// is durable — it will survive even if a later batch fails.
    ///
    /// <para>Use this for large ingestion workloads where you want periodic
    /// checkpoints without closing and reopening the scope.</para>
    /// </summary>
    /// <returns>The number of operations that were flushed.</returns>
    public int Flush()
    {
        EnsureActive();
        if (_pendingOps == 0) return 0;

        int flushed = _pendingOps;
        _tx.Commit();
        _tx.Dispose();

        // Start a fresh transaction for the next batch
        _tx = _db.BeginTransaction();
        _pendingOps = 0;
        _flushCount++;
        return flushed;
    }

    /// <summary>
    /// Commits all pending operations and completes the scope.
    /// No further writes are allowed after this call.
    /// Use this when you need to detect commit errors explicitly
    /// rather than relying on auto-commit in Dispose.
    /// </summary>
    public void Complete()
    {
        EnsureActive();
        if (_pendingOps > 0)
        {
            _tx.Commit();
        }
        _tx.Dispose();
        _finalized = true;
        _pendingOps = 0;
    }

    /// <summary>
    /// Discards all pending (unflushed) operations. Previously flushed
    /// batches remain committed. No further writes are allowed.
    /// </summary>
    public void Rollback()
    {
        if (_disposed || _finalized) return;
        _tx.Rollback();
        _tx.Dispose();
        _finalized = true;
        _pendingOps = 0;
    }

    private void EnsureActive()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_finalized) throw new InvalidOperationException("Scope already finalized.");
    }

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

    /// <summary>
    /// Auto-commits pending operations if no exception occurred.
    /// Rolls back on exception. Previously flushed batches are unaffected.
    /// </summary>
    public void Dispose()
    {
        DisposeCore(fromFinalizer: false);
        GC.SuppressFinalize(this);
    }

    private void DisposeCore(bool fromFinalizer)
    {
        if (_disposed) return;
        _disposed = true;

        if (!_finalized)
        {
            if (fromFinalizer)
            {
                // Finalizer: cannot commit reliably (managed refs may be collected).
                // Best-effort rollback to release the file lock.
                try { _tx.Rollback(); } catch { /* swallow */ }
                try { _tx.Dispose(); } catch { /* swallow */ }
            }
            else
            {
                // Deterministic: attempt commit, rollback on failure.
                try
                {
                    if (_pendingOps > 0)
                    {
                        _tx.Commit();
                    }
                }
                catch
                {
                    try { _tx.Rollback(); } catch { /* swallow rollback error */ }
                    throw;
                }
                finally
                {
                    _tx.Dispose();
                }
            }
        }

        if (_ownsDb)
        {
            try { _db.Dispose(); } catch when (fromFinalizer) { /* swallow in finalizer */ }
        }
    }
}
