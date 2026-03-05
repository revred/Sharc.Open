// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using Sharc.Core;
using Sharc.Core.IO;
using Sharc.Core.Schema;

namespace Sharc;

/// <summary>
/// A pre-resolved writer handle that eliminates schema lookup and root page
/// scanning on repeated Insert, Delete, and Update calls. Created via
/// <see cref="SharcWriter.PrepareWriter(string)"/>.
/// </summary>
/// <remarks>
/// <para>At construction, <see cref="TableInfo"/> and the table's root page are resolved once.
/// Each mutation auto-commits via a pooled <see cref="ShadowPageSource"/>, matching the
/// <see cref="SharcWriter"/> pooling pattern.</para>
/// <para>Thread-safe: each thread gets its own <see cref="ShadowPageSource"/> cache via
/// <see cref="ThreadLocal{T}"/>. A single instance can be shared across N threads.</para>
/// <para>Dispose is safe to call concurrently with mutations: Dispose acquires an exclusive
/// write lock and waits for all active operations to finish before cleaning up.</para>
/// </remarks>
public sealed class PreparedWriter : IPreparedWriter
{
    private readonly SharcDatabase _db;
    private readonly string _tableName;
    private readonly TableInfo? _tableInfo;
    private readonly Dictionary<string, uint> _rootCache;
    private volatile bool _disposed;

    // Per-thread cached ShadowPageSource
    private readonly ThreadLocal<WriterSlot> _slot;

    // Protects _slot access against concurrent Dispose.
    // Mutations: read lock (concurrent from different threads). Dispose: write lock (exclusive).
    private readonly ReaderWriterLockSlim _guard = new(LockRecursionPolicy.NoRecursion);

    private sealed class WriterSlot : IDisposable
    {
        internal ShadowPageSource? CachedShadow;

        public void Dispose()
        {
            CachedShadow?.Dispose();
            CachedShadow = null;
        }
    }

    internal PreparedWriter(SharcDatabase db, string tableName, TableInfo? tableInfo,
        Dictionary<string, uint> rootCache)
    {
        _db = db;
        _tableName = tableName;
        _tableInfo = tableInfo;
        _rootCache = rootCache;
        _slot = new ThreadLocal<WriterSlot>(trackAllValues: true);
    }

    /// <inheritdoc/>
    public long Insert(params ColumnValue[] values)
    {
        if (!SharcRuntime.IsSingleThreaded) _guard.EnterReadLock();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var db = _db;
            var slot = GetSlot();
            using var tx = BeginAutoCommitTransaction(db, slot);
            long rowId = SharcWriter.InsertCore(tx, _tableName, values, _tableInfo, _rootCache);
            CapturePooledShadow(tx, slot);
            tx.Commit();
            return rowId;
        }
        finally
        {
            if (!SharcRuntime.IsSingleThreaded) _guard.ExitReadLock();
        }
    }

    /// <inheritdoc/>
    public bool Delete(long rowId)
    {
        if (!SharcRuntime.IsSingleThreaded) _guard.EnterReadLock();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var db = _db;
            var slot = GetSlot();
            using var tx = BeginAutoCommitTransaction(db, slot);
            bool found = SharcWriter.DeleteCore(tx, _tableName, rowId, _rootCache);
            CapturePooledShadow(tx, slot);
            tx.Commit();
            return found;
        }
        finally
        {
            if (!SharcRuntime.IsSingleThreaded) _guard.ExitReadLock();
        }
    }

    /// <inheritdoc/>
    public bool Update(long rowId, params ColumnValue[] values)
    {
        if (!SharcRuntime.IsSingleThreaded) _guard.EnterReadLock();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var db = _db;
            var slot = GetSlot();
            using var tx = BeginAutoCommitTransaction(db, slot);
            bool found = SharcWriter.UpdateCore(tx, _tableName, rowId, values, _tableInfo, _rootCache);
            CapturePooledShadow(tx, slot);
            tx.Commit();
            return found;
        }
        finally
        {
            if (!SharcRuntime.IsSingleThreaded) _guard.ExitReadLock();
        }
    }

    private WriterSlot GetSlot() => _slot.Value ??= new WriterSlot();

    private static Transaction BeginAutoCommitTransaction(SharcDatabase db, WriterSlot slot)
    {
        if (slot.CachedShadow == null)
            return db.BeginTransaction();

        return db.BeginPooledTransaction(slot.CachedShadow);
    }

    private static void CapturePooledShadow(Transaction tx, WriterSlot slot)
    {
        slot.CachedShadow ??= tx.GetShadowSource();
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;

        // Write lock: waits for all active mutations to finish before cleanup.
        if (!SharcRuntime.IsSingleThreaded) _guard.EnterWriteLock();
        try
        {
            if (_disposed) return;
            _disposed = true;

            // Clean up all per-thread slots
            foreach (var slot in _slot.Values)
                slot.Dispose();

            _slot.Dispose();
        }
        finally
        {
            if (!SharcRuntime.IsSingleThreaded) _guard.ExitWriteLock();
        }

        if (!SharcRuntime.IsSingleThreaded) _guard.Dispose();
    }
}
