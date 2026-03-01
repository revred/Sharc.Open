// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using Sharc.Core;
using Sharc.Core.Schema;
using Sharc.Query;
using Sharc.Query.Intent;
using Sharc.Query.Optimization;

namespace Sharc;

/// <summary>
/// A pre-compiled query handle that eliminates parse, plan cache, view resolution,
/// and filter compilation overhead on repeated execution. Created via
/// <see cref="SharcDatabase.Prepare(string)"/>.
/// </summary>
/// <remarks>
/// <para>Phase 1 supports simple single-table SELECT queries (with optional WHERE, ORDER BY,
/// LIMIT, GROUP BY). Compound queries, CTEs, and JOINs throw <see cref="NotSupportedException"/>
/// at prepare time.</para>
/// <para>Thread-safe: each thread gets its own cursor+reader+paramCache via
/// <see cref="ThreadLocal{T}"/>. A single <see cref="PreparedQuery"/> instance can be shared
/// across N threads.</para>
/// <para>Dispose is safe to call concurrently with Execute: Dispose acquires an exclusive
/// write lock and waits for all active Execute calls to finish before cleaning up.</para>
/// </remarks>
public sealed class PreparedQuery : IPreparedReader
{
    private readonly SharcDatabase _db;
    private volatile bool _disposed;

    // Pre-resolved at Prepare() time - immutable, shared across threads
    internal readonly TableInfo Table;
    internal readonly int[]? Projection;
    internal readonly int RowidAliasOrdinal;
    internal readonly IFilterNode? StaticFilter;
    internal readonly QueryIntent Intent;
    internal readonly bool NeedsPostProcessing;

    // Pre-resolved at Prepare() time - avoids TryCreateIndexSeekCursor on every Execute()
    private readonly bool _hasIndexSeek;

    // Per-thread execution state
    private readonly ThreadLocal<QuerySlot> _slot;

    // Protects _slot access against concurrent Dispose.
    // Execute: read lock (concurrent). Dispose: write lock (exclusive).
    private readonly ReaderWriterLockSlim _guard = new(LockRecursionPolicy.NoRecursion);

    private sealed class QuerySlot : IDisposable
    {
        internal IBTreeCursor? Cursor;
        internal SharcDataReader? Reader;
        internal Dictionary<long, IFilterNode>? ParamCache;
        internal IReadOnlyDictionary<string, object>? LastParametersRef;
        internal IFilterNode? LastFilterNode;

        public void Dispose()
        {
            if (Reader != null)
            {
                Reader.DisposeForReal();
                Reader = null;
            }
            if (Cursor != null)
            {
                Cursor.Dispose();
                Cursor = null;
            }
            ParamCache?.Clear();
            ParamCache = null;
            LastParametersRef = null;
            LastFilterNode = null;
        }
    }

    internal PreparedQuery(
        SharcDatabase db,
        TableInfo table,
        int[]? projection,
        int rowidAliasOrdinal,
        IFilterNode? staticFilter,
        QueryIntent intent,
        bool needsPostProcessing)
    {
        _db = db;
        Table = table;
        Projection = projection;
        RowidAliasOrdinal = rowidAliasOrdinal;
        StaticFilter = staticFilter;
        Intent = intent;
        NeedsPostProcessing = needsPostProcessing;
        _slot = new ThreadLocal<QuerySlot>(trackAllValues: true);

        // Pre-resolve index seek at Prepare time - determines cursor type once
        var seekCursor = db.TryCreateIndexSeekCursorForPrepared(intent, table);
        if (seekCursor != null)
        {
            _hasIndexSeek = true;
            seekCursor.Dispose();
        }
    }

    /// <summary>
    /// Executes the prepared query with no parameters and returns a data reader.
    /// Skips SQL parsing, plan cache lookup, view resolution, and filter compilation.
    /// </summary>
    /// <returns>A <see cref="SharcDataReader"/> positioned before the first matching row.</returns>
    /// <exception cref="ObjectDisposedException">The prepared query has been disposed.</exception>
    public SharcDataReader Execute()
    {
        return Execute(null);
    }

    /// <summary>
    /// Executes the prepared query with the given parameters and returns a data reader.
    /// Each thread gets its own cached cursor+reader. After the first call on a thread,
    /// subsequent calls reset and reuse, eliminating per-call allocation.
    /// </summary>
    /// <param name="parameters">Named parameters to bind, or null for no parameters.</param>
    /// <returns>A <see cref="SharcDataReader"/> positioned before the first matching row.</returns>
    /// <exception cref="ObjectDisposedException">The prepared query has been disposed.</exception>
    public SharcDataReader Execute(IReadOnlyDictionary<string, object>? parameters)
    {
        if (!SharcRuntime.IsSingleThreaded) _guard.EnterReadLock();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var db = _db;
            var slot = _slot.Value ??= new QuerySlot();

            // Determine filter node to use
            IFilterNode? node = StaticFilter;

            if (node == null && Intent.Filter.HasValue)
            {
                if (ReferenceEquals(parameters, slot.LastParametersRef) && slot.LastFilterNode != null)
                {
                    node = slot.LastFilterNode;
                }
                else
                {
                    // Parameterized filter - check per-thread param cache
                    slot.ParamCache ??= new Dictionary<long, IFilterNode>();
                    long paramKey = ComputeParamCacheKey(parameters);
                    if (!slot.ParamCache.TryGetValue(paramKey, out node))
                    {
                        var filterStar = IntentToFilterBridge.Build(
                            Intent.Filter.Value, parameters, Intent.TableAlias);
                        node = FilterTreeCompiler.CompileBaked(
                            filterStar, Table.Columns, RowidAliasOrdinal);
                        slot.ParamCache[paramKey] = node;
                    }

                    slot.LastParametersRef = parameters;
                    slot.LastFilterNode = node;
                }
            }

            // Fast path: reuse per-thread cached cursor + reader
            if (slot.Reader != null)
            {
                slot.Reader.ResetForReuse(node);

                if (NeedsPostProcessing)
                    return QueryPostProcessor.Apply(slot.Reader, Intent);

                return slot.Reader;
            }

            // First call on this thread: create cursor + reader, cache in slot
            IBTreeCursor cursor;
            if (_hasIndexSeek)
            {
                cursor = db.TryCreateIndexSeekCursorForPrepared(Intent, Table)
                    ?? db.CreateTableCursorForPrepared(Table);
            }
            else
            {
                cursor = db.CreateTableCursorForPrepared(Table);
            }

            var reader = new SharcDataReader(cursor, db.Decoder, new SharcDataReader.CursorReaderConfig
            {
                Columns = Table.Columns,
                Projection = Projection,
                BTreeReader = db.BTreeReaderInternal,
                TableIndexes = Table.Indexes,
                FilterNode = node
            });

            // Cache for reuse - mark reader as owned so Dispose() resets instead of destroying
            reader.MarkReusable();
            slot.Cursor = cursor;
            slot.Reader = reader;

            if (NeedsPostProcessing)
                return QueryPostProcessor.Apply(reader, Intent);

            return reader;
        }
        finally
        {
            if (!SharcRuntime.IsSingleThreaded) _guard.ExitReadLock();
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;

        // Write lock: waits for all active Execute calls to finish before cleanup.
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

    private static long ComputeParamCacheKey(IReadOnlyDictionary<string, object>? parameters)
        => ParameterKeyHasher.Compute(parameters);
}
