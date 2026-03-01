// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using Sharc.Core;
using Sharc.Core.Schema;

namespace Sharc;

/// <summary>
/// A pre-resolved reader handle that eliminates schema lookup, cursor creation,
/// and ArrayPool allocation on repeated Seek/Read calls. Created via
/// <see cref="SharcDatabase.PrepareReader(string, string[])"/>.
/// </summary>
/// <remarks>
/// <para>Thread-safe: each thread gets its own cursor+reader via <see cref="ThreadLocal{T}"/>.
/// A single <see cref="PreparedReader"/> instance can be shared across N threads.</para>
/// <para>After the first <see cref="CreateReader"/> call on each thread, the cursor and reader
/// are cached. Subsequent calls reset traversal state via <see cref="IBTreeCursor.Reset"/> and
/// reuse the same buffers, eliminating per-call allocation and setup overhead.</para>
/// <para>Dispose is safe to call concurrently with CreateReader: Dispose acquires an exclusive
/// write lock and waits for all active CreateReader calls to finish before cleaning up.</para>
/// </remarks>
public sealed class PreparedReader : IPreparedReader
{
    // Immutable template — resolved once at PrepareReader() time
    private readonly SharcDatabase _db;
    private readonly TableInfo _table;
    private readonly SharcDataReader.CursorReaderConfig _config;

    // Per-thread execution state
    private readonly ThreadLocal<ReaderSlot> _slot;
    private volatile bool _disposed;

    // Protects _slot access against concurrent Dispose.
    // CreateReader: read lock (concurrent). Dispose: write lock (exclusive).
    private readonly ReaderWriterLockSlim _guard = new(LockRecursionPolicy.NoRecursion);

    private sealed class ReaderSlot : IDisposable
    {
        internal IBTreeCursor Cursor;
        internal SharcDataReader Reader;

        internal ReaderSlot(IBTreeCursor cursor, SharcDataReader reader)
        {
            Cursor = cursor;
            Reader = reader;
            reader.MarkReusable();
        }

        public void Dispose()
        {
            Reader.DisposeForReal();
            Cursor.Dispose();
        }
    }

    internal PreparedReader(SharcDatabase db, TableInfo table, SharcDataReader.CursorReaderConfig config)
    {
        _db = db;
        _table = table;
        _config = config;
        _slot = new ThreadLocal<ReaderSlot>(trackAllValues: true);
    }

    /// <inheritdoc/>
    public SharcDataReader Execute() => CreateReader();

    /// <summary>
    /// Returns the per-thread cached reader, reset for a new Seek/Read pass.
    /// Zero allocation after the first call on each thread.
    /// </summary>
    /// <returns>The cached <see cref="SharcDataReader"/> ready for Seek or Read.</returns>
    /// <exception cref="ObjectDisposedException">The prepared reader has been disposed.</exception>
    public SharcDataReader CreateReader()
    {
        if (!SharcRuntime.IsSingleThreaded) _guard.EnterReadLock();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var slot = _slot.Value;
            if (slot != null)
            {
                slot.Reader.ResetForReuse(null);
                return slot.Reader;
            }

            // First call on this thread: create cursor + reader
            var cursor = _db.CreateTableCursorForPrepared(_table);
            var reader = new SharcDataReader(cursor, _db.Decoder, _config);
            _slot.Value = new ReaderSlot(cursor, reader);
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

        // Write lock: waits for all active CreateReader calls to finish before cleanup.
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
