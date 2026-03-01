// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using Sharc.Core;
using Sharc.Core.BTree;
using Sharc.Core.Format;
using Sharc.Core.IO;
using Sharc.Exceptions;

namespace Sharc;

/// <summary>
/// Represents an ACID transaction in Sharc.
/// All writes performed during the transaction are buffered in memory and
/// only persisted to the underlying storage upon <see cref="Commit"/>.
/// </summary>
public sealed class Transaction : IDisposable
{
    private readonly SharcDatabase _db;
    private readonly IWritablePageSource _baseSource;
    private readonly ShadowPageSource _shadowSource;
    private readonly bool _ownsShadow; // false when shadow is pooled by SharcWriter
    private Dictionary<string, HashSet<long>>? _rowMutations;
    private BTreeMutator? _mutator;
    private IndexBTreeMutator? _indexMutator;
    private FreelistManager? _freelistManager;
    private bool _isCompleted;
    private bool _disposed;

    /// <summary>
    /// Gets the transaction-aware page source.
    /// All reads check buffered writes first, and all writes are buffered here.
    /// </summary>
    public IPageSource PageSource => _shadowSource;

    /// <summary>
    /// Returns the shadow page source for pooling by SharcWriter.
    /// </summary>
    internal ShadowPageSource GetShadowSource() => _shadowSource;

    /// <summary>
    /// Returns a cached <see cref="BTreeMutator"/> for this transaction, creating one on first call.
    /// The mutator's page cache is shared across all operations within the transaction.
    /// A fresh mutator and FreelistManager are always created per-transaction to avoid stale freelist state.
    /// </summary>
    internal BTreeMutator FetchMutator(int usablePageSize)
    {
        if (_mutator != null)
            return _mutator;

        // Initialize freelist from the database header
        var header = _db.Header;
        _freelistManager = new FreelistManager(_shadowSource, _shadowSource.PageSize);
        _freelistManager.Initialize(header.FirstFreelistPage, header.FreelistPageCount);

        Func<uint>? freePageAllocator = _freelistManager.HasFreePages ? _freelistManager.PopFreePage : null;

        _mutator = new BTreeMutator(_shadowSource, usablePageSize,
            freePageAllocator, _freelistManager.PushFreePage);
        return _mutator;
    }

    /// <summary>
    /// Returns a cached <see cref="IndexBTreeMutator"/> for this transaction.
    /// Shares the freelist and page number allocator with the table mutator
    /// to prevent both mutators from allocating the same page numbers.
    /// </summary>
    internal IndexBTreeMutator FetchIndexMutator(int usablePageSize)
    {
        if (_indexMutator != null)
            return _indexMutator;

        // Ensure table mutator + freelist are initialized
        var tableMutator = FetchMutator(usablePageSize);

        Func<uint>? freePageAllocator = _freelistManager!.HasFreePages ? _freelistManager.PopFreePage : null;

        // Share the page number allocator with the table mutator so both
        // use the same counter when extending the database file.
        _indexMutator = new IndexBTreeMutator(_shadowSource, usablePageSize,
            freePageAllocator, tableMutator.AllocateNextPage);
        return _indexMutator;
    }

    internal Transaction(SharcDatabase db, IWritablePageSource baseSource)
    {
        _db = db ?? throw new ArgumentNullException(nameof(db));
        _baseSource = baseSource ?? throw new ArgumentNullException(nameof(baseSource));
        _shadowSource = new ShadowPageSource(baseSource);
        _ownsShadow = true;
    }

    /// <summary>
    /// Creates a transaction that reuses a pooled ShadowPageSource.
    /// The shadow must have been Reset() before this call.
    /// Caller owns the shadow lifecycle — this transaction will not dispose it.
    /// The mutator is always created fresh per-transaction.
    /// </summary>
    internal Transaction(SharcDatabase db, IWritablePageSource baseSource,
        ShadowPageSource cachedShadow)
    {
        _db = db ?? throw new ArgumentNullException(nameof(db));
        _baseSource = baseSource ?? throw new ArgumentNullException(nameof(baseSource));
        _shadowSource = cachedShadow;
        _ownsShadow = false;
    }

    /// <summary>
    /// Persists all buffered changes to the underlying storage.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The transaction has been disposed.</exception>
    /// <exception cref="InvalidOperationException">The transaction has already been committed or rolled back.</exception>
    public void Commit()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_isCompleted) throw new InvalidOperationException("Transaction already completed.");

        try
        {
            List<TransactionRowMutation>? committedMutations = null;
            if (_rowMutations is { Count: > 0 })
                committedMutations = FlattenRowMutations(_rowMutations);

            // Track whether B-tree operations occurred (mutator was used)
            bool hadMutator = _mutator != null;

            // Release mutator page buffers before flushing dirty pages.
            _mutator?.Dispose();
            _mutator = null;
            _indexMutator?.Dispose();
            _indexMutator = null;

            if (_shadowSource.DirtyPageCount == 0)
            {
                _isCompleted = true;
                _db.EndTransaction(this);
                return;
            }

            // Update page 1 header only when B-tree operations modified the database.
            // Raw page writes (without mutator) skip header update — the caller manages the header.
            if (hadMutator)
                UpdateDatabaseHeader();

            string? journalPath = null;
            if (_db.FilePath != null)
            {
                journalPath = _db.FilePath + ".journal";
                RollbackJournal.CreateJournal(journalPath, _baseSource, _shadowSource.GetDirtyPageNumbers());
            }

            _shadowSource.WriteDirtyPagesTo(_baseSource);
            _baseSource.Flush();

            if (journalPath != null && File.Exists(journalPath))
            {
                File.Delete(journalPath);
            }

            _isCompleted = true;
            _db.EndTransaction(this);
            if (committedMutations is { Count: > 0 })
                _db.NotifyTransactionCommitted(committedMutations);
        }
        catch (Exception ex)
        {
            throw new SharcException("Failed to commit transaction. Changes may not be fully persisted.", ex);
        }
    }

    /// <summary>
    /// Discards all buffered changes.
    /// </summary>
    public void Rollback()
    {
        if (_disposed || _isCompleted) return;
        _mutator?.Dispose();
        _mutator = null;
        _indexMutator?.Dispose();
        _indexMutator = null;
        _rowMutations?.Clear();
        _shadowSource.ClearShadow();
        _isCompleted = true;
        _db.EndTransaction(this);
    }

    /// <summary>
    /// Writes a page to the transaction buffer.
    /// </summary>
    internal void WritePage(uint pageNumber, ReadOnlySpan<byte> source)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_isCompleted) throw new InvalidOperationException("Transaction already completed.");
        _shadowSource.WritePage(pageNumber, source);
    }

    /// <summary>
    /// Tracks that a row in the given table was mutated in this transaction.
    /// </summary>
    internal void TrackRowMutation(string tableName, long rowId)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_isCompleted) throw new InvalidOperationException("Transaction already completed.");

        _rowMutations ??= new Dictionary<string, HashSet<long>>(StringComparer.OrdinalIgnoreCase);
        if (!_rowMutations.TryGetValue(tableName, out var rows))
        {
            rows = new HashSet<long>();
            _rowMutations[tableName] = rows;
        }
        rows.Add(rowId);
    }

    private uint? _newSchemaCookie;

    /// <summary>
    /// Allocates a new root page for a table.
    /// </summary>
    internal uint AllocateTableRoot(int usablePageSize)
    {
        var mutator = FetchMutator(usablePageSize);
        return mutator.AllocateNewPage();
    }

    /// <summary>
    /// Allocates a new root page for an index (LeafIndex 0x0A).
    /// </summary>
    internal uint AllocateIndexRoot(int usablePageSize)
    {
        var mutator = FetchMutator(usablePageSize);
        return mutator.AllocateNewPage(Core.Format.BTreePageType.LeafIndex);
    }

    /// <summary>
    /// Sets the new schema cookie value to be written to the database header on commit.
    /// </summary>
    internal void SetSchemaCookie(uint cookie)
    {
        _newSchemaCookie = cookie;
    }

    /// <summary>
    /// Executes a DDL statement (CREATE TABLE, ALTER TABLE, etc) within this transaction.
    /// </summary>
    public void Execute(string sql, Core.Trust.AgentInfo? agent = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_isCompleted) throw new InvalidOperationException("Transaction already completed.");

        SharcSchemaWriter.Execute(_db, this, sql, agent);
    }

    /// <summary>
    /// Updates the database header on page 1 with current metadata.
    /// Called during commit to persist PageCount, ChangeCounter, and freelist pointers.
    /// </summary>
    private void UpdateDatabaseHeader()
    {
        int pageSize = _shadowSource.PageSize;

        // Read page 1 (may already be in shadow if modified during this transaction)
        Span<byte> page1 = stackalloc byte[pageSize];
        _shadowSource.ReadPage(1, page1);

        // Parse existing header
        var oldHeader = DatabaseHeader.Parse(page1);

        // Compute new values
        int newPageCount = _shadowSource.PageCount;
        uint newChangeCounter = oldHeader.ChangeCounter + 1;

        // Read freelist state from manager (if active), otherwise preserve existing values
        uint freelistFirstPage = _freelistManager?.FirstTrunkPage ?? oldHeader.FirstFreelistPage;
        int freelistCount = _freelistManager?.FreelistPageCount ?? oldHeader.FreelistPageCount;

        uint schemaCookie = _newSchemaCookie ?? oldHeader.SchemaCookie;

        // Build updated header preserving all fields except PageCount, ChangeCounter, and freelist
        var newHeader = new DatabaseHeader(
            pageSize: oldHeader.PageSize,
            writeVersion: oldHeader.WriteVersion,
            readVersion: oldHeader.ReadVersion,
            reservedBytesPerPage: oldHeader.ReservedBytesPerPage,
            changeCounter: newChangeCounter,
            pageCount: newPageCount,
            firstFreelistPage: freelistFirstPage,
            freelistPageCount: freelistCount,
            schemaCookie: schemaCookie,
            schemaFormat: oldHeader.SchemaFormat,
            textEncoding: oldHeader.TextEncoding,
            userVersion: oldHeader.UserVersion,
            applicationId: oldHeader.ApplicationId,
            sqliteVersionNumber: oldHeader.SqliteVersionNumber
        );

        // Write updated header back to page 1
        DatabaseHeader.Write(page1, newHeader);
        _shadowSource.WritePage(1, page1);
    }

    private static List<TransactionRowMutation> FlattenRowMutations(
        Dictionary<string, HashSet<long>> rowMutations)
    {
        int capacity = 0;
        foreach (var pair in rowMutations)
            capacity += pair.Value.Count;

        var flattened = new List<TransactionRowMutation>(capacity);
        foreach (var pair in rowMutations)
        {
            foreach (long rowId in pair.Value)
                flattened.Add(new TransactionRowMutation(pair.Key, rowId));
        }

        return flattened;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        if (!_isCompleted)
        {
            Rollback();
        }
        _mutator?.Dispose();
        _mutator = null;
        _indexMutator?.Dispose();
        _indexMutator = null;
        if (_ownsShadow)
            _shadowSource.Dispose();
        _disposed = true;
    }
}
