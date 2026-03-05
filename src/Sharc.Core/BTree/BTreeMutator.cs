// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Buffers;
using System.Buffers.Binary;
using Sharc.Core.Format;

namespace Sharc.Core.BTree;

/// <summary>
/// Insert/update/delete operations on a table B-tree.
/// Mirrors <see cref="BTreeCursor{TPageSource}"/> for navigation,
/// but modifies pages via <see cref="IWritablePageSource"/>.
/// Page buffers are rented from <see cref="ArrayPool{T}"/> and cached
/// for the lifetime of this instance. Call <see cref="Dispose"/> to return them.
/// </summary>
/// <remarks>
/// TD-12: Refactored into three composable helpers:
/// <see cref="BTreePageRewriter"/> — cell insertion, removal, defragmentation, page building.
/// <see cref="OverflowChainWriter"/> — overflow chain construction for large payloads.
/// BTreeMutator retains tree navigation, public API, and split orchestration.
/// </remarks>
internal sealed class BTreeMutator : IDisposable
{
    private readonly IWritablePageSource _source;
    private readonly int _usablePageSize;
    private readonly Dictionary<uint, byte[]> _pageCache = new(4);
    private readonly List<byte[]> _rentedBuffers = new(4);
    private bool _disposed;

    /// <summary>Header size for an interior table page.</summary>
    private const int InteriorHeaderSize = SQLiteLayout.TableInteriorHeaderSize; // 12

    private readonly Func<uint>? _freePageAllocator;
    private readonly Action<uint>? _freePageCallback;
    private uint _nextAllocPage;

    private readonly BTreePageRewriter _rewriter;
    private readonly OverflowChainWriter _overflowWriter;

    /// <summary>
    /// Initializes a new mutator for the given writable page source.
    /// </summary>
    /// <param name="source">The writable page source to modify.</param>
    /// <param name="usablePageSize">Usable bytes per page (page size minus reserved space).</param>
    /// <param name="freePageAllocator">Optional delegate that returns a free page number from the freelist, or 0 if none available.</param>
    /// <param name="freePageCallback">Optional delegate invoked when a page is freed during split/merge operations.</param>
    public BTreeMutator(IWritablePageSource source, int usablePageSize,
        Func<uint>? freePageAllocator = null, Action<uint>? freePageCallback = null)
    {
        _source = source;
        _usablePageSize = usablePageSize;
        _freePageAllocator = freePageAllocator;
        _freePageCallback = freePageCallback;

        _rewriter = new BTreePageRewriter(source, usablePageSize, _rentedBuffers);
        _overflowWriter = new OverflowChainWriter(
            source, usablePageSize, _pageCache, _rentedBuffers,
            freePageAllocator, AllocateNextPage);
    }

    /// <summary>Number of pages currently held in the internal cache. Exposed for testing.</summary>
    internal int CachedPageCount => _pageCache.Count;

    // ── Public API ─────────────────────────────────────────────────

    /// <summary>
    /// Inserts a record into the table B-tree rooted at <paramref name="rootPage"/>.
    /// Returns the (possibly new) root page number — the root changes when the root page splits.
    /// </summary>
    public uint Insert(uint rootPage, long rowId, ReadOnlySpan<byte> recordPayload)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Build the cell bytes — stackalloc for typical small cells, ArrayPool for large ones.
        int cellSize = CellBuilder.ComputeTableLeafCellSize(rowId, recordPayload.Length, _usablePageSize);
        byte[]? rentedCell = cellSize > 512
            ? ArrayPool<byte>.Shared.Rent(cellSize)
            : null;
        try
        {
            Span<byte> cellBuf = rentedCell != null
                ? rentedCell.AsSpan(0, cellSize)
                : stackalloc byte[cellSize];

            // If the record overflows, write overflow pages and patch the cell's pointer
            CellBuilder.BuildTableLeafCell(rowId, recordPayload, cellBuf, _usablePageSize);
            int inlineSize = CellParser.CalculateInlinePayloadSize(recordPayload.Length, _usablePageSize);
            if (inlineSize < recordPayload.Length)
            {
                _overflowWriter.WriteOverflowChain(cellBuf, recordPayload, inlineSize);
            }

            ReadOnlySpan<byte> cellBytes = cellBuf;

            // Navigate from root to the correct leaf, collecting the ancestor path.
            // Max B-tree depth for SQLite is ~20 (4096-byte pages, min 2 keys per interior page).
            Span<InsertPathEntry> path = stackalloc InsertPathEntry[20];
            int pathCount = 0;
            uint currentPage = rootPage;

            while (true)
            {
                var page = ReadPageBuffer(currentPage);
                int hdrOff = currentPage == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
                var hdr = BTreePageHeader.Parse(page.AsSpan(hdrOff));

                if (hdr.IsLeaf)
                {
                    // Binary search for insertion point
                    int insertIdx = FindLeafInsertionPoint(page, hdrOff, hdr, rowId);
                    path[pathCount++] = new InsertPathEntry(currentPage, insertIdx);
                    break;
                }

                // Interior page — binary search for child
                int childIdx = FindInteriorChild(page, hdrOff, hdr, rowId, out uint childPage);
                path[pathCount++] = new InsertPathEntry(currentPage, childIdx);
                currentPage = childPage;
            }

            // Insert into the leaf (last element in path). Splits propagate upward.
            return InsertCellAndSplit(path[..pathCount], pathCount - 1, cellBytes, rowId, rootPage);
        }
        finally
        {
            if (rentedCell != null)
                ArrayPool<byte>.Shared.Return(rentedCell);
        }
    }

    /// <summary>
    /// Deletes a record from the table B-tree by rowid.
    /// Returns whether the row was found and the (unchanged) root page number.
    /// Interior page keys are NOT modified — they remain as valid routing hints per SQLite spec.
    /// </summary>
    public MutationResult Delete(uint rootPage, long rowId)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Navigate from root to the correct leaf
        uint currentPage = rootPage;
        while (true)
        {
            var page = ReadPageBuffer(currentPage);
            int hdrOff = currentPage == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
            var hdr = BTreePageHeader.Parse(page.AsSpan(hdrOff));

            if (hdr.IsLeaf)
            {
                var (cellIdx, found) = FindLeafCellByRowId(page, hdrOff, hdr, rowId);
                if (!found)
                    return new MutationResult(false, rootPage);

                _rewriter.RemoveCellFromPage(page, hdrOff, hdr, cellIdx);
                WritePageBuffer(currentPage, page);
                return new MutationResult(true, rootPage);
            }

            // Interior page — descend to child
            FindInteriorChild(page, hdrOff, hdr, rowId, out uint childPage);
            currentPage = childPage;
        }
    }

    /// <summary>
    /// Updates a record in the table B-tree by deleting the old record and inserting a new one
    /// with the same rowid. Returns whether the row was found and the (possibly new) root page.
    /// </summary>
    public MutationResult Update(uint rootPage, long rowId, ReadOnlySpan<byte> newRecordPayload)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var (found, root) = Delete(rootPage, rowId);
        if (!found) return new MutationResult(false, root);
        root = Insert(root, rowId, newRecordPayload);
        return new MutationResult(true, root);
    }

    /// <summary>
    /// Returns the maximum rowid in the table B-tree, or 0 if the tree is empty.
    /// </summary>
    public long GetMaxRowId(uint rootPage)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        uint pageNum = rootPage;
        while (true)
        {
            var page = ReadPageBuffer(pageNum);
            int hdrOff = pageNum == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
            var hdr = BTreePageHeader.Parse(page.AsSpan(hdrOff));

            if (hdr.IsLeaf)
            {
                if (hdr.CellCount == 0) return 0;
                // Last cell has the max rowid
                int cellPtr = hdr.GetCellPointer(page.AsSpan(hdrOff), hdr.CellCount - 1);
                CellParser.ParseTableLeafCell(page.AsSpan(cellPtr), out _, out long maxRowId);
                return maxRowId;
            }

            // Interior — descend to right-most child
            pageNum = hdr.RightChildPage;
        }
    }

    /// <summary>
    /// Returns all rented buffers and clears the page cache, but keeps the object reusable.
    /// Dictionary/List capacity is preserved to avoid re-allocation on the next cycle.
    /// </summary>
    public void Reset()
    {
        ReturnRentedBuffers();
        _nextAllocPage = 0;
        _disposed = false;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        ReturnRentedBuffers();
    }

    private void ReturnRentedBuffers()
    {
        _pageCache.Clear();
        foreach (var buf in _rentedBuffers)
            ArrayPool<byte>.Shared.Return(buf);
        _rentedBuffers.Clear();
    }

    // ── Navigation helpers ─────────────────────────────────────────

    /// <summary>
    /// Binary-search a leaf page for an exact rowid match.
    /// Returns the cell index and whether it was an exact match.
    /// </summary>
    private static (int Index, bool Found) FindLeafCellByRowId(byte[] page, int hdrOff, BTreePageHeader hdr, long rowId)
    {
        int lo = 0, hi = hdr.CellCount - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int cellPtr = hdr.GetCellPointer(page.AsSpan(hdrOff), mid);
            CellParser.ParseTableLeafCell(page.AsSpan(cellPtr), out _, out long midRowId);

            if (midRowId < rowId) lo = mid + 1;
            else if (midRowId > rowId) hi = mid - 1;
            else return (mid, true);
        }
        return (lo, false);
    }

    /// <summary>Binary-search a leaf page for the insertion index of <paramref name="rowId"/>.</summary>
    private static int FindLeafInsertionPoint(byte[] page, int hdrOff, BTreePageHeader hdr, long rowId)
    {
        int lo = 0, hi = hdr.CellCount - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int cellPtr = hdr.GetCellPointer(page.AsSpan(hdrOff), mid);
            CellParser.ParseTableLeafCell(page.AsSpan(cellPtr), out _, out long midRowId);

            if (midRowId < rowId) lo = mid + 1;
            else if (midRowId > rowId) hi = mid - 1;
            else return mid; // exact match — overwrite position
        }
        return lo; // insertion point
    }

    /// <summary>
    /// Binary-search an interior page to find the child to descend into.
    /// Also returns the cell index for the ancestor path.
    /// </summary>
    private static int FindInteriorChild(byte[] page, int hdrOff, BTreePageHeader hdr,
        long rowId, out uint childPage)
    {
        int lo = 0, hi = hdr.CellCount - 1;
        int idx = -1;

        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int cellPtr = hdr.GetCellPointer(page.AsSpan(hdrOff), mid);
            CellParser.ParseTableInteriorCell(page.AsSpan(cellPtr), out _, out long key);

            if (key >= rowId) { idx = mid; hi = mid - 1; }
            else lo = mid + 1;
        }

        if (idx != -1)
        {
            int cellPtr = hdr.GetCellPointer(page.AsSpan(hdrOff), idx);
            CellParser.ParseTableInteriorCell(page.AsSpan(cellPtr), out uint leftChild, out _);
            childPage = leftChild;
            return idx;
        }

        // rowId > all keys → descend to rightmost child
        childPage = hdr.RightChildPage;
        return hdr.CellCount; // index beyond last cell (signals right-child descent)
    }

    // ── Insert + split engine ──────────────────────────────────────

    /// <summary>
    /// Inserts a cell into the page at <paramref name="pathIndex"/> in the ancestor path.
    /// If the page is full, splits it and promotes the median into the parent.
    /// Returns the (possibly new) root page number.
    /// </summary>
    private uint InsertCellAndSplit(
        Span<InsertPathEntry> path,
        int pathIndex,
        ReadOnlySpan<byte> cellBytes,
        long promotedRowId,
        uint currentRoot)
    {
        var (pageNum, insertIdx) = path[pathIndex];
        var pageBuf = ReadPageBuffer(pageNum);
        int hdrOff = pageNum == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
        var hdr = BTreePageHeader.Parse(pageBuf.AsSpan(hdrOff));

        // Try to insert into existing page
        if (_rewriter.TryInsertCell(pageBuf, hdrOff, hdr, insertIdx, cellBytes))
        {
            WritePageBuffer(pageNum, pageBuf);
            return currentRoot;
        }

        // ── Page is full — split ──────────────────────────────────

        // Gather all existing cells + the new cell into a contiguous buffer.
        var (cellBuf, refCount) = _rewriter.GatherCellsWithInsertion(pageBuf, hdrOff, hdr, insertIdx, cellBytes);
        var cells = new ReadOnlySpan<BTreePageRewriter.CellRef>(_rewriter.CellRefBuffer, 0, refCount);

        // Split point: divide roughly in half by total byte count
        int totalBytes = 0;
        for (int i = 0; i < cells.Length; i++) totalBytes += cells[i].Length;
        int halfTarget = totalBytes / 2;

        int splitIdx = 0;
        int runningBytes = 0;
        for (int i = 0; i < cells.Length; i++)
        {
            runningBytes += cells[i].Length;
            if (runningBytes >= halfTarget) { splitIdx = i; break; }
        }

        // Ensure split leaves at least 1 cell on each side
        if (splitIdx == 0) splitIdx = 1;
        if (splitIdx >= refCount - 1) splitIdx = refCount - 2;

        // For leaf pages: left gets [0..splitIdx], right gets [splitIdx+1..end]
        // The median (splitIdx) is promoted to parent
        bool isLeaf = hdr.IsLeaf;

        // Parse the median cell to get the promoted rowId
        var medianRef = cells[splitIdx];
        var medianSpan = cellBuf.AsSpan(medianRef.Offset, medianRef.Length);
        long medianRowId;
        if (isLeaf)
        {
            CellParser.ParseTableLeafCell(medianSpan, out _, out medianRowId);
        }
        else
        {
            CellParser.ParseTableInteriorCell(medianSpan, out _, out medianRowId);
        }

        // Allocate a new page for the right sibling
        uint newPageNum = AllocateNewPage();

        // Build left page (reuse existing page buffer)
        if (isLeaf)
        {
            _rewriter.BuildLeafPage(pageBuf, hdrOff, cellBuf, cells[..(splitIdx + 1)]);
            WritePageBuffer(pageNum, pageBuf);

            var rightBuf = RentPageBuffer();
            _rewriter.BuildLeafPage(rightBuf, 0, cellBuf, cells[(splitIdx + 1)..]);
            WritePageBuffer(newPageNum, rightBuf);
        }
        else
        {
            CellParser.ParseTableInteriorCell(medianSpan, out uint medianLeftChild, out _);

            _rewriter.BuildInteriorPage(pageBuf, hdrOff, cellBuf, cells[..splitIdx], medianLeftChild);
            WritePageBuffer(pageNum, pageBuf);

            var rightBuf = RentPageBuffer();
            uint originalRightChild = hdr.RightChildPage;
            _rewriter.BuildInteriorPage(rightBuf, 0, cellBuf, cells[(splitIdx + 1)..], originalRightChild);
            WritePageBuffer(newPageNum, rightBuf);
        }

        if (pathIndex == 0)
        {
            // Root split with retention:
            uint newLeftPage = AllocateNewPage();

            // CRITICAL: When splitting root on page 1, the left content contains the
            // 100-byte database header at offset 0. The B-tree data starts at offset 100.
            // The new left page is a normal page (not page 1), so its B-tree data must
            // start at offset 0. Rebuild it rather than copying verbatim.
            if (pageNum == 1)
            {
                var newLeftBuf = RentPageBuffer();
                if (isLeaf)
                    _rewriter.BuildLeafPage(newLeftBuf, 0, cellBuf, cells[..(splitIdx + 1)]);
                else
                {
                    CellParser.ParseTableInteriorCell(medianSpan, out uint mlc, out _);
                    _rewriter.BuildInteriorPage(newLeftBuf, 0, cellBuf, cells[..splitIdx], mlc);
                }
                WritePageBuffer(newLeftPage, newLeftBuf);
            }
            else
            {
                var leftContent = ReadPageBuffer(pageNum);
                WritePageBuffer(newLeftPage, leftContent);
            }

            var rootBuf = RentPageBuffer();

            // CRITICAL: If we are splitting the root on Page 1, we MUST preserve the 100-byte database header.
            if (pageNum == 1)
            {
                pageBuf.AsSpan(0, SQLiteLayout.DatabaseHeaderSize).CopyTo(rootBuf);
            }

            Span<byte> interiorCell = stackalloc byte[16];
            int interiorSize = CellBuilder.BuildTableInteriorCell(newLeftPage, medianRowId, interiorCell);

            var newRootHdr = new BTreePageHeader(
                BTreePageType.InteriorTable,
                0, 1,
                (ushort)(_usablePageSize - interiorSize),
                0,
                newPageNum
            );

            int rootHdrOff = pageNum == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
            BTreePageHeader.Write(rootBuf.AsSpan(rootHdrOff), newRootHdr);

            int cellPtrOff = rootHdrOff + InteriorHeaderSize;
            ushort cellContentOff = (ushort)(_usablePageSize - interiorSize);
            BinaryPrimitives.WriteUInt16BigEndian(rootBuf.AsSpan(cellPtrOff), cellContentOff);

            interiorCell[..interiorSize].CopyTo(rootBuf.AsSpan(cellContentOff));

            WritePageBuffer(pageNum, rootBuf);

            return currentRoot;
        }
        else
        {
            Span<byte> interiorCell = stackalloc byte[16];
            int interiorSize = CellBuilder.BuildTableInteriorCell(pageNum, medianRowId, interiorCell);

            UpdateParentAfterSplit(path, pathIndex - 1, interiorCell[..interiorSize], newPageNum);

            return InsertCellAndSplit(path, pathIndex - 1, interiorCell[..interiorSize], medianRowId, currentRoot);
        }
    }

    /// <summary>
    /// After splitting, update the parent page so that the pointer that previously led to
    /// the old child now properly references both the old page and the new page.
    /// </summary>
    private void UpdateParentAfterSplit(
        Span<InsertPathEntry> path,
        int parentPathIndex,
        ReadOnlySpan<byte> promotedCell,
        uint newRightChild)
    {
        var (parentPageNum, parentCellIdx) = path[parentPathIndex];
        var parentBuf = ReadPageBuffer(parentPageNum);
        int hdrOff = parentPageNum == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
        var hdr = BTreePageHeader.Parse(parentBuf.AsSpan(hdrOff));

        if (parentCellIdx >= hdr.CellCount)
        {
            BinaryPrimitives.WriteUInt32BigEndian(
                parentBuf.AsSpan(hdrOff + SQLiteLayout.RightChildPageOffset), newRightChild);
            WritePageBuffer(parentPageNum, parentBuf);
        }
        else
        {
            int cellPtr = hdr.GetCellPointer(parentBuf.AsSpan(hdrOff), parentCellIdx);
            BinaryPrimitives.WriteUInt32BigEndian(parentBuf.AsSpan(cellPtr), newRightChild);
            WritePageBuffer(parentPageNum, parentBuf);
        }
    }

    // ── I/O helpers ────────────────────────────────────────────────

    /// <summary>Reads a page into a cached buffer, renting from the pool if not already cached.</summary>
    private byte[] ReadPageBuffer(uint pageNumber)
    {
        if (_pageCache.TryGetValue(pageNumber, out var cached))
            return cached;

        var buf = RentPageBuffer();
        _source.ReadPage(pageNumber, buf.AsSpan(0, _source.PageSize));
        _pageCache[pageNumber] = buf;
        return buf;
    }

    /// <summary>Writes a page buffer to the underlying page source and caches it.</summary>
    private void WritePageBuffer(uint pageNumber, byte[] buffer)
    {
        _source.WritePage(pageNumber, buffer.AsSpan(0, _source.PageSize));
        _pageCache[pageNumber] = buffer;
    }

    /// <summary>Rents a page-sized buffer from the pool, clears it, and tracks it for return on Dispose.</summary>
    private byte[] RentPageBuffer()
    {
        var buf = ArrayPool<byte>.Shared.Rent(_source.PageSize);
        buf.AsSpan(0, _source.PageSize).Clear();
        _rentedBuffers.Add(buf);
        return buf;
    }

    /// <summary>
    /// Provides the next auto-allocated page number for overflow/split operations.
    /// Called by <see cref="OverflowChainWriter"/> when the freelist is empty.
    /// Also exposed to <see cref="IndexBTreeMutator"/> via delegate to prevent
    /// double-allocation when both mutators extend the file within the same transaction.
    /// </summary>
    internal uint AllocateNextPage()
    {
        if (_nextAllocPage == 0)
            _nextAllocPage = (uint)_source.PageCount + 1;
        return _nextAllocPage++;
    }

    /// <summary>
    /// Allocates a new page initialized as a leaf table page.
    /// </summary>
    internal uint AllocateNewPage() => AllocateNewPage(BTreePageType.LeafTable);

    /// <summary>
    /// Allocates a new page with the specified B-tree page type, first trying the freelist, then extending the file.
    /// </summary>
    internal uint AllocateNewPage(BTreePageType pageType)
    {
        uint page = _freePageAllocator?.Invoke() ?? 0;

        if (page == 0)
            page = AllocateNextPage();

        var buf = RentPageBuffer();
        var hdr = new BTreePageHeader(
            pageType, 0, 0,
            (ushort)_usablePageSize, 0, 0
        );
        BTreePageHeader.Write(buf, hdr);
        _pageCache[page] = buf;
        WritePageBuffer(page, buf);

        return page;
    }
}
