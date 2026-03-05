// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.


using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sharc.Core.Format;
using Sharc.Exceptions;

namespace Sharc.Core.BTree;

/// <summary>
/// Forward-only cursor that traverses a table b-tree in rowid order.
/// Uses an inline union stack to track position through interior pages and descends to leaf pages.
/// </summary>
internal sealed class BTreeCursor<TPageSource> : IBTreeCursor
    where TPageSource : class, IPageSource
{
    private readonly TPageSource _pageSource;
    private readonly int _usablePageSize;

    // Inline union stack — 8 packed ulongs, zero heap allocation
    private CursorStack _stack;
    private ulong[]? _stackOverflow;
    private int _stackTop;
    private const int StackCapacity = 8;

    // Current leaf page state — union-projected: only CellCount from full header
    private uint _currentLeafPage;
    private ushort _leafCellCount;
    private int _currentCellIndex;

    // State flags — union of 3 bools into 1 byte
    private byte _state;
    private const byte StateInitialized = 1;
    private const byte StateExhausted = 2;
    private const byte StateDisposed = 4;

    // Leaf page cache — self-healing via _cachedLeafPageNum
    private ReadOnlyMemory<byte> _cachedLeafMemory;
    private uint _cachedLeafPageNum;

    // Same-leaf fast path: rowid range of cached leaf. When _cachedLeafMaxRowId == long.MinValue
    // the range is invalid. Uses _cachedLeafPageNum != 0 as secondary guard.
    // These two longs piggyback on the existing _cachedLeafMemory/PageNum cache —
    // same lifecycle, same invalidation path. No extra validity flag needed.
    private long _cachedLeafMinRowId;
    private long _cachedLeafMaxRowId = long.MinValue;

    // Current cell data
    private long _rowId;
    private int _payloadSize;
    private byte[]? _assembledPayload;
    private int _inlinePayloadOffset;

    // Reusable overflow cycle detection set - cleared between overflow assemblies
    private HashSet<uint>? _visitedOverflowPages;

    private readonly uint _rootPage;
    private readonly IWritablePageSource? _writableSource;
    private long _snapshotVersion;

    /// <summary>
    /// Initializes a new cursor positioned before the first row of the table B-tree.
    /// </summary>
    /// <param name="pageSource">The page source for reading B-tree pages.</param>
    /// <param name="rootPage">The root page number of the table B-tree.</param>
    /// <param name="usablePageSize">Usable bytes per page (page size minus reserved space).</param>
    public BTreeCursor(TPageSource pageSource, uint rootPage, int usablePageSize)
    {
        _pageSource = pageSource;
        _rootPage = rootPage;
        _usablePageSize = usablePageSize;
        _writableSource = pageSource as IWritablePageSource;
    }

    /// <inheritdoc />
    public long RowId => _rowId;

    /// <inheritdoc />
    public int PayloadSize => _payloadSize;

    /// <inheritdoc />
    public bool IsStale
    {
        get
        {
            if (_writableSource is null) return false;
            long current = _writableSource.DataVersion;
            if (_snapshotVersion == 0)
            {
                _snapshotVersion = current;
                return false;
            }
            return current != _snapshotVersion;
        }
    }

    /// <inheritdoc />
    public ReadOnlySpan<byte> Payload
    {
        get
        {
            if (_assembledPayload != null)
                return _assembledPayload.AsSpan(0, _payloadSize);

            // Return inline payload from cached leaf page
            return GetCachedLeafPage().Slice(_inlinePayloadOffset, _payloadSize);
        }
    }

    /// <inheritdoc />
    public void Reset()
    {
        ReturnAssembledPayload();
        _stackTop = 0;
        _state = 0;
        // Don't clear _currentLeafPage — it's used by Seek() same-rowid fast path.
        // MoveNext() uses StateInitialized (cleared above) so it will re-descend properly.
        // Preserve _cachedLeafPageNum, _cachedLeafMemory, _cachedLeafMinRowId, _cachedLeafMaxRowId
        // so that Seek() can use the same-leaf fast path after Reset().
        _snapshotVersion = _writableSource?.DataVersion ?? 0;
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MoveNext()
    {
        // Return any previously assembled overflow buffer
        ReturnAssembledPayload();

        if ((_state & StateExhausted) != 0)
            return false;

        Debug.Assert((_state & StateDisposed) == 0, "MoveNext called on disposed cursor");

        if ((_state & StateInitialized) == 0)
        {
            _state |= StateInitialized;
            if (_snapshotVersion == 0)
                _snapshotVersion = _writableSource?.DataVersion ?? 0;
            DescendToLeftmostLeaf(_rootPage);
        }

        return AdvanceToNextCell();
    }

    /// <inheritdoc />
    public bool MoveLast()
    {
        ObjectDisposedException.ThrowIf((_state & StateDisposed) != 0, this);
        ReturnAssembledPayload();
        _stackTop = 0;
        _state = (byte)((_state & ~StateExhausted) | StateInitialized);

        uint pageNum = _rootPage;
        while (true)
        {
             var page = _pageSource.GetPage(pageNum);
             int headerOffset = pageNum == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
             var header = BTreePageHeader.Parse(page[headerOffset..]);

             if (header.IsLeaf)
             {
                 _currentLeafPage = pageNum;
                 _leafCellCount = header.CellCount;

                 if (header.CellCount == 0)
                 {
                     _state |= StateExhausted;
                     return false;
                 }

                 _currentCellIndex = header.CellCount - 1;
                 ParseCurrentLeafCell();
                 return true;
             }

             // Interior page - follow right pointer
             pageNum = header.RightChildPage;
        }
    }

    /// <inheritdoc />
    public bool Seek(long rowId)
    {
        ObjectDisposedException.ThrowIf((_state & StateDisposed) != 0, this);
        ReturnAssembledPayload();

        // Check data version for writable sources — must run before fast paths
        // so that IsStale is always refreshed and leaf cache is invalidated on writes.
        bool versionChanged = false;
        if (_writableSource != null)
        {
            long ver = _writableSource.DataVersion;
            versionChanged = ver != _snapshotVersion;
            _snapshotVersion = ver;
            if (versionChanged)
            {
                // Data changed — invalidate leaf cache
                _cachedLeafMaxRowId = long.MinValue;
                _cachedLeafPageNum = 0;
            }
        }

        // ── Same-rowid fast path ─────────────────────────────────────
        // If seeking to the exact same rowid as the last successful Seek
        // and data hasn't changed, the cell data is still valid — skip all traversal.
        // Uses _currentLeafPage != 0 to verify a prior Seek() actually landed on a cell.
        if (!versionChanged && _currentLeafPage != 0 && _rowId == rowId
            && _currentCellIndex < _leafCellCount && _assembledPayload == null)
        {
            _state = (byte)((_state & ~StateExhausted) | StateInitialized);
            return true;
        }

        _stackTop = 0;
        _state = (byte)((_state & ~StateExhausted) | StateInitialized);

        // ── Same-leaf fast path ──────────────────────────────────────
        // If the cached leaf page has a valid rowid range and the target falls
        // within it, skip interior page traversal entirely and binary search
        // only within the cached leaf. Saves 2+ GetPage() calls (interior pages).
        if (_cachedLeafPageNum != 0 && rowId >= _cachedLeafMinRowId && rowId <= _cachedLeafMaxRowId)
        {
            _currentLeafPage = _cachedLeafPageNum;
            var page = _cachedLeafMemory.Span;

            // We know this is a leaf page — skip full header parse.
            // Leaf cell pointer array starts at headerOffset + 8 (TableLeafHeaderSize).
            int headerOffset = _currentLeafPage == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
            int cellPtrBase = headerOffset + SQLiteLayout.TableLeafHeaderSize;

            // Binary search within the cached leaf
            int low = 0;
            int high = _leafCellCount - 1;
            while (low <= high)
            {
                int mid = low + ((high - low) >> 1);
                int cellOffset = BinaryPrimitives.ReadUInt16BigEndian(page[(cellPtrBase + mid * 2)..]);
                CellParser.ParseTableLeafCell(page[cellOffset..], out int _, out long cellRowId);

                if (cellRowId < rowId)
                    low = mid + 1;
                else if (cellRowId > rowId)
                    high = mid - 1;
                else
                {
                    _currentCellIndex = mid;
                    ParseCurrentLeafCell();
                    return true;
                }
            }

            _currentCellIndex = low;
            if (_currentCellIndex < _leafCellCount)
            {
                ParseCurrentLeafCell();
                return false;
            }
            // Target not in this leaf after all — fall through to full descent
        }

        bool exactMatch = DescendToLeaf(_rootPage, rowId);

        if (_currentCellIndex < _leafCellCount)
        {
            ParseCurrentLeafCell();
            return exactMatch;
        }
        else
        {
            // Moved past end of leaf, try next leaf
            if (MoveToNextLeaf())
            {
                _currentCellIndex = 0;
                ParseCurrentLeafCell();
                return false;
            }
            else
            {
                _state |= StateExhausted;
                return false;
            }
        }
    }

    private void DescendToLeftmostLeaf(uint pageNumber)
    {
        while (true)
        {
            var page = _pageSource.GetPage(pageNumber);
            int headerOffset = pageNumber == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
            var header = BTreePageHeader.Parse(page[headerOffset..]);

            if (header.IsLeaf)
            {
                _currentLeafPage = pageNumber;
                _leafCellCount = header.CellCount;
                // Pre-cache the leaf so first ParseCurrentLeafCell gets a free cache hit
                _cachedLeafMemory = _pageSource.GetPageMemory(pageNumber);
                _cachedLeafPageNum = pageNumber;
                _currentCellIndex = -1; // Will be incremented by AdvanceToNextCell
                return;
            }

            // Interior page - push onto stack and descend to leftmost child
            if (header.CellCount == 0)
            {
                // Interior page with no cells - go to right child
                StackPush(pageNumber, 0);
                pageNumber = header.RightChildPage;
                continue;
            }

            // Push this interior page (starting before first cell)
            StackPush(pageNumber, 0);

            // Descend to the left child of the first cell
            // Read the single cell pointer on-demand (no array allocation)
            ushort cellPtr = header.GetCellPointer(page[headerOffset..], 0);
            uint leftChild = BinaryPrimitives.ReadUInt32BigEndian(page[cellPtr..]);
            pageNumber = leftChild;
        }
    }

    private bool DescendToLeaf(uint pageNumber, long targetRowId)
    {
        while (true)
        {
            var page = _pageSource.GetPage(pageNumber);
            int headerOffset = pageNumber == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
            var header = BTreePageHeader.Parse(page[headerOffset..]);

            if (header.IsLeaf)
            {
                _currentLeafPage = pageNumber;
                _leafCellCount = header.CellCount;

                // Pre-cache the leaf so ParseCurrentLeafCell gets a free cache hit
                _cachedLeafMemory = _pageSource.GetPageMemory(pageNumber);
                _cachedLeafPageNum = pageNumber;
                page = _cachedLeafMemory.Span;

                // Cache the rowid range for same-leaf fast path on next Seek()
                if (header.CellCount > 0)
                {
                    int firstCellOff = header.GetCellPointer(page[headerOffset..], 0);
                    CellParser.ParseTableLeafCell(page[firstCellOff..], out _, out _cachedLeafMinRowId);
                    int lastCellOff = header.GetCellPointer(page[headerOffset..], header.CellCount - 1);
                    CellParser.ParseTableLeafCell(page[lastCellOff..], out _, out _cachedLeafMaxRowId);
                }
                else
                {
                    _cachedLeafMaxRowId = long.MinValue;
                }

                // Binary search leaf cells using on-demand pointer reads
                int low = 0;
                int high = header.CellCount - 1;
                while (low <= high)
                {
                    int mid = low + ((high - low) >> 1);
                    int cellOffset = header.GetCellPointer(page[headerOffset..], mid);

                    CellParser.ParseTableLeafCell(page[cellOffset..], out int _, out long rowId);

                    if (rowId < targetRowId)
                        low = mid + 1;
                    else if (rowId > targetRowId)
                        high = mid - 1;
                    else
                    {
                        _currentCellIndex = mid;
                        return true;
                    }
                }

                _currentCellIndex = low;
                return false;
            }

            // Interior page - binary search for the correct child
            int idx = -1;
            int l = 0;
            int r = header.CellCount - 1;

            while (l <= r)
            {
                int mid = l + ((r - l) >> 1);
                int cellOffset = header.GetCellPointer(page[headerOffset..], mid);
                CellParser.ParseTableInteriorCell(page[cellOffset..], out _, out long key);

                if (key >= targetRowId)
                {
                    idx = mid;
                    r = mid - 1;
                }
                else
                {
                    l = mid + 1;
                }
            }

            if (idx != -1)
            {
                StackPush(pageNumber, idx);
                int cellOffset = header.GetCellPointer(page[headerOffset..], idx);
                CellParser.ParseTableInteriorCell(page[cellOffset..], out uint leftChild, out _);
                pageNumber = leftChild;
            }
            else
            {
                pageNumber = header.RightChildPage;
            }
        }
    }

    private bool AdvanceToNextCell()
    {
        _currentCellIndex++;

        while (true)
        {
            if (_currentCellIndex < _leafCellCount)
            {
                // Parse the current leaf cell
                ParseCurrentLeafCell();
                return true;
            }

            // Current leaf exhausted - try to move to next leaf via stack
            if (!MoveToNextLeaf())
            {
                _state |= StateExhausted;
                return false;
            }

            _currentCellIndex = 0;
        }
    }

    private bool MoveToNextLeaf()
    {
        while (_stackTop > 0)
        {
            ulong frame = StackPop();
            uint pageId = CursorStack.PageId(frame);
            int nextCellIndex = CursorStack.CellIndex(frame) + 1;

            // Re-derive header from cached page (interior pages are in page source cache)
            var interiorPage = _pageSource.GetPage(pageId);
            int headerOffset = pageId == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
            var header = BTreePageHeader.Parse(interiorPage[headerOffset..]);

            if (nextCellIndex < header.CellCount)
            {
                // More cells in this interior page — push updated state and descend
                StackPush(pageId, nextCellIndex);

                // Read the single cell pointer on-demand (no array allocation)
                ushort cellPtr = header.GetCellPointer(interiorPage[headerOffset..], nextCellIndex);
                uint leftChild = BinaryPrimitives.ReadUInt32BigEndian(interiorPage[cellPtr..]);

                DescendToLeftmostLeaf(leftChild);
                return true;
            }

            // All cells exhausted - descend to right child
            if (header.RightChildPage != 0)
            {
                DescendToLeftmostLeaf(header.RightChildPage);
                return true;
            }
        }

        return false;
    }

    private void ParseCurrentLeafCell()
    {
        var page = GetCachedLeafPage();
        // Inline cell pointer math — leaf HeaderSize is always 8
        int ho = _currentLeafPage == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
        int cellOffset = BinaryPrimitives.ReadUInt16BigEndian(
            page[(ho + SQLiteLayout.TableLeafHeaderSize + _currentCellIndex * 2)..]);

        int cellHeaderSize = CellParser.ParseTableLeafCell(
            page[cellOffset..], out _payloadSize, out _rowId);

        int payloadStart = cellOffset + cellHeaderSize;
        int inlineSize = CellParser.CalculateInlinePayloadSize(_payloadSize, _usablePageSize);

        if (inlineSize >= _payloadSize)
        {
            // All payload is inline — offset into cached leaf memory
            _inlinePayloadOffset = payloadStart;
            _assembledPayload = null;
        }
        else
        {
            // Overflow - assemble the full payload
            AssembleOverflowPayload(page, payloadStart, inlineSize);
        }
    }

    /// <summary>
    /// Returns the current leaf page data, caching it to avoid redundant GetPage()/GetPageMemory() calls
    /// when iterating multiple cells on the same leaf.
    /// </summary>
    private ReadOnlySpan<byte> GetCachedLeafPage()
    {
        if (_currentLeafPage != _cachedLeafPageNum)
        {
            _cachedLeafMemory = _pageSource.GetPageMemory(_currentLeafPage);
            _cachedLeafPageNum = _currentLeafPage;
        }
        return _cachedLeafMemory.Span;
    }

    private void AssembleOverflowPayload(ReadOnlySpan<byte> page, int payloadStart, int inlineSize)
    {
        _assembledPayload = ArrayPool<byte>.Shared.Rent(_payloadSize);

        // Copy inline portion
        page.Slice(payloadStart, inlineSize).CopyTo(_assembledPayload);

        // Read overflow page pointer (4 bytes after inline payload)
        uint overflowPage = BinaryPrimitives.ReadUInt32BigEndian(
            page[(payloadStart + inlineSize)..]);

        int remaining = _payloadSize - inlineSize;
        int destOffset = inlineSize;
        int overflowDataSize = _usablePageSize - 4;

        // Reuse the HashSet instead of allocating a new one per overflow cell
        _visitedOverflowPages ??= new HashSet<uint>();
        _visitedOverflowPages.Clear();

        while (overflowPage != 0 && remaining > 0)
        {
            if (!_visitedOverflowPages.Add(overflowPage))
                throw new CorruptPageException(overflowPage,
                    "Overflow page chain cycle detected.");

            var ovfPage = _pageSource.GetPage(overflowPage);
            int toCopy = Math.Min(remaining, overflowDataSize);
            ovfPage.Slice(4, toCopy).CopyTo(_assembledPayload.AsSpan(destOffset));

            destOffset += toCopy;
            remaining -= toCopy;

            overflowPage = BinaryPrimitives.ReadUInt32BigEndian(ovfPage);
        }
    }

    private void ReturnAssembledPayload()
    {
        if (_assembledPayload != null)
        {
            ArrayPool<byte>.Shared.Return(_assembledPayload);
            _assembledPayload = null;
        }
    }

    private void StackPush(uint pageId, int cellIndex)
    {
        ulong packed = CursorStack.Pack(pageId, cellIndex);
        if (_stackTop < StackCapacity)
        {
            _stack[_stackTop++] = packed;
            return;
        }
        PushOverflow(packed);
    }

    private void PushOverflow(ulong packed)
    {
        _stackOverflow ??= new ulong[8];
        int idx = _stackTop - StackCapacity;
        if (idx == _stackOverflow.Length)
            Array.Resize(ref _stackOverflow, _stackOverflow.Length * 2);
        _stackOverflow[idx] = packed;
        _stackTop++;
    }

    private ulong StackPop()
    {
        --_stackTop;
        return _stackTop < StackCapacity ? _stack[_stackTop] : _stackOverflow![_stackTop - StackCapacity];
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if ((_state & StateDisposed) != 0) return;
        _state |= StateDisposed | StateExhausted;
        ReturnAssembledPayload();
    }
}
