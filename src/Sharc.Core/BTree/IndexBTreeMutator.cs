// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Buffers;
using System.Buffers.Binary;
using Sharc.Core.Format;
using Sharc.Core.Primitives;
using Sharc.Core.Records;

namespace Sharc.Core.BTree;

/// <summary>
/// Insert/delete operations on an index B-tree.
/// Index B-trees differ from table B-trees: cells are sorted by record content
/// (indexed column values + table rowid) rather than by a single rowid key.
/// </summary>
/// <remarks>
/// Cell formats:
///   Index leaf (0x0A):     [payloadSize:varint][payload]
///   Index interior (0x02): [leftChild:4BE][payloadSize:varint][payload]
/// Payload is a standard SQLite record: [header][col1_value][col2_value]...[table_rowid]
/// The last column is always the table rowid.
/// </remarks>
internal sealed class IndexBTreeMutator : IDisposable
{
    private readonly IWritablePageSource _source;
    private readonly int _usablePageSize;
    private readonly Dictionary<uint, byte[]> _pageCache = new(4);
    private readonly List<byte[]> _rentedBuffers = new(4);
    private bool _disposed;

    private readonly Func<uint>? _freePageAllocator;
    private readonly Func<uint> _newPageAllocator;

    private readonly BTreePageRewriter _rewriter;
    private readonly OverflowChainWriter _overflowWriter;

    /// <summary>
    /// Creates an index B-tree mutator.
    /// </summary>
    /// <param name="source">The writable page source.</param>
    /// <param name="usablePageSize">Usable page size.</param>
    /// <param name="freePageAllocator">Optional freelist allocator (returns 0 if empty).</param>
    /// <param name="newPageAllocator">
    /// Allocator for new pages beyond the freelist. This MUST be shared with the
    /// table BTreeMutator to prevent double-allocation of the same page numbers.
    /// </param>
    public IndexBTreeMutator(IWritablePageSource source, int usablePageSize,
        Func<uint>? freePageAllocator, Func<uint> newPageAllocator)
    {
        _source = source;
        _usablePageSize = usablePageSize;
        _freePageAllocator = freePageAllocator;
        _newPageAllocator = newPageAllocator;

        _rewriter = new BTreePageRewriter(source, usablePageSize, _rentedBuffers);
        _overflowWriter = new OverflowChainWriter(
            source, usablePageSize, _pageCache, _rentedBuffers,
            freePageAllocator, () => _newPageAllocator());
    }

    /// <summary>
    /// Inserts an index entry into the index B-tree rooted at <paramref name="rootPage"/>.
    /// The payload is a SQLite record containing [indexed columns..., table rowid].
    /// Returns the (possibly new) root page number.
    /// </summary>
    public uint Insert(uint rootPage, ReadOnlySpan<byte> indexRecordPayload)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Build index leaf cell: [payloadSize:varint][payload]
        int cellSize = CellBuilder.ComputeIndexLeafCellSize(indexRecordPayload.Length, _usablePageSize);
        byte[] cellArray = new byte[cellSize];
        CellBuilder.BuildIndexLeafCell(indexRecordPayload, cellArray, _usablePageSize);

        // Handle overflow if needed
        int inlineSize = IndexCellParser.CalculateIndexInlinePayloadSize(indexRecordPayload.Length, _usablePageSize);
        if (inlineSize < indexRecordPayload.Length)
        {
            _overflowWriter.WriteOverflowChain(cellArray.AsSpan(0, cellSize), indexRecordPayload, inlineSize);
        }

        ReadOnlySpan<byte> cellBytes = cellArray.AsSpan(0, cellSize);

        // Navigate from root to the correct leaf, comparing by record content
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
                int insertIdx = FindIndexLeafInsertionPoint(page, hdrOff, hdr, indexRecordPayload);
                path[pathCount++] = new InsertPathEntry(currentPage, insertIdx);
                break;
            }

            int childIdx = FindIndexInteriorChild(page, hdrOff, hdr, indexRecordPayload, out uint childPage);
            path[pathCount++] = new InsertPathEntry(currentPage, childIdx);
            currentPage = childPage;
        }

        return InsertCellAndSplit(path[..pathCount], pathCount - 1, cellBytes, indexRecordPayload, rootPage);
    }

    /// <summary>
    /// Deletes an index entry from the index B-tree by matching the exact record payload.
    /// Returns whether the entry was found and the (unchanged) root page.
    /// </summary>
    public (bool Found, uint RootPage) Delete(uint rootPage, ReadOnlySpan<byte> indexRecordPayload)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        uint currentPage = rootPage;
        while (true)
        {
            var page = ReadPageBuffer(currentPage);
            int hdrOff = currentPage == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
            var hdr = BTreePageHeader.Parse(page.AsSpan(hdrOff));

            if (hdr.IsLeaf)
            {
                var (cellIdx, found) = FindIndexLeafCellByKey(page, hdrOff, hdr, indexRecordPayload);
                if (!found)
                    return (false, rootPage);

                _rewriter.RemoveCellFromPage(page, hdrOff, hdr, cellIdx);
                WritePageBuffer(currentPage, page);
                return (true, rootPage);
            }

            FindIndexInteriorChild(page, hdrOff, hdr, indexRecordPayload, out uint childPage);
            currentPage = childPage;
        }
    }

    public void Reset()
    {
        ReturnRentedBuffers();
        _disposed = false;
    }

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

    // ── Record comparison ─────────────────────────────────────────

    /// <summary>
    /// Compares two index record payloads using SQLite collation rules.
    /// Returns negative if a &lt; b, zero if equal, positive if a &gt; b.
    /// Comparison is column-by-column using SQLite type affinity ordering:
    /// NULL &lt; INTEGER/REAL &lt; TEXT &lt; BLOB.
    /// </summary>
    private static int CompareIndexRecords(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        // Parse record headers to get serial types
        int aHeaderSize = VarintDecoder.Read(a, out long aHeaderLen);
        int bHeaderSize = VarintDecoder.Read(b, out long bHeaderLen);

        int aPos = aHeaderSize;
        int bPos = bHeaderSize;

        int aBodyStart = (int)aHeaderLen;
        int bBodyStart = (int)bHeaderLen;

        int aBodyPos = aBodyStart;
        int bBodyPos = bBodyStart;

        // Compare column by column
        while (aPos < aBodyStart && bPos < bBodyStart)
        {
            int aStBytes = VarintDecoder.Read(a[aPos..], out long aSt);
            int bStBytes = VarintDecoder.Read(b[bPos..], out long bSt);
            aPos += aStBytes;
            bPos += bStBytes;

            int aSize = SerialTypeCodec.GetContentSize(aSt);
            int bSize = SerialTypeCodec.GetContentSize(bSt);

            int cmp = CompareValues(aSt, a.Slice(aBodyPos, aSize), bSt, b.Slice(bBodyPos, bSize));
            if (cmp != 0) return cmp;

            aBodyPos += aSize;
            bBodyPos += bSize;
        }

        // If one has more columns, it's "greater"
        return (aBodyStart - aPos).CompareTo(bBodyStart - bPos);
    }

    /// <summary>
    /// Compares two values by their serial types following SQLite collation:
    /// NULL &lt; integers/reals &lt; text &lt; blob.
    /// </summary>
    private static int CompareValues(long stA, ReadOnlySpan<byte> bodyA, long stB, ReadOnlySpan<byte> bodyB)
    {
        var classA = SerialTypeCodec.GetStorageClass(stA);
        var classB = SerialTypeCodec.GetStorageClass(stB);

        // Different storage classes: NULL < INTEGER/REAL < TEXT < BLOB
        if (classA != classB)
            return StorageClassOrder(classA).CompareTo(StorageClassOrder(classB));

        return classA switch
        {
            ColumnStorageClass.Null => 0,
            ColumnStorageClass.Integral => DecodeInt(stA, bodyA).CompareTo(DecodeInt(stB, bodyB)),
            ColumnStorageClass.Real => DecodeReal(stA, bodyA).CompareTo(DecodeReal(stB, bodyB)),
            ColumnStorageClass.Text => bodyA.SequenceCompareTo(bodyB),
            ColumnStorageClass.Blob => bodyA.SequenceCompareTo(bodyB),
            _ => 0
        };
    }

    private static int StorageClassOrder(ColumnStorageClass sc) => sc switch
    {
        ColumnStorageClass.Null => 0,
        ColumnStorageClass.Integral => 1,
        ColumnStorageClass.Real => 1,
        ColumnStorageClass.Text => 2,
        ColumnStorageClass.Blob => 3,
        _ => 4
    };

    private static long DecodeInt(long serialType, ReadOnlySpan<byte> body) => serialType switch
    {
        0 => 0,
        8 => 0,
        9 => 1,
        1 => (sbyte)body[0],
        2 => BinaryPrimitives.ReadInt16BigEndian(body),
        3 => (body[0] << 16) | (body[1] << 8) | body[2],
        4 => BinaryPrimitives.ReadInt32BigEndian(body),
        5 => ((long)(sbyte)body[0] << 40) | ((long)body[1] << 32) | ((long)body[2] << 24)
             | ((long)body[3] << 16) | ((long)body[4] << 8) | body[5],
        6 => BinaryPrimitives.ReadInt64BigEndian(body),
        _ => 0
    };

    private static double DecodeReal(long serialType, ReadOnlySpan<byte> body) => serialType switch
    {
        7 => BinaryPrimitives.ReadDoubleBigEndian(body),
        _ => DecodeInt(serialType, body)  // integer compared as real
    };

    // ── Navigation helpers ─────────────────────────────────────────

    /// <summary>
    /// Binary-search a leaf index page for the insertion point of the given key.
    /// </summary>
    private int FindIndexLeafInsertionPoint(byte[] page, int hdrOff, BTreePageHeader hdr,
        ReadOnlySpan<byte> targetKey)
    {
        int lo = 0, hi = hdr.CellCount - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int cellPtr = hdr.GetCellPointer(page.AsSpan(hdrOff), mid);
            var cellPayload = ExtractIndexLeafPayload(page, cellPtr);

            int cmp = CompareIndexRecords(cellPayload, targetKey);
            if (cmp < 0) lo = mid + 1;
            else if (cmp > 0) hi = mid - 1;
            else return mid;
        }
        return lo;
    }

    /// <summary>
    /// Binary-search a leaf index page for an exact key match.
    /// </summary>
    private (int Index, bool Found) FindIndexLeafCellByKey(byte[] page, int hdrOff,
        BTreePageHeader hdr, ReadOnlySpan<byte> targetKey)
    {
        int lo = 0, hi = hdr.CellCount - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int cellPtr = hdr.GetCellPointer(page.AsSpan(hdrOff), mid);
            var cellPayload = ExtractIndexLeafPayload(page, cellPtr);

            int cmp = CompareIndexRecords(cellPayload, targetKey);
            if (cmp < 0) lo = mid + 1;
            else if (cmp > 0) hi = mid - 1;
            else return (mid, true);
        }
        return (lo, false);
    }

    /// <summary>
    /// Binary-search an interior index page to find the child to descend into.
    /// </summary>
    private int FindIndexInteriorChild(byte[] page, int hdrOff, BTreePageHeader hdr,
        ReadOnlySpan<byte> targetKey, out uint childPage)
    {
        int lo = 0, hi = hdr.CellCount - 1;
        int idx = -1;

        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int cellPtr = hdr.GetCellPointer(page.AsSpan(hdrOff), mid);
            var cellPayload = ExtractIndexInteriorPayload(page, cellPtr);

            int cmp = CompareIndexRecords(cellPayload, targetKey);
            if (cmp >= 0) { idx = mid; hi = mid - 1; }
            else lo = mid + 1;
        }

        if (idx != -1)
        {
            int cellPtr = hdr.GetCellPointer(page.AsSpan(hdrOff), idx);
            childPage = BinaryPrimitives.ReadUInt32BigEndian(page.AsSpan(cellPtr));
            return idx;
        }

        childPage = hdr.RightChildPage;
        return hdr.CellCount;
    }

    /// <summary>
    /// Extracts the record payload from an index leaf cell.
    /// </summary>
    private ReadOnlySpan<byte> ExtractIndexLeafPayload(byte[] page, int cellPtr)
    {
        int headerBytes = IndexCellParser.ParseIndexLeafCell(page.AsSpan(cellPtr), out int payloadSize);
        int inlineSize = IndexCellParser.CalculateIndexInlinePayloadSize(payloadSize, _usablePageSize);
        // For simplicity, only use inline portion for comparison (overflow keys are rare for typical indexes)
        return page.AsSpan(cellPtr + headerBytes, inlineSize);
    }

    /// <summary>
    /// Extracts the record payload from an index interior cell.
    /// </summary>
    private ReadOnlySpan<byte> ExtractIndexInteriorPayload(byte[] page, int cellPtr)
    {
        int headerBytes = IndexCellParser.ParseIndexInteriorCell(page.AsSpan(cellPtr), out _, out int payloadSize);
        int inlineSize = IndexCellParser.CalculateIndexInlinePayloadSize(payloadSize, _usablePageSize);
        return page.AsSpan(cellPtr + headerBytes, inlineSize);
    }

    // ── Insert + split engine ──────────────────────────────────────

    private uint InsertCellAndSplit(
        Span<InsertPathEntry> path,
        int pathIndex,
        ReadOnlySpan<byte> cellBytes,
        ReadOnlySpan<byte> recordPayload,
        uint currentRoot)
    {
        var (pageNum, insertIdx) = path[pathIndex];
        var pageBuf = ReadPageBuffer(pageNum);
        int hdrOff = pageNum == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
        var hdr = BTreePageHeader.Parse(pageBuf.AsSpan(hdrOff));

        if (_rewriter.TryInsertCell(pageBuf, hdrOff, hdr, insertIdx, cellBytes))
        {
            WritePageBuffer(pageNum, pageBuf);
            return currentRoot;
        }

        // ── Page is full — split ──────────────────────────────────
        var (cellBuf, refCount) = _rewriter.GatherIndexCellsWithInsertion(pageBuf, hdrOff, hdr, insertIdx, cellBytes);
        var cells = new ReadOnlySpan<BTreePageRewriter.CellRef>(_rewriter.CellRefBuffer, 0, refCount);

        // Split roughly in half by total byte count
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

        if (splitIdx == 0) splitIdx = 1;
        if (splitIdx >= refCount - 1) splitIdx = refCount - 2;

        bool isLeaf = hdr.IsLeaf;

        // Extract the median cell's record payload for promotion
        var medianRef = cells[splitIdx];
        var medianCellSpan = cellBuf.AsSpan(medianRef.Offset, medianRef.Length);
        ReadOnlySpan<byte> medianPayload;
        if (isLeaf)
        {
            int payHdr = IndexCellParser.ParseIndexLeafCell(medianCellSpan, out int paySize);
            medianPayload = medianCellSpan.Slice(payHdr, paySize);
        }
        else
        {
            int payHdr = IndexCellParser.ParseIndexInteriorCell(medianCellSpan, out _, out int paySize);
            medianPayload = medianCellSpan.Slice(payHdr, paySize);
        }

        // Copy median payload to heap since we need it after page rebuilding
        byte[] medianPayloadCopy = medianPayload.ToArray();

        uint newPageNum = AllocateNewPage();

        if (isLeaf)
        {
            _rewriter.BuildIndexLeafPage(pageBuf, hdrOff, cellBuf, cells[..(splitIdx + 1)]);
            WritePageBuffer(pageNum, pageBuf);

            var rightBuf = RentPageBuffer();
            _rewriter.BuildIndexLeafPage(rightBuf, 0, cellBuf, cells[(splitIdx + 1)..]);
            WritePageBuffer(newPageNum, rightBuf);
        }
        else
        {
            IndexCellParser.ParseIndexInteriorCell(medianCellSpan, out uint medianLeftChild, out _);

            _rewriter.BuildIndexInteriorPage(pageBuf, hdrOff, cellBuf, cells[..splitIdx], medianLeftChild);
            WritePageBuffer(pageNum, pageBuf);

            var rightBuf = RentPageBuffer();
            uint originalRightChild = hdr.RightChildPage;
            _rewriter.BuildIndexInteriorPage(rightBuf, 0, cellBuf, cells[(splitIdx + 1)..], originalRightChild);
            WritePageBuffer(newPageNum, rightBuf);
        }

        if (pathIndex == 0)
        {
            // Root split
            uint newLeftPage = AllocateNewPage();

            if (pageNum == 1)
            {
                var newLeftBuf = RentPageBuffer();
                if (isLeaf)
                    _rewriter.BuildIndexLeafPage(newLeftBuf, 0, cellBuf, cells[..(splitIdx + 1)]);
                else
                {
                    IndexCellParser.ParseIndexInteriorCell(medianCellSpan, out uint mlc, out _);
                    _rewriter.BuildIndexInteriorPage(newLeftBuf, 0, cellBuf, cells[..splitIdx], mlc);
                }
                WritePageBuffer(newLeftPage, newLeftBuf);
            }
            else
            {
                var leftContent = ReadPageBuffer(pageNum);
                WritePageBuffer(newLeftPage, leftContent);
            }

            var rootBuf = RentPageBuffer();

            if (pageNum == 1)
            {
                pageBuf.AsSpan(0, SQLiteLayout.DatabaseHeaderSize).CopyTo(rootBuf);
            }

            // Build an interior index cell for the new root
            int interiorCellSize = CellBuilder.ComputeIndexInteriorCellSize(medianPayloadCopy.Length, _usablePageSize);
            Span<byte> interiorCell = interiorCellSize <= 256 ? stackalloc byte[interiorCellSize] : new byte[interiorCellSize];
            int interiorSize = CellBuilder.BuildIndexInteriorCell(newLeftPage, medianPayloadCopy, interiorCell, _usablePageSize);

            var newRootHdr = new BTreePageHeader(
                BTreePageType.InteriorIndex,
                0, 1,
                (ushort)(_usablePageSize - interiorSize),
                0,
                newPageNum
            );

            int rootHdrOff = pageNum == 1 ? SQLiteLayout.DatabaseHeaderSize : 0;
            BTreePageHeader.Write(rootBuf.AsSpan(rootHdrOff), newRootHdr);

            int cellPtrOff = rootHdrOff + SQLiteLayout.IndexInteriorHeaderSize;
            ushort cellContentOff = (ushort)(_usablePageSize - interiorSize);
            BinaryPrimitives.WriteUInt16BigEndian(rootBuf.AsSpan(cellPtrOff), cellContentOff);

            interiorCell[..interiorSize].CopyTo(rootBuf.AsSpan(cellContentOff));

            WritePageBuffer(pageNum, rootBuf);

            return currentRoot;
        }
        else
        {
            // Non-root split: promote median to parent
            int interiorCellSize = CellBuilder.ComputeIndexInteriorCellSize(medianPayloadCopy.Length, _usablePageSize);
            Span<byte> interiorCell = interiorCellSize <= 256 ? stackalloc byte[interiorCellSize] : new byte[interiorCellSize];
            int interiorSize = CellBuilder.BuildIndexInteriorCell(pageNum, medianPayloadCopy, interiorCell, _usablePageSize);

            UpdateParentAfterSplit(path, pathIndex - 1, interiorCell[..interiorSize], newPageNum);

            return InsertCellAndSplit(path, pathIndex - 1, interiorCell[..interiorSize], medianPayloadCopy, currentRoot);
        }
    }

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

    private byte[] ReadPageBuffer(uint pageNumber)
    {
        if (_pageCache.TryGetValue(pageNumber, out var cached))
            return cached;

        var buf = RentPageBuffer();
        _source.ReadPage(pageNumber, buf.AsSpan(0, _source.PageSize));
        _pageCache[pageNumber] = buf;
        return buf;
    }

    private void WritePageBuffer(uint pageNumber, byte[] buffer)
    {
        _source.WritePage(pageNumber, buffer.AsSpan(0, _source.PageSize));
        _pageCache[pageNumber] = buffer;
    }

    private byte[] RentPageBuffer()
    {
        var buf = ArrayPool<byte>.Shared.Rent(_source.PageSize);
        buf.AsSpan(0, _source.PageSize).Clear();
        _rentedBuffers.Add(buf);
        return buf;
    }

    private uint AllocateNewPage()
    {
        uint page = _freePageAllocator?.Invoke() ?? 0;
        if (page == 0) page = _newPageAllocator();

        var buf = RentPageBuffer();
        var hdr = new BTreePageHeader(
            BTreePageType.LeafIndex, 0, 0,
            (ushort)_usablePageSize, 0, 0
        );
        BTreePageHeader.Write(buf, hdr);
        _pageCache[page] = buf;
        WritePageBuffer(page, buf);
        return page;
    }
}
