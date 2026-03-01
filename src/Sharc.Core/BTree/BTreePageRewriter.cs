// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Sharc.Core.Format;

namespace Sharc.Core.BTree;

/// <summary>
/// Page-level operations: cell insertion, removal, defragmentation, and page building.
/// Extracted from BTreeMutator for composability and testability.
/// </summary>
internal sealed class BTreePageRewriter
{
    private readonly IWritablePageSource _source;
    private readonly int _usablePageSize;
    private readonly List<byte[]> _rentedBuffers;

    /// <summary>Header size for a leaf table page.</summary>
    private const int LeafHeaderSize = SQLiteLayout.TableLeafHeaderSize;   // 8
    /// <summary>Header size for an interior table page.</summary>
    private const int InteriorHeaderSize = SQLiteLayout.TableInteriorHeaderSize; // 12

    /// <summary>Describes a cell's location within a contiguous assembly buffer.</summary>
    internal readonly struct CellRef
    {
        public readonly int Offset;
        public readonly int Length;
        public CellRef(int offset, int length) { Offset = offset; Length = length; }
    }

    private CellRef[]? _cellRefBuffer; // lazy — only allocated when a split or defrag is needed

    public BTreePageRewriter(
        IWritablePageSource source,
        int usablePageSize,
        List<byte[]> rentedBuffers)
    {
        _source = source;
        _usablePageSize = usablePageSize;
        _rentedBuffers = rentedBuffers;
    }

    [MemberNotNull(nameof(_cellRefBuffer))]
    internal void EnsureCellRefCapacity(int needed)
    {
        if (_cellRefBuffer == null || _cellRefBuffer.Length < needed)
            _cellRefBuffer = new CellRef[Math.Max(needed, _cellRefBuffer?.Length * 2 ?? 256)];
    }

    /// <summary>
    /// Tries to insert a cell into a page. Returns false if there isn't enough free space.
    /// </summary>
    public bool TryInsertCell(byte[] pageBuf, int hdrOff, BTreePageHeader hdr,
        int insertIdx, ReadOnlySpan<byte> cellBytes)
    {
        int headerSize = hdr.HeaderSize;
        int cellPtrArrayEnd = hdrOff + headerSize + (hdr.CellCount + 1) * 2;
        int cellContentStart = hdr.CellContentOffset == 0 ? _usablePageSize : hdr.CellContentOffset;

        int requiredSpace = cellBytes.Length + 2;
        int availableSpace = cellContentStart - cellPtrArrayEnd;

        if (availableSpace < requiredSpace)
        {
            int totalFree = availableSpace + hdr.FragmentedFreeBytes;
            if (totalFree >= requiredSpace)
            {
                DefragmentPage(pageBuf, hdrOff, hdr);
                hdr = BTreePageHeader.Parse(pageBuf.AsSpan(hdrOff));
                cellContentStart = hdr.CellContentOffset == 0 ? _usablePageSize : hdr.CellContentOffset;
                cellPtrArrayEnd = hdrOff + headerSize + (hdr.CellCount + 1) * 2;
                availableSpace = cellContentStart - cellPtrArrayEnd;
            }

            if (availableSpace < requiredSpace)
                return false;
        }

        int newCellOffset = cellContentStart - cellBytes.Length;
        cellBytes.CopyTo(pageBuf.AsSpan(newCellOffset));

        var pageSpan = pageBuf.AsSpan();
        int ptrBase = hdrOff + headerSize;
        for (int i = hdr.CellCount - 1; i >= insertIdx; i--)
        {
            ushort ptr = BinaryPrimitives.ReadUInt16BigEndian(pageSpan[(ptrBase + i * 2)..]);
            BinaryPrimitives.WriteUInt16BigEndian(pageSpan[(ptrBase + (i + 1) * 2)..], ptr);
        }

        BinaryPrimitives.WriteUInt16BigEndian(pageSpan[(ptrBase + insertIdx * 2)..], (ushort)newCellOffset);

        var newHdr = new BTreePageHeader(
            hdr.PageType,
            hdr.FirstFreeblockOffset,
            (ushort)(hdr.CellCount + 1),
            (ushort)newCellOffset,
            hdr.FragmentedFreeBytes,
            hdr.RightChildPage
        );
        BTreePageHeader.Write(pageSpan[hdrOff..], newHdr);

        return true;
    }

    /// <summary>
    /// Collects all cells from a page plus the new cell at the insertion point.
    /// Cell descriptors are written to the internal cell ref buffer.
    /// </summary>
    public (byte[] cellBuf, int refCount) GatherCellsWithInsertion(byte[] pageBuf, int hdrOff,
        BTreePageHeader hdr, int insertIdx, ReadOnlySpan<byte> newCell)
    {
        var pageSpan = pageBuf.AsSpan();

        // Calculate total bytes needed
        int totalBytes = newCell.Length;
        for (int i = 0; i < hdr.CellCount; i++)
        {
            int cellPtr = hdr.GetCellPointer(pageSpan[hdrOff..], i);
            totalBytes += MeasureCell(pageSpan[cellPtr..], hdr.IsLeaf);
        }

        var buffer = ArrayPool<byte>.Shared.Rent(totalBytes);
        _rentedBuffers.Add(buffer);

        EnsureCellRefCapacity(hdr.CellCount + 1);

        int refCount = 0;
        int writeOff = 0;

        for (int i = 0; i < insertIdx; i++)
        {
            int cellPtr = hdr.GetCellPointer(pageSpan[hdrOff..], i);
            int cellLen = MeasureCell(pageSpan[cellPtr..], hdr.IsLeaf);
            pageSpan.Slice(cellPtr, cellLen).CopyTo(buffer.AsSpan(writeOff));
            _cellRefBuffer[refCount++] = new CellRef(writeOff, cellLen);
            writeOff += cellLen;
        }

        newCell.CopyTo(buffer.AsSpan(writeOff));
        _cellRefBuffer[refCount++] = new CellRef(writeOff, newCell.Length);
        writeOff += newCell.Length;

        for (int i = insertIdx; i < hdr.CellCount; i++)
        {
            int cellPtr = hdr.GetCellPointer(pageSpan[hdrOff..], i);
            int cellLen = MeasureCell(pageSpan[cellPtr..], hdr.IsLeaf);
            pageSpan.Slice(cellPtr, cellLen).CopyTo(buffer.AsSpan(writeOff));
            _cellRefBuffer[refCount++] = new CellRef(writeOff, cellLen);
            writeOff += cellLen;
        }

        return (buffer, refCount);
    }

    /// <summary>
    /// Gets the internal cell ref buffer for reading cell descriptors after <see cref="GatherCellsWithInsertion"/>.
    /// </summary>
    internal CellRef[]? CellRefBuffer => _cellRefBuffer;

    /// <summary>
    /// Measures the byte length of a cell starting at the given position.
    /// Dispatches to table or index cell format based on page type.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int MeasureCell(ReadOnlySpan<byte> cellData, bool isLeaf)
        => MeasureCell(cellData, isLeaf, isIndex: false);

    /// <summary>
    /// Measures the byte length of a cell starting at the given position.
    /// Dispatches to table or index cell format based on <paramref name="isIndex"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int MeasureCell(ReadOnlySpan<byte> cellData, bool isLeaf, bool isIndex)
    {
        if (isIndex)
        {
            if (isLeaf)
            {
                int off = IndexCellParser.ParseIndexLeafCell(cellData, out int payloadSize);
                int inlineSize = IndexCellParser.CalculateIndexInlinePayloadSize(payloadSize, _usablePageSize);
                int total = off + inlineSize;
                if (inlineSize < payloadSize) total += 4;
                return total;
            }
            else
            {
                int off = IndexCellParser.ParseIndexInteriorCell(cellData, out _, out int payloadSize);
                int inlineSize = IndexCellParser.CalculateIndexInlinePayloadSize(payloadSize, _usablePageSize);
                int total = off + inlineSize;
                if (inlineSize < payloadSize) total += 4;
                return total;
            }
        }

        if (isLeaf)
        {
            int off = CellParser.ParseTableLeafCell(cellData, out int payloadSize, out _);
            int inlineSize = CellParser.CalculateInlinePayloadSize(payloadSize, _usablePageSize);
            int total = off + inlineSize;
            if (inlineSize < payloadSize) total += 4; // overflow pointer
            return total;
        }
        else
        {
            return CellParser.ParseTableInteriorCell(cellData, out _, out _);
        }
    }

    /// <summary>
    /// Collects all index cells from a page plus the new cell at the insertion point.
    /// Uses index-specific cell measurement.
    /// </summary>
    public (byte[] cellBuf, int refCount) GatherIndexCellsWithInsertion(byte[] pageBuf, int hdrOff,
        BTreePageHeader hdr, int insertIdx, ReadOnlySpan<byte> newCell)
    {
        var pageSpan = pageBuf.AsSpan();
        bool isLeaf = hdr.IsLeaf;

        int totalBytes = newCell.Length;
        for (int i = 0; i < hdr.CellCount; i++)
        {
            int cellPtr = hdr.GetCellPointer(pageSpan[hdrOff..], i);
            totalBytes += MeasureCell(pageSpan[cellPtr..], isLeaf, isIndex: true);
        }

        var buffer = ArrayPool<byte>.Shared.Rent(totalBytes);
        _rentedBuffers.Add(buffer);

        EnsureCellRefCapacity(hdr.CellCount + 1);

        int refCount = 0;
        int writeOff = 0;

        for (int i = 0; i < insertIdx; i++)
        {
            int cellPtr = hdr.GetCellPointer(pageSpan[hdrOff..], i);
            int cellLen = MeasureCell(pageSpan[cellPtr..], isLeaf, isIndex: true);
            pageSpan.Slice(cellPtr, cellLen).CopyTo(buffer.AsSpan(writeOff));
            _cellRefBuffer[refCount++] = new CellRef(writeOff, cellLen);
            writeOff += cellLen;
        }

        newCell.CopyTo(buffer.AsSpan(writeOff));
        _cellRefBuffer[refCount++] = new CellRef(writeOff, newCell.Length);
        writeOff += newCell.Length;

        for (int i = insertIdx; i < hdr.CellCount; i++)
        {
            int cellPtr = hdr.GetCellPointer(pageSpan[hdrOff..], i);
            int cellLen = MeasureCell(pageSpan[cellPtr..], isLeaf, isIndex: true);
            pageSpan.Slice(cellPtr, cellLen).CopyTo(buffer.AsSpan(writeOff));
            _cellRefBuffer[refCount++] = new CellRef(writeOff, cellLen);
            writeOff += cellLen;
        }

        return (buffer, refCount);
    }

    /// <summary>Builds a leaf table page from a span of cell descriptors in a contiguous buffer.</summary>
    public void BuildLeafPage(byte[] pageBuf, int hdrOff, byte[] cellBuf, ReadOnlySpan<CellRef> cells)
    {
        var span = pageBuf.AsSpan(0, _source.PageSize);
        span[hdrOff..].Clear();

        int contentEnd = _usablePageSize;
        int ptrBase = hdrOff + LeafHeaderSize;

        for (int i = 0; i < cells.Length; i++)
        {
            var cell = cells[i];
            contentEnd -= cell.Length;
            cellBuf.AsSpan(cell.Offset, cell.Length).CopyTo(span[contentEnd..]);
            BinaryPrimitives.WriteUInt16BigEndian(span[(ptrBase + i * 2)..], (ushort)contentEnd);
        }

        var hdr = new BTreePageHeader(
            BTreePageType.LeafTable, 0,
            (ushort)cells.Length,
            (ushort)contentEnd,
            0, 0
        );
        BTreePageHeader.Write(span[hdrOff..], hdr);
    }

    /// <summary>Builds an interior table page from a span of cell descriptors in a contiguous buffer.</summary>
    public void BuildInteriorPage(byte[] pageBuf, int hdrOff, byte[] cellBuf,
        ReadOnlySpan<CellRef> cells, uint rightChildPage)
    {
        var span = pageBuf.AsSpan(0, _source.PageSize);
        span[hdrOff..].Clear();

        int contentEnd = _usablePageSize;
        int ptrBase = hdrOff + InteriorHeaderSize;

        for (int i = 0; i < cells.Length; i++)
        {
            var cell = cells[i];
            contentEnd -= cell.Length;
            cellBuf.AsSpan(cell.Offset, cell.Length).CopyTo(span[contentEnd..]);
            BinaryPrimitives.WriteUInt16BigEndian(span[(ptrBase + i * 2)..], (ushort)contentEnd);
        }

        var hdr = new BTreePageHeader(
            BTreePageType.InteriorTable, 0,
            (ushort)cells.Length,
            (ushort)contentEnd,
            0,
            rightChildPage
        );
        BTreePageHeader.Write(span[hdrOff..], hdr);
    }

    /// <summary>Builds an index leaf page (page type 0x0A) from a span of cell descriptors.</summary>
    public void BuildIndexLeafPage(byte[] pageBuf, int hdrOff, byte[] cellBuf, ReadOnlySpan<CellRef> cells)
    {
        var span = pageBuf.AsSpan(0, _source.PageSize);
        span[hdrOff..].Clear();

        int contentEnd = _usablePageSize;
        int ptrBase = hdrOff + LeafHeaderSize;

        for (int i = 0; i < cells.Length; i++)
        {
            var cell = cells[i];
            contentEnd -= cell.Length;
            cellBuf.AsSpan(cell.Offset, cell.Length).CopyTo(span[contentEnd..]);
            BinaryPrimitives.WriteUInt16BigEndian(span[(ptrBase + i * 2)..], (ushort)contentEnd);
        }

        var hdr = new BTreePageHeader(
            BTreePageType.LeafIndex, 0,
            (ushort)cells.Length,
            (ushort)contentEnd,
            0, 0
        );
        BTreePageHeader.Write(span[hdrOff..], hdr);
    }

    /// <summary>Builds an index interior page (page type 0x02) from a span of cell descriptors.</summary>
    public void BuildIndexInteriorPage(byte[] pageBuf, int hdrOff, byte[] cellBuf,
        ReadOnlySpan<CellRef> cells, uint rightChildPage)
    {
        var span = pageBuf.AsSpan(0, _source.PageSize);
        span[hdrOff..].Clear();

        int contentEnd = _usablePageSize;
        int ptrBase = hdrOff + InteriorHeaderSize;

        for (int i = 0; i < cells.Length; i++)
        {
            var cell = cells[i];
            contentEnd -= cell.Length;
            cellBuf.AsSpan(cell.Offset, cell.Length).CopyTo(span[contentEnd..]);
            BinaryPrimitives.WriteUInt16BigEndian(span[(ptrBase + i * 2)..], (ushort)contentEnd);
        }

        var hdr = new BTreePageHeader(
            BTreePageType.InteriorIndex, 0,
            (ushort)cells.Length,
            (ushort)contentEnd,
            0,
            rightChildPage
        );
        BTreePageHeader.Write(span[hdrOff..], hdr);
    }

    /// <summary>
    /// Removes the cell at <paramref name="cellIndex"/> from a leaf page.
    /// Shifts the cell pointer array left and recomputes FragmentedFreeBytes accurately.
    /// </summary>
    public void RemoveCellFromPage(byte[] pageBuf, int hdrOff, BTreePageHeader hdr, int cellIndex)
    {
        var pageSpan = pageBuf.AsSpan();
        int headerSize = hdr.HeaderSize;
        int ptrBase = hdrOff + headerSize;
        bool isIndex = !hdr.IsTable;

        for (int i = cellIndex; i < hdr.CellCount - 1; i++)
        {
            ushort ptr = BinaryPrimitives.ReadUInt16BigEndian(pageSpan[(ptrBase + (i + 1) * 2)..]);
            BinaryPrimitives.WriteUInt16BigEndian(pageSpan[(ptrBase + i * 2)..], ptr);
        }

        BinaryPrimitives.WriteUInt16BigEndian(pageSpan[(ptrBase + (hdr.CellCount - 1) * 2)..], 0);

        int newCellCount = hdr.CellCount - 1;

        if (newCellCount == 0)
        {
            var emptyHdr = new BTreePageHeader(
                hdr.PageType, 0, 0, (ushort)_usablePageSize, 0, hdr.RightChildPage);
            BTreePageHeader.Write(pageSpan[hdrOff..], emptyHdr);
            return;
        }

        ushort newCellContentOffset = ushort.MaxValue;
        int totalCellBytes = 0;
        for (int i = 0; i < newCellCount; i++)
        {
            ushort ptr = BinaryPrimitives.ReadUInt16BigEndian(pageSpan[(ptrBase + i * 2)..]);
            if (ptr < newCellContentOffset) newCellContentOffset = ptr;
            totalCellBytes += MeasureCell(pageSpan[ptr..], hdr.IsLeaf, isIndex);
        }

        int cellContentAreaSize = _usablePageSize - newCellContentOffset;
        int newFragmented = cellContentAreaSize - totalCellBytes;

        if (newFragmented > 255)
        {
            int totalCellBytesForDefrag = 0;
            for (int i = 0; i < newCellCount; i++)
            {
                int ptr = BinaryPrimitives.ReadUInt16BigEndian(pageSpan[(ptrBase + i * 2)..]);
                totalCellBytesForDefrag += MeasureCell(pageSpan[ptr..], hdr.IsLeaf, isIndex);
            }

            var defragBuf = ArrayPool<byte>.Shared.Rent(totalCellBytesForDefrag);
            _rentedBuffers.Add(defragBuf);
            EnsureCellRefCapacity(newCellCount);
            int defragRefCount = 0;
            int writeOff = 0;

            for (int i = 0; i < newCellCount; i++)
            {
                int ptr = BinaryPrimitives.ReadUInt16BigEndian(pageSpan[(ptrBase + i * 2)..]);
                int len = MeasureCell(pageSpan[ptr..], hdr.IsLeaf, isIndex);
                pageSpan.Slice(ptr, len).CopyTo(defragBuf.AsSpan(writeOff));
                _cellRefBuffer[defragRefCount++] = new CellRef(writeOff, len);
                writeOff += len;
            }
            var defragCells = new ReadOnlySpan<CellRef>(_cellRefBuffer, 0, defragRefCount);
            if (isIndex)
                BuildIndexLeafPage(pageBuf, hdrOff, defragBuf, defragCells);
            else
                BuildLeafPage(pageBuf, hdrOff, defragBuf, defragCells);
            return;
        }

        var newHdr = new BTreePageHeader(
            hdr.PageType,
            hdr.FirstFreeblockOffset,
            (ushort)newCellCount,
            newCellContentOffset,
            (byte)newFragmented,
            hdr.RightChildPage
        );
        BTreePageHeader.Write(pageSpan[hdrOff..], newHdr);
    }

    /// <summary>
    /// Compacts all cells on a page to recover fragmented free space.
    /// </summary>
    public void DefragmentPage(byte[] pageBuf, int hdrOff, BTreePageHeader hdr)
    {
        var pageSpan = pageBuf.AsSpan();
        int ptrBase = hdrOff + hdr.HeaderSize;
        bool isIndex = !hdr.IsTable;

        // Measure total cell bytes and collect descriptors into _cellRefBuffer
        int totalCellBytes = 0;
        for (int i = 0; i < hdr.CellCount; i++)
        {
            int ptr = hdr.GetCellPointer(pageSpan[hdrOff..], i);
            totalCellBytes += MeasureCell(pageSpan[ptr..], hdr.IsLeaf, isIndex);
        }

        var defragBuf = ArrayPool<byte>.Shared.Rent(totalCellBytes);
        _rentedBuffers.Add(defragBuf);
        EnsureCellRefCapacity(hdr.CellCount);
        int writeOff = 0;

        for (int i = 0; i < hdr.CellCount; i++)
        {
            int ptr = hdr.GetCellPointer(pageSpan[hdrOff..], i);
            int len = MeasureCell(pageSpan[ptr..], hdr.IsLeaf, isIndex);
            pageSpan.Slice(ptr, len).CopyTo(defragBuf.AsSpan(writeOff));
            _cellRefBuffer[i] = new CellRef(writeOff, len);
            writeOff += len;
        }

        var cells = new ReadOnlySpan<CellRef>(_cellRefBuffer, 0, hdr.CellCount);
        int contentEnd = _usablePageSize;
        for (int i = 0; i < cells.Length; i++)
        {
            var cell = cells[i];
            contentEnd -= cell.Length;
            defragBuf.AsSpan(cell.Offset, cell.Length).CopyTo(pageSpan[contentEnd..]);
            BinaryPrimitives.WriteUInt16BigEndian(pageSpan[(ptrBase + i * 2)..], (ushort)contentEnd);
        }

        var newHdr = new BTreePageHeader(
            hdr.PageType,
            0,
            hdr.CellCount,
            (ushort)contentEnd,
            0,
            hdr.RightChildPage
        );
        BTreePageHeader.Write(pageSpan[hdrOff..], newHdr);
    }
}
