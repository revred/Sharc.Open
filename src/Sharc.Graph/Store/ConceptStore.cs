// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.


using Sharc.Core;
using Sharc.Core.BTree;
using Sharc.Core.IO;
using Sharc.Core.Records;
using Sharc.Graph.Model;
using Sharc.Graph.Schema;
using Sharc.Core.Schema;

namespace Sharc.Graph.Store;

/// <summary>
/// Reads concept (node) records from the graph's concept table B-tree.
/// </summary>
internal sealed class ConceptStore
{
    private readonly IBTreeReader _reader;
    public IBTreeReader Reader => _reader;
    private readonly ISchemaAdapter _schema;
    private readonly RecordDecoder _decoder = new();
    private int _tableRootPage;
    private int _columnCount;
    private string _tableName = "";

    // Index scan support
    private int _keyIndexRootPage = -1;

    // Column ordinals
    private int _colId = -1;
    private int _colKey = -1;
    private int _colType = -1;
    private int _colData = -1;
    private int _colCvn = -1;
    private int _colLvn = -1;
    private int _colSync = -1;
    private int _colUpdated = -1;
    private int _colTokens = -1;
    private int _colAlias = -1;

    // Persistent cursors for reuse
    private IIndexBTreeCursor? _reusableIndexCursor;
    private IBTreeCursor? _reusableTableCursor;
    private IBTreeCursor? _reusableScanCursor;

    // Fixed buffers to avoid stackalloc in loops
    private readonly long[] _serialsBuffer = new long[64];
    private readonly int[] _offsetsBuffer = new int[64];
    private readonly long[] _indexSerialsBuffer = new long[16];
    private readonly int[] _indexOffsetsBuffer = new int[16];

    public ConceptStore(IBTreeReader reader, ISchemaAdapter schema)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
    }

    public void Initialize(SharcSchema schemaInfo)
    {
        _tableName = _schema.NodeTableName;
        var table = schemaInfo.GetTable(_tableName);
        _tableRootPage = table.RootPage;
        _columnCount = table.Columns.Count;

        _colId = table.GetColumnOrdinal(_schema.NodeIdColumn);
        _colKey = table.GetColumnOrdinal(_schema.NodeKeyColumn);
        _colType = table.GetColumnOrdinal(_schema.NodeTypeColumn);
        _colData = table.GetColumnOrdinal(_schema.NodeDataColumn);

        _colCvn = _schema.NodeCvnColumn != null ? table.GetColumnOrdinal(_schema.NodeCvnColumn) : -1;
        _colLvn = _schema.NodeLvnColumn != null ? table.GetColumnOrdinal(_schema.NodeLvnColumn) : -1;
        _colSync = _schema.NodeSyncColumn != null ? table.GetColumnOrdinal(_schema.NodeSyncColumn) : -1;
        _colUpdated = _schema.NodeUpdatedColumn != null ? table.GetColumnOrdinal(_schema.NodeUpdatedColumn) : -1;
        _colTokens = _schema.NodeTokensColumn != null ? table.GetColumnOrdinal(_schema.NodeTokensColumn) : -1;
        _colAlias = _schema.NodeAliasColumn != null ? table.GetColumnOrdinal(_schema.NodeAliasColumn) : -1;

        // Key Index (Integer based)
        int keyIndexRoot = -1;
        foreach (var idx in schemaInfo.Indexes)
        {
            if (idx.TableName.Equals(_tableName, StringComparison.OrdinalIgnoreCase) &&
                idx.Columns.Count > 0 &&
                idx.Columns[0].Name.Equals(_schema.NodeKeyColumn, StringComparison.OrdinalIgnoreCase))
            {
                keyIndexRoot = idx.RootPage;
                break;
            }
        }
        _keyIndexRootPage = keyIndexRoot;
    }

    public GraphRecord? Get(NodeKey key, bool includeData = true)
    {
        if (_tableRootPage == 0) throw new InvalidOperationException("Store not initialized.");

        if (_keyIndexRootPage > 0)
            return GetViaIndex(_keyIndexRootPage, key, includeData);

        return GetViaTableScan(key, includeData);
    }

    private GraphRecord? GetViaIndex(int indexRootPage, NodeKey key, bool includeData)
    {
        _reusableIndexCursor ??= _reader.CreateIndexCursor((uint)indexRootPage);
        _reusableTableCursor ??= _reader.CreateCursor((uint)_tableRootPage);

        var indexCursor = _reusableIndexCursor;
        var tableCursor = _reusableTableCursor;

        // Devirtualization for WASM
        var cachedTableCursor = tableCursor as BTreeCursor<CachedPageSource>;
        var memoryTableCursor = tableCursor as BTreeCursor<MemoryPageSource>;

        indexCursor.Reset();
        if (!indexCursor.SeekFirst(key.Value))
            return null;

        do
        {
            var indexPayload = indexCursor.Payload;
            int indexColCount = _decoder.ReadSerialTypes(indexPayload, _indexSerialsBuffer, out int indexBodyOffset);
            
            // Only need column [0] (key) and [last] (rowid)
            int keyOffset = indexBodyOffset;
            long keySt = _indexSerialsBuffer[0];
            
            long indexValue = _decoder.DecodeInt64At(indexPayload, keySt, keyOffset);
            if (indexValue > key.Value) return null;
            if (indexValue != key.Value) continue;

            // Compute rowid offset (last column)
            int rowIdOrdinal = indexColCount - 1;
            RecordDecoder.ComputeColumnOffsetsIncremental(_indexSerialsBuffer, 0, indexColCount, indexBodyOffset, _indexOffsetsBuffer);
            long rowId = _decoder.DecodeInt64At(indexPayload, _indexSerialsBuffer[rowIdOrdinal], _indexOffsetsBuffer[rowIdOrdinal]);

            if (tableCursor.Seek(rowId))
            {
                ReadOnlySpan<byte> payload;
                if (cachedTableCursor != null) payload = cachedTableCursor.Payload;
                else if (memoryTableCursor != null) payload = memoryTableCursor.Payload;
                else payload = tableCursor.Payload;
                
                return MapToRecordSelective(payload, includeData);
            }
        }
        while (indexCursor.MoveNext());

        return null;
    }

    public GraphRecord? Get(string id)
    {
        if (_tableRootPage == 0) throw new InvalidOperationException("Store not initialized.");
        return GetViaIdTableScan(id);
    }

    private GraphRecord? GetViaIdTableScan(string id)
    {
        _reusableScanCursor ??= _reader.CreateCursor((uint)_tableRootPage);
        var cursor = _reusableScanCursor;
        cursor.Reset();

        while (cursor.MoveNext())
        {
            var payload = cursor.Payload;
            _decoder.ReadSerialTypes(payload, _serialsBuffer, out int bodyOffset);
            _decoder.ComputeColumnOffsets(_serialsBuffer, _columnCount, bodyOffset, _offsetsBuffer);
            string rowIdStr = _colId >= 0 && _colId < _columnCount ? _decoder.DecodeStringAt(payload, _serialsBuffer[_colId], _offsetsBuffer[_colId]) : "";
            if (!rowIdStr.Equals(id, StringComparison.OrdinalIgnoreCase)) continue;
            return MapToRecordSelectiveWithOffsets(payload);
        }
        return null;
    }

    private GraphRecord? GetViaTableScan(NodeKey key, bool includeData)
    {
        _reusableScanCursor ??= _reader.CreateCursor((uint)_tableRootPage);
        var cursor = _reusableScanCursor;
        cursor.Reset();

        // Devirtualization
        var cachedCursor = cursor as BTreeCursor<CachedPageSource>;
        var memoryCursor = cursor as BTreeCursor<MemoryPageSource>;

        while (cursor.MoveNext())
        {
            ReadOnlySpan<byte> payload;
            if (cachedCursor != null) payload = cachedCursor.Payload;
            else if (memoryCursor != null) payload = memoryCursor.Payload;
            else payload = cursor.Payload;

            _decoder.ReadSerialTypes(payload, _serialsBuffer, out int bodyOffset);
            
            // Lazy offset for _colKey
            int keyOffset = RecordDecoder.ComputeColumnOffsetsIncremental(_serialsBuffer, 0, _colKey + 1, bodyOffset, _offsetsBuffer);
            long barId = _colKey >= 0 && _colKey < _columnCount ? _decoder.DecodeInt64At(payload, _serialsBuffer[_colKey], _offsetsBuffer[_colKey]) : 0;
            if (barId != key.Value) continue;
            
            // Now compute remaining offsets if needed for MapToRecordSelectiveWithOffsets
            if (includeData)
            {
                RecordDecoder.ComputeColumnOffsetsIncremental(_serialsBuffer, _colKey + 1, _columnCount, keyOffset, _offsetsBuffer);
            }
            return MapToRecordSelectiveWithOffsets(payload, includeData);
        }
        return null;
    }

    private GraphRecord MapToRecordSelective(ReadOnlySpan<byte> payload, bool includeData)
    {
        _decoder.ReadSerialTypes(payload, _serialsBuffer, out int bodyOffset);
        _decoder.ComputeColumnOffsets(_serialsBuffer, _columnCount, bodyOffset, _offsetsBuffer);
        return MapToRecordSelectiveWithOffsets(payload, includeData);
    }

    /// <summary>
    /// Builds a GraphRecord using pre-computed offsets in _serialsBuffer/_offsetsBuffer.
    /// Callers must have already called ReadSerialTypes + ComputeColumnOffsets.
    /// </summary>
    private GraphRecord MapToRecordSelectiveWithOffsets(ReadOnlySpan<byte> payload, bool includeData = true)
    {
        long keyVal = _colKey >= 0 && _colKey < _columnCount ? _decoder.DecodeInt64At(payload, _serialsBuffer[_colKey], _offsetsBuffer[_colKey]) : 0;
        NodeKey key = new NodeKey(keyVal);

        int typeId = _colType >= 0 && _colType < _columnCount ? (int)_decoder.DecodeInt64At(payload, _serialsBuffer[_colType], _offsetsBuffer[_colType]) : 0;

        string? id = null;
        string? data = null;

        if (includeData)
        {
            id = _colId >= 0 && _colId < _columnCount ? _decoder.DecodeStringAt(payload, _serialsBuffer[_colId], _offsetsBuffer[_colId]) : null;
            data = _colData >= 0 && _colData < _columnCount ? _decoder.DecodeStringAt(payload, _serialsBuffer[_colData], _offsetsBuffer[_colData]) : "{}";
        }

        string typeStr = _schema.TypeNames.TryGetValue(typeId, out var name)
            ? name
            : typeId.ToString(System.Globalization.CultureInfo.InvariantCulture);

        long updatedUnix = _colUpdated >= 0 && _colUpdated < _columnCount ? _decoder.DecodeInt64At(payload, _serialsBuffer[_colUpdated], _offsetsBuffer[_colUpdated]) : 0;
        var updatedAt = updatedUnix > 0 ? DateTimeOffset.FromUnixTimeSeconds(updatedUnix) : (DateTimeOffset?)null;

        var result = new GraphRecord(new RecordId(typeStr, id, key), key, typeId, data, updatedAt, updatedAt)
        {
            CVN = _colCvn >= 0 && _colCvn < _columnCount ? (int)_decoder.DecodeInt64At(payload, _serialsBuffer[_colCvn], _offsetsBuffer[_colCvn]) : 0,
            LVN = _colLvn >= 0 && _colLvn < _columnCount ? (int)_decoder.DecodeInt64At(payload, _serialsBuffer[_colLvn], _offsetsBuffer[_colLvn]) : 0,
            SyncStatus = _colSync >= 0 && _colSync < _columnCount ? (int)_decoder.DecodeInt64At(payload, _serialsBuffer[_colSync], _offsetsBuffer[_colSync]) : 0,
            Tokens = _colTokens >= 0 && _colTokens < _columnCount ? (int)_decoder.DecodeInt64At(payload, _serialsBuffer[_colTokens], _offsetsBuffer[_colTokens]) : 0,
            Alias = (_colAlias >= 0 && _colAlias < _columnCount && _serialsBuffer[_colAlias] != 0) ? _decoder.DecodeStringAt(payload, _serialsBuffer[_colAlias], _offsetsBuffer[_colAlias]) : null
        };

        return result;
    }

    /// <summary>
    /// Full table scan returning only node keys — no data, no JSON, no allocation beyond the key list.
    /// </summary>
    internal List<NodeKey> FetchAllKeys()
    {
        if (_tableRootPage == 0) throw new InvalidOperationException("Store not initialized.");

        _reusableScanCursor ??= _reader.CreateCursor((uint)_tableRootPage);
        var cursor = _reusableScanCursor;
        cursor.Reset();

        // Devirtualization
        var cachedCursor = cursor as BTreeCursor<CachedPageSource>;
        var memoryCursor = cursor as BTreeCursor<MemoryPageSource>;

        var keys = new List<NodeKey>();
        while (cursor.MoveNext())
        {
            ReadOnlySpan<byte> payload;
            if (cachedCursor != null) payload = cachedCursor.Payload;
            else if (memoryCursor != null) payload = memoryCursor.Payload;
            else payload = cursor.Payload;

            _decoder.ReadSerialTypes(payload, _serialsBuffer, out int bodyOffset);
            
            // Optimization: Only compute offsets up to _colKey
            RecordDecoder.ComputeColumnOffsetsIncremental(_serialsBuffer, 0, _colKey + 1, bodyOffset, _offsetsBuffer);
            
            long keyVal = _colKey >= 0 && _colKey < _columnCount
                ? _decoder.DecodeInt64At(payload, _serialsBuffer[_colKey], _offsetsBuffer[_colKey])
                : 0;
            if (keyVal != 0)
                keys.Add(new NodeKey(keyVal));
        }

        return keys;
    }

    public void Dispose()
    {
        _reusableIndexCursor?.Dispose();
        _reusableTableCursor?.Dispose();
        _reusableScanCursor?.Dispose();
    }
}