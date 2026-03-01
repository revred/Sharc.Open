// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Text;
using System.Buffers.Binary;
using Sharc.Core;
using Sharc.Core.BTree;
using Sharc.Core.Format;
using Sharc.Core.IO;
using Sharc.Core.Records;
using Sharc.Core.Schema;
using Sharc.Crypto;
using Sharc.Exceptions;

namespace Sharc;

/// <summary>
/// Internal factory for SharcDatabase instances.
/// </summary>
internal static class SharcDatabaseFactory
{
    public static SharcDatabase Create(string path)
    {
        if (File.Exists(path))
            throw new InvalidOperationException($"File already exists: {path}");

        var data = BuildNewDatabaseBytes();
        File.WriteAllBytes(path, data);
        return Open(path, new SharcOpenOptions { Writable = true });
    }

    /// <summary>
    /// Creates a new, empty Sharc database entirely in memory (no filesystem access).
    /// Suitable for Blazor WASM and other environments without file I/O.
    /// </summary>
    public static SharcDatabase CreateInMemory()
    {
        var data = BuildNewDatabaseBytes();
        return OpenMemory(data, new SharcOpenOptions { Writable = true });
    }

    private static byte[] BuildNewDatabaseBytes()
    {
        int pageSize = 4096;
        var data = new byte[pageSize * 5];

        // 1. Database Header
        var dbHeader = new DatabaseHeader(pageSize, 1, 1, 0, 1, 5, 0, 0, 1, 4, 1, 0, 0, 3042000);
        DatabaseHeader.Write(data, dbHeader);

        // System table schema
        var ledgerCols = new[] {
            ColumnValue.Text(0, Encoding.UTF8.GetBytes("table")),
            ColumnValue.Text(1, Encoding.UTF8.GetBytes("_sharc_ledger")),
            ColumnValue.Text(2, Encoding.UTF8.GetBytes("_sharc_ledger")),
            ColumnValue.FromInt64(3, 2), // RootPage = 2
            ColumnValue.Text(4, Encoding.UTF8.GetBytes("CREATE TABLE _sharc_ledger(SequenceNumber INTEGER PRIMARY KEY, Timestamp INTEGER, AgentId TEXT, Payload BLOB, PayloadHash BLOB, PreviousHash BLOB, Signature BLOB)"))
        };

        var agentsCols = new[] {
            ColumnValue.Text(0, Encoding.UTF8.GetBytes("table")),
            ColumnValue.Text(1, Encoding.UTF8.GetBytes("_sharc_agents")),
            ColumnValue.Text(2, Encoding.UTF8.GetBytes("_sharc_agents")),
            ColumnValue.FromInt64(3, 3), // RootPage = 3
            ColumnValue.Text(4, Encoding.UTF8.GetBytes("CREATE TABLE _sharc_agents(AgentId TEXT PRIMARY KEY, Class INTEGER, PublicKey BLOB, AuthorityCeiling INTEGER, WriteScope TEXT, ReadScope TEXT, ValidityStart INTEGER, ValidityEnd INTEGER, ParentAgent TEXT, CoSignRequired INTEGER, Signature BLOB, Algorithm INTEGER)"))
        };

        var scoresCols = new[] {
            ColumnValue.Text(0, Encoding.UTF8.GetBytes("table")),
            ColumnValue.Text(1, Encoding.UTF8.GetBytes("_sharc_scores")),
            ColumnValue.Text(2, Encoding.UTF8.GetBytes("_sharc_scores")),
            ColumnValue.FromInt64(3, 4), // RootPage = 4
            ColumnValue.Text(4, Encoding.UTF8.GetBytes("CREATE TABLE _sharc_scores(AgentId TEXT PRIMARY KEY, Score REAL, Confidence REAL, LastUpdated INTEGER, RatingCount INTEGER, Alpha REAL, Beta REAL)"))
        };

        var auditCols = new[] {
            ColumnValue.Text(0, Encoding.UTF8.GetBytes("table")),
            ColumnValue.Text(1, Encoding.UTF8.GetBytes("_sharc_audit")),
            ColumnValue.Text(2, Encoding.UTF8.GetBytes("_sharc_audit")),
            ColumnValue.FromInt64(3, 5), // RootPage = 5
            ColumnValue.Text(4, Encoding.UTF8.GetBytes("CREATE TABLE _sharc_audit(EventId INTEGER PRIMARY KEY, Timestamp INTEGER, EventType INTEGER, AgentId TEXT, Details TEXT, PreviousHash BLOB, Hash BLOB)"))
        };

        // Encode records
        byte[] r1 = new byte[RecordEncoder.ComputeEncodedSize(ledgerCols)];
        RecordEncoder.EncodeRecord(ledgerCols, r1);

        byte[] r2 = new byte[RecordEncoder.ComputeEncodedSize(agentsCols)];
        RecordEncoder.EncodeRecord(agentsCols, r2);

        byte[] r3 = new byte[RecordEncoder.ComputeEncodedSize(scoresCols)];
        RecordEncoder.EncodeRecord(scoresCols, r3);

        byte[] r4 = new byte[RecordEncoder.ComputeEncodedSize(auditCols)];
        RecordEncoder.EncodeRecord(auditCols, r4);

        // Build Page 1 (Schema)
        Span<byte> cell1 = stackalloc byte[r1.Length + 10];
        int l1 = CellBuilder.BuildTableLeafCell(1, r1, cell1, pageSize);
        ushort o1 = (ushort)(pageSize - l1);
        cell1[..l1].CopyTo(data.AsSpan(o1));

        Span<byte> cell2 = stackalloc byte[r2.Length + 10];
        int l2 = CellBuilder.BuildTableLeafCell(2, r2, cell2, pageSize);
        ushort o2 = (ushort)(o1 - l2);
        cell2[..l2].CopyTo(data.AsSpan(o2));

        Span<byte> cell3 = stackalloc byte[r3.Length + 10];
        int l3 = CellBuilder.BuildTableLeafCell(3, r3, cell3, pageSize);
        ushort o3 = (ushort)(o2 - l3);
        cell3[..l3].CopyTo(data.AsSpan(o3));

        Span<byte> cell4 = stackalloc byte[r4.Length + 10];
        int l4 = CellBuilder.BuildTableLeafCell(4, r4, cell4, pageSize);
        ushort o4 = (ushort)(o3 - l4);
        cell4[..l4].CopyTo(data.AsSpan(o4));

        var p1Header = new BTreePageHeader(BTreePageType.LeafTable, 0, (ushort)4, o4, 0, 0);
        BTreePageHeader.Write(data.AsSpan(SQLiteLayout.DatabaseHeaderSize), p1Header);

        int ptrOffset = SQLiteLayout.DatabaseHeaderSize + SQLiteLayout.TableLeafHeaderSize;
        BinaryPrimitives.WriteUInt16BigEndian(data.AsSpan(ptrOffset), o1); ptrOffset += SQLiteLayout.CellPointerSize;
        BinaryPrimitives.WriteUInt16BigEndian(data.AsSpan(ptrOffset), o2); ptrOffset += SQLiteLayout.CellPointerSize;
        BinaryPrimitives.WriteUInt16BigEndian(data.AsSpan(ptrOffset), o3); ptrOffset += SQLiteLayout.CellPointerSize;
        BinaryPrimitives.WriteUInt16BigEndian(data.AsSpan(ptrOffset), o4);

        // Page 2-5
        for (int i = 1; i <= 4; i++)
        {
            var pHeader = new BTreePageHeader(BTreePageType.LeafTable, 0, 0, (ushort)pageSize, 0, 0);
            BTreePageHeader.Write(data.AsSpan(pageSize * i), pHeader);
        }

        return data;
    }

    public static SharcDatabase Open(string path, SharcOpenOptions? options = null)
    {
        options ??= new SharcOpenOptions();

        var journalPath = path + ".journal";
        if (File.Exists(journalPath))
        {
            RollbackJournal.Recover(path, journalPath);
            File.Delete(journalPath);
        }

        bool isEncryptedFile;
        {
            Span<byte> magic = stackalloc byte[6];
            using var probe = new FileStream(path, FileMode.Open, FileAccess.Read, options.FileShareMode);
            int read = probe.Read(magic);
            isEncryptedFile = read >= 6 && EncryptionHeader.HasMagic(magic);
        }

        if (isEncryptedFile)
        {
            byte[] fileData;
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, options.FileShareMode))
            {
                fileData = new byte[fs.Length];
                fs.ReadExactly(fileData);
            }
            return OpenEncrypted(fileData, options);
        }

        IPageSource pageSource = options.PreloadToMemory
            ? new MemoryPageSource(File.ReadAllBytes(path))
            : new FilePageSource(path, options.FileShareMode, allowWrites: options.Writable);

        try
        {
            var headerSpan = pageSource.GetPage(1);
            var header = DatabaseHeader.Parse(headerSpan);

            if (header.IsWalMode)
            {
                var walPath = path + "-wal";
                if (File.Exists(walPath))
                {
                    byte[] walData;
                    using (var walStream = new FileStream(walPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    {
                        walData = new byte[walStream.Length];
                        walStream.ReadExactly(walData);
                    }

                    if (walData.Length >= WalHeader.HeaderSize)
                    {
                        var walFrameMap = WalReader.ReadFrameMap(walData, header.PageSize);
                        if (walFrameMap.Count > 0)
                            pageSource = new WalPageSource(pageSource, walData, walFrameMap);
                    }
                }
            }

            return CreateFromPageSource(pageSource, options, filePath: path);
        }
        catch
        {
            pageSource.Dispose();
            throw;
        }
    }

    private static SharcDatabase OpenEncrypted(byte[] fileData, SharcOpenOptions options)
    {
        var password = options.Encryption?.Password
            ?? throw new SharcCryptoException("Password required for encrypted database.");

        var encHeader = EncryptionHeader.Parse(fileData);
        var passwordBytes = Encoding.UTF8.GetBytes(password);

        var keyHandle = SharcKeyHandle.DeriveKey(
            passwordBytes, encHeader.Salt.Span,
            encHeader.TimeCost, encHeader.MemoryCostKiB, encHeader.Parallelism);

        try
        {
            var computedHmac = keyHandle.ComputeHmac(encHeader.Salt.Span);
            if (!computedHmac.AsSpan().SequenceEqual(encHeader.VerificationHash.Span))
                throw new SharcCryptoException("Wrong password or corrupted encryption header.");

            var transform = new AesGcmPageTransform(keyHandle);
            int encryptedPageSize = transform.TransformedPageSize(encHeader.PageSize);

            var encryptedData = new ReadOnlyMemory<byte>(fileData, EncryptionHeader.HeaderSize, fileData.Length - EncryptionHeader.HeaderSize);
            var innerSource = new MemoryPageSource(encryptedData, encryptedPageSize, encHeader.PageCount);
            var decryptingSource = new DecryptingPageSource(innerSource, transform, encHeader.PageSize);

            try
            {
                return CreateFromPageSource(decryptingSource, options, isEncrypted: true, keyHandle: keyHandle);
            }
            catch
            {
                decryptingSource.Dispose();
                throw;
            }
        }
        catch
        {
            keyHandle.Dispose();
            throw;
        }
    }

    public static SharcDatabase OpenMemory(ReadOnlyMemory<byte> data, SharcOpenOptions? options = null)
    {
        if (data.IsEmpty)
            throw new InvalidDatabaseException("Database buffer is empty.");

        if (!DatabaseHeader.HasValidMagic(data.Span))
            throw new InvalidDatabaseException("Invalid SQLite magic string.");

        options ??= new SharcOpenOptions();
        SharcSchema? cachedSchema = null;
        SchemaCache.SchemaCacheHandle? schemaCacheHandle = null;

        if (!options.Writable)
        {
            var header = DatabaseHeader.Parse(data.Span);
            SchemaCache.TryGet(data, header, out cachedSchema, out schemaCacheHandle);
        }

        IPageSource pageSource = new MemoryPageSource(data);
        return CreateFromPageSource(pageSource, options,
            preloadedSchema: cachedSchema,
            schemaCacheHandle: schemaCacheHandle);
    }

    public static SharcDatabase CreateFromPageSource(IPageSource rawSource, SharcOpenOptions options,
        bool isEncrypted = false, string? filePath = null, SharcKeyHandle? keyHandle = null,
        SharcSchema? preloadedSchema = null, SchemaCache.SchemaCacheHandle? schemaCacheHandle = null)
    {
        IPageSource pageSource = rawSource;

        if (options.PageCacheSize > 0 && rawSource is not MemoryPageSource)
            pageSource = new CachedPageSource(rawSource, options.PageCacheSize);

        var headerSpan = pageSource.GetPage(1);
        var header = DatabaseHeader.Parse(headerSpan);

        if (header.TextEncoding is 2 or 3)
            throw new UnsupportedFeatureException("UTF-16 text encoding");

        var proxySource = new ProxyPageSource(pageSource);
        var recordDecoder = new RecordDecoder();

        // Dispatch to generic BTreeReader<T> based on concrete page source type.
        // For read-only databases, bypass ProxyPageSource to eliminate an interface dispatch layer.
        // For writable databases, route through ProxyPageSource (needed for SetTarget during transactions).
        IBTreeReader bTreeReader = options.Writable
            ? new BTreeReader<ProxyPageSource>(proxySource, header)
            : CreateSpecializedReader(pageSource, header);

        var info = new SharcDatabaseInfo
        {
            PageSize = header.PageSize,
            PageCount = header.PageCount,
            TextEncoding = (SharcTextEncoding)header.TextEncoding,
            SchemaFormat = header.SchemaFormat,
            UserVersion = header.UserVersion,
            ApplicationId = header.ApplicationId,
            SqliteVersion = header.SqliteVersionNumber,
            IsWalMode = header.IsWalMode,
            IsEncrypted = isEncrypted
        };

        return new SharcDatabase(proxySource, pageSource, header, bTreeReader, recordDecoder,
            info, filePath, keyHandle, preloadedSchema, schemaCacheHandle);
    }

    /// <summary>
    /// Creates a BTreeReader specialized for the concrete page source type.
    /// The JIT devirtualizes GetPage/GetPageMemory calls inside cursors when
    /// the page source type is a sealed class known at generic instantiation time.
    /// </summary>
    private static IBTreeReader CreateSpecializedReader(IPageSource pageSource, DatabaseHeader header)
    {
        return pageSource switch
        {
            MemoryPageSource mem => new BTreeReader<MemoryPageSource>(mem, header),
            CachedPageSource cached => new BTreeReader<CachedPageSource>(cached, header),
            WalPageSource wal => new BTreeReader<WalPageSource>(wal, header),
            DecryptingPageSource dec => new BTreeReader<DecryptingPageSource>(dec, header),
            SafeM2MPageSource mmap => new BTreeReader<SafeM2MPageSource>(mmap, header),
            _ => new BTreeReader<IPageSource>(pageSource, header)
        };
    }
}
