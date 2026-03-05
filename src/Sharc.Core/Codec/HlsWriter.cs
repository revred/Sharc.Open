// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Sharc.Core.Codec;

/// <summary>
/// Splits a file into Hash-Linked Segments (.hls) for storage under a size ceiling.
/// Produces a manifest (.hls) and N chunk files (.001.hls, .002.hls, ...).
/// Each chunk is hash-linked via SHA-256, enabling integrity verification
/// of individual segments without requiring the full file.
///
/// Supports progressive/LOD, password sharding, streaming, and
/// distribution across network boundaries.
/// </summary>
public static class HlsWriter
{
    /// <summary>
    /// Splits <paramref name="inputPath"/> into chunks of at most
    /// <paramref name="chunkMaxBytes"/> data bytes each.
    /// Writes the manifest and chunks into <paramref name="outputDir"/>.
    /// Returns the manifest path.
    /// </summary>
    public static string Split(string inputPath, string? outputDir = null, int chunkMaxBytes = HlsManifest.DefaultChunkMaxSize)
    {
        if (!File.Exists(inputPath))
            throw new FileNotFoundException("Input file not found.", inputPath);
        if (chunkMaxBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(chunkMaxBytes));

        outputDir ??= Path.GetDirectoryName(inputPath) ?? ".";
        Directory.CreateDirectory(outputDir);

        string baseName = Path.GetFileName(inputPath);
        string manifestPath = Path.Combine(outputDir, baseName + ".hls");

        byte[] fileData = File.ReadAllBytes(inputPath);
        long originalSize = fileData.Length;
        byte[] originalHash = SHA256.HashData(fileData);

        int chunkCount = (int)Math.Ceiling((double)originalSize / chunkMaxBytes);
        if (chunkCount == 0) chunkCount = 1;

        var chunkHashes = new byte[chunkCount][];

        // Write chunks
        int offset = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            int dataSize = Math.Min(chunkMaxBytes, fileData.Length - offset);
            var chunkData = fileData.AsSpan(offset, dataSize);
            chunkHashes[i] = SHA256.HashData(chunkData);

            string chunkPath = Path.Combine(outputDir, $"{baseName}.{(i + 1):D3}.hls");
            WriteChunk(chunkPath, i + 1, chunkData);
            offset += dataSize;
        }

        // Write manifest
        WriteManifest(manifestPath, originalSize, chunkMaxBytes, chunkCount,
            originalHash, baseName, chunkHashes);

        return manifestPath;
    }

    private static void WriteManifest(string path, long originalSize, int chunkMaxSize,
        int chunkCount, byte[] originalHash, string originalFileName, byte[][] chunkHashes)
    {
        int totalSize = HlsManifest.ManifestHeaderSize + chunkCount * 32;
        var buffer = new byte[totalSize];
        var span = buffer.AsSpan();

        // Magic
        HlsManifest.Magic.CopyTo(span);
        // Version
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(10), HlsManifest.CurrentVersion);
        // Flags: checksummed
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(12), (ushort)HlsManifest.HlsFlags.Checksummed);
        // OriginalSize
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(14), originalSize);
        // ChunkMaxSize
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(22), chunkMaxSize);
        // ChunkCount
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(26), chunkCount);
        // OriginalSHA256
        originalHash.AsSpan().CopyTo(span.Slice(30));
        // OriginalFileName (UTF-8, 256 bytes null-padded)
        var nameBytes = Encoding.UTF8.GetBytes(originalFileName);
        nameBytes.AsSpan(0, Math.Min(nameBytes.Length, 255)).CopyTo(span.Slice(62));
        // Chunk hashes
        for (int i = 0; i < chunkCount; i++)
            chunkHashes[i].AsSpan().CopyTo(span.Slice(318 + i * 32));

        File.WriteAllBytes(path, buffer);
    }

    private static void WriteChunk(string path, int chunkIndex, ReadOnlySpan<byte> data)
    {
        var buffer = new byte[HlsManifest.ChunkHeaderSize + data.Length];
        var span = buffer.AsSpan();

        HlsManifest.ChunkMagic.CopyTo(span);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(10), chunkIndex);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(14), data.Length);
        data.CopyTo(span.Slice(HlsManifest.ChunkHeaderSize));

        File.WriteAllBytes(path, buffer);
    }
}
