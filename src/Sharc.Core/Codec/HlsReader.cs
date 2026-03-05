// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Sharc.Core.Codec;

/// <summary>
/// Reads and reassembles Hash-Linked Segments (.hls) archives back into the original file.
/// Validates each segment's integrity via its SHA-256 hash stored in the manifest,
/// then verifies the reassembled file hash against the original.
/// </summary>
public static class HlsReader
{
    /// <summary>
    /// Parses an .hls manifest file and returns its metadata.
    /// </summary>
    public static HlsManifest ReadManifest(string manifestPath)
    {
        if (!File.Exists(manifestPath))
            throw new FileNotFoundException("Manifest not found.", manifestPath);

        byte[] data = File.ReadAllBytes(manifestPath);
        if (data.Length < HlsManifest.ManifestHeaderSize)
            throw new InvalidDataException("File too small to be an HLS manifest.");

        var span = data.AsSpan();

        // Validate magic
        if (!span.Slice(0, 10).SequenceEqual(HlsManifest.Magic))
            throw new InvalidDataException("Invalid HLS manifest magic.");

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(10));
        var flags = (HlsManifest.HlsFlags)BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(12));
        long originalSize = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(14));
        int chunkMaxSize = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(22));
        int chunkCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(26));
        byte[] originalHash = span.Slice(30, 32).ToArray();

        // Read original file name (null-terminated within 256 bytes)
        var nameSpan = span.Slice(62, 256);
        int nameLen = nameSpan.IndexOf((byte)0);
        if (nameLen < 0) nameLen = 256;
        string originalFileName = Encoding.UTF8.GetString(nameSpan.Slice(0, nameLen));

        // Read chunk hashes
        var chunkHashes = new byte[chunkCount][];
        for (int i = 0; i < chunkCount; i++)
        {
            int offset = 318 + i * 32;
            chunkHashes[i] = offset + 32 <= data.Length
                ? span.Slice(offset, 32).ToArray()
                : new byte[32];
        }

        return new HlsManifest
        {
            Version = version,
            Flags = flags,
            OriginalSize = originalSize,
            ChunkMaxSize = chunkMaxSize,
            ChunkCount = chunkCount,
            OriginalSha256 = originalHash,
            OriginalFileName = originalFileName,
            ChunkSha256 = chunkHashes,
        };
    }

    /// <summary>
    /// Reassembles an .hls archive into the original file.
    /// Chunk files must be in the same directory as the manifest.
    /// </summary>
    public static void Reassemble(string manifestPath, string outputPath)
    {
        var manifest = ReadManifest(manifestPath);
        string dir = Path.GetDirectoryName(manifestPath) ?? ".";
        string baseName = manifest.OriginalFileName;

        using var output = new FileStream(outputPath, FileMode.Create, FileAccess.ReadWrite);

        for (int i = 0; i < manifest.ChunkCount; i++)
        {
            string chunkPath = Path.Combine(dir, $"{baseName}.{(i + 1):D3}.hls");
            if (!File.Exists(chunkPath))
                throw new FileNotFoundException($"Chunk {i + 1} not found.", chunkPath);

            byte[] chunkFile = File.ReadAllBytes(chunkPath);
            var chunkSpan = chunkFile.AsSpan();

            // Validate chunk magic
            if (chunkSpan.Length < HlsManifest.ChunkHeaderSize ||
                !chunkSpan.Slice(0, 10).SequenceEqual(HlsManifest.ChunkMagic))
                throw new InvalidDataException($"Chunk {i + 1} has invalid magic.");

            int chunkIndex = BinaryPrimitives.ReadInt32LittleEndian(chunkSpan.Slice(10));
            int dataSize = BinaryPrimitives.ReadInt32LittleEndian(chunkSpan.Slice(14));

            if (chunkIndex != i + 1)
                throw new InvalidDataException($"Chunk index mismatch: expected {i + 1}, got {chunkIndex}.");

            var dataSpan = chunkSpan.Slice(HlsManifest.ChunkHeaderSize, dataSize);

            // Validate chunk hash if checksummed
            if (manifest.Flags.HasFlag(HlsManifest.HlsFlags.Checksummed))
            {
                byte[] actualHash = SHA256.HashData(dataSpan);
                if (!actualHash.AsSpan().SequenceEqual(manifest.ChunkSha256[i]))
                    throw new InvalidDataException($"Chunk {i + 1} SHA-256 mismatch.");
            }

            output.Write(dataSpan);
        }

        output.Flush();

        // Validate original file hash
        if (manifest.Flags.HasFlag(HlsManifest.HlsFlags.Checksummed))
        {
            output.Position = 0;
            byte[] actualHash = SHA256.HashData(output);
            if (!actualHash.AsSpan().SequenceEqual(manifest.OriginalSha256))
                throw new InvalidDataException("Reassembled file SHA-256 does not match manifest.");
        }
    }
}
