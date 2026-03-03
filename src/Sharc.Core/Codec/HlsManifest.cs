// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

#pragma warning disable CS1591 // Missing XML comment — binary format constants
#pragma warning disable CA1711 // Identifier should not have incorrect suffix

namespace Sharc.Core.Codec;

/// <summary>
/// Parsed manifest for a Hash-Linked Segments (.hls) archive.
/// Each segment (chunk) is linked by its SHA-256 hash stored in the manifest,
/// forming an integrity chain from individual chunks back to the original file.
/// The manifest describes the original file, chunk layout, and integrity hashes.
/// </summary>
public sealed class HlsManifest
{
    /// <summary>10-byte magic identifying an .hls manifest file.</summary>
    public static ReadOnlySpan<byte> Magic => "SHARC-HLS\0"u8;

    /// <summary>10-byte magic identifying an .hls chunk file.</summary>
    public static ReadOnlySpan<byte> ChunkMagic => "HLS-CHUNK\0"u8;

    public const ushort CurrentVersion = 1;

    public const int ManifestHeaderSize = 318; // 10+2+2+8+4+4+32+256

    public const int ChunkHeaderSize = 18; // 10+4+4

    /// <summary>Default maximum data bytes per chunk (19 MB).</summary>
    public const int DefaultChunkMaxSize = 19 * 1024 * 1024;

    [Flags]
    public enum HlsFlags : ushort
    {
        None = 0,
        Encrypted = 1 << 0,
        Progressive = 1 << 1,
        Checksummed = 1 << 2,
    }

    public ushort Version { get; init; }
    public HlsFlags Flags { get; init; }
    public long OriginalSize { get; init; }
    public int ChunkMaxSize { get; init; }
    public int ChunkCount { get; init; }
    public byte[] OriginalSha256 { get; init; } = Array.Empty<byte>();
    public string OriginalFileName { get; init; } = string.Empty;
    public byte[][] ChunkSha256 { get; init; } = Array.Empty<byte[]>();
}
