// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Sharc.Vector;

/// <summary>
/// Encodes and decodes float vectors to/from SQLite BLOB format.
/// Layout: IEEE 754 little-endian, 4 bytes per dimension.
/// A 384-dim vector = 1,536 byte BLOB.
/// </summary>
public static class BlobVectorCodec
{
    private const int BytesPerDimension = sizeof(float);

    /// <summary>Decodes a BLOB span into a float span (zero-copy reinterpret).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<float> Decode(ReadOnlySpan<byte> blob)
    {
        if ((blob.Length & (BytesPerDimension - 1)) != 0)
            throw new FormatException(
                $"Vector blob length {blob.Length} is invalid. Length must be a multiple of {BytesPerDimension}.");

        return MemoryMarshal.Cast<byte, float>(blob);
    }

    /// <summary>
    /// Attempts to decode a BLOB span into a float span without throwing.
    /// Returns false when the payload length is not a valid float-vector encoding.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryDecode(ReadOnlySpan<byte> blob, out ReadOnlySpan<float> vector)
    {
        if ((blob.Length & (BytesPerDimension - 1)) != 0)
        {
            vector = default;
            return false;
        }

        vector = MemoryMarshal.Cast<byte, float>(blob);
        return true;
    }

    /// <summary>Encodes a float array into a BLOB byte array for storage.</summary>
    public static byte[] Encode(ReadOnlySpan<float> vector)
    {
        var bytes = new byte[vector.Length * BytesPerDimension];
        MemoryMarshal.AsBytes(vector).CopyTo(bytes);
        return bytes;
    }

    /// <summary>Encodes into a caller-provided buffer (zero-alloc).</summary>
    public static int Encode(ReadOnlySpan<float> vector, Span<byte> destination)
    {
        var source = MemoryMarshal.AsBytes(vector);
        if (destination.Length < source.Length)
            throw new ArgumentException(
                $"Destination buffer too small. Required {source.Length} bytes but got {destination.Length}.",
                nameof(destination));
        source.CopyTo(destination);
        return source.Length;
    }

    /// <summary>Returns the dimensionality of a vector stored in the given BLOB.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetDimensions(int blobByteLength)
    {
        if (blobByteLength < 0)
            throw new ArgumentOutOfRangeException(nameof(blobByteLength), blobByteLength, "Length must be non-negative.");
        if ((blobByteLength & (BytesPerDimension - 1)) != 0)
            throw new FormatException(
                $"Vector blob length {blobByteLength} is invalid. Length must be a multiple of {BytesPerDimension}.");
        return blobByteLength / BytesPerDimension;
    }
}
