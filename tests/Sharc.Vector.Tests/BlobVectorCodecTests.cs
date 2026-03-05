// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using Sharc.Vector;
using Xunit;

namespace Sharc.Vector.Tests;

public sealed class BlobVectorCodecTests
{
    [Fact]
    public void Encode_Decode_Roundtrip()
    {
        float[] original = [1.0f, 2.5f, -3.14f, 0.0f, float.MaxValue];
        byte[] encoded = BlobVectorCodec.Encode(original);
        ReadOnlySpan<float> decoded = BlobVectorCodec.Decode(encoded);

        Assert.Equal(original.Length, decoded.Length);
        for (int i = 0; i < original.Length; i++)
            Assert.Equal(original[i], decoded[i]);
    }

    [Fact]
    public void Decode_ReturnsCorrectLength()
    {
        float[] vector = [1.0f, 2.0f, 3.0f];
        byte[] encoded = BlobVectorCodec.Encode(vector);

        ReadOnlySpan<float> decoded = BlobVectorCodec.Decode(encoded);

        Assert.Equal(3, decoded.Length);
    }

    [Fact]
    public void Decode_InvalidByteLength_ThrowsFormatException()
    {
        byte[] invalid = [1, 2, 3];
        Assert.Throws<FormatException>(() => BlobVectorCodec.Decode(invalid));
    }

    [Fact]
    public void TryDecode_InvalidByteLength_ReturnsFalse()
    {
        byte[] invalid = [1, 2, 3];
        bool ok = BlobVectorCodec.TryDecode(invalid, out ReadOnlySpan<float> decoded);
        Assert.False(ok);
        Assert.True(decoded.IsEmpty);
    }

    [Fact]
    public void GetDimensions_384Dim_Returns384()
    {
        int blobSize = 384 * sizeof(float); // 1536 bytes
        Assert.Equal(384, BlobVectorCodec.GetDimensions(blobSize));
    }

    [Fact]
    public void GetDimensions_1536Dim_Returns1536()
    {
        int blobSize = 1536 * sizeof(float); // 6144 bytes
        Assert.Equal(1536, BlobVectorCodec.GetDimensions(blobSize));
    }

    [Fact]
    public void GetDimensions_ZeroBytes_ReturnsZero()
    {
        Assert.Equal(0, BlobVectorCodec.GetDimensions(0));
    }

    [Fact]
    public void GetDimensions_InvalidByteLength_ThrowsFormatException()
    {
        Assert.Throws<FormatException>(() => BlobVectorCodec.GetDimensions(6));
    }

    [Fact]
    public void Encode_IntoBuffer_WritesCorrectBytes()
    {
        float[] vector = [1.0f, 2.0f, 3.0f];
        byte[] buffer = new byte[vector.Length * sizeof(float)];

        int bytesWritten = BlobVectorCodec.Encode(vector, buffer);

        Assert.Equal(12, bytesWritten);

        // Decode from buffer and verify
        ReadOnlySpan<float> decoded = BlobVectorCodec.Decode(buffer);
        Assert.Equal(1.0f, decoded[0]);
        Assert.Equal(2.0f, decoded[1]);
        Assert.Equal(3.0f, decoded[2]);
    }

    [Fact]
    public void Encode_EmptyVector_ReturnsEmptyArray()
    {
        float[] empty = [];
        byte[] encoded = BlobVectorCodec.Encode(empty);

        Assert.Empty(encoded);
    }

    [Fact]
    public void Encode_SingleFloat_Produces4Bytes()
    {
        float[] single = [42.0f];
        byte[] encoded = BlobVectorCodec.Encode(single);

        Assert.Equal(4, encoded.Length);
        Assert.Equal(1, BlobVectorCodec.GetDimensions(encoded.Length));
    }

    [Fact]
    public void Decode_PreservesNegativeZero()
    {
        float[] vector = [-0.0f];
        byte[] encoded = BlobVectorCodec.Encode(vector);
        ReadOnlySpan<float> decoded = BlobVectorCodec.Decode(encoded);

        Assert.True(float.IsNegative(decoded[0]));
    }

    [Fact]
    public void Decode_PreservesSpecialValues()
    {
        float[] vector = [float.NaN, float.PositiveInfinity, float.NegativeInfinity, float.Epsilon];
        byte[] encoded = BlobVectorCodec.Encode(vector);
        ReadOnlySpan<float> decoded = BlobVectorCodec.Decode(encoded);

        Assert.True(float.IsNaN(decoded[0]));
        Assert.True(float.IsPositiveInfinity(decoded[1]));
        Assert.True(float.IsNegativeInfinity(decoded[2]));
        Assert.Equal(float.Epsilon, decoded[3]);
    }
}
