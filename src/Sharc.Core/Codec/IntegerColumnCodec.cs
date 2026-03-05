// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.
#pragma warning disable CS1591

using System.Buffers;
using System.Buffers.Binary;
using System.IO;

namespace Sharc.Core.Codec;

/// <summary>
/// Top-level columnar integer compression codec — "The Bloodline".
/// Compresses int32/int64 columns using a 3-step pipeline:
///
/// 1. Mode Detection — analyse column to pick optimal decomposition
/// 2. Delta Encoding — consecutive differences collapse nearby values
/// 3. Bin + ANS Entropy Coding — approaches Shannon entropy limit
///
/// For small columns (&lt;10 elements), falls back to varint encoding
/// to avoid ANS overhead on tiny inputs.
///
/// Storage format (v2, CRC-enabled):
///   [1 byte:  mode (bit7 = crc-present, bits0-1 = delta_order)]
///   [4 bytes: element_count (LE)]
///   [4 bytes: payload_size (LE)]
///   [4 bytes: payload_crc32 (LE)]
///   [N bytes: payload]
///
/// Zero-allocation hot path: pools all intermediate arrays, uses
/// BinaryPrimitives for header encoding, span-based DeltaEncoder overloads.
/// </summary>
public sealed class IntegerColumnCodec
{
    /// <summary>Minimum elements before using ANS. Below this, use varint.</summary>
    private const int AnsThreshold = 10;

    /// <summary>Legacy header size: 1 (delta_order) + 4 (count) + 4 (payload_size).</summary>
    private const int HeaderSizeLegacy = 9;

    /// <summary>Current header size: legacy + 4-byte CRC32 payload checksum.</summary>
    private const int HeaderSizeV2 = 13;

    private const byte CrcFlag = 0x80;
    private const byte DeltaOrderMask = 0x03;

    public static byte[] Encode(ReadOnlySpan<long> values)
    {
        if (values.Length == 0) return [];

        int deltaOrder = DeltaEncoder.RecommendOrder(values);
        int len = values.Length;

        // Pool the delta-encoded intermediate
        var deltaPool = ArrayPool<long>.Shared.Rent(len);
        var unsignedPool = ArrayPool<ulong>.Shared.Rent(len);
        try
        {
            var deltaSpan = deltaPool.AsSpan(0, len);

            switch (deltaOrder)
            {
                case 1:
                    DeltaEncoder.EncodeDelta1(values, deltaSpan);
                    break;
                case 2:
                    DeltaEncoder.EncodeDelta2(values, deltaSpan);
                    break;
                default:
                    values.CopyTo(deltaSpan);
                    break;
            }

            // ZigZag encode into pooled ulong[]
            var unsignedSpan = unsignedPool.AsSpan(0, len);
            DeltaEncoder.ZigZagEncode(deltaSpan, unsignedSpan);

            byte[] payload = len < AnsThreshold
                ? EncodeVarint(unsignedSpan)
                : EncodeAns(unsignedSpan);

            // Write header + payload using BinaryPrimitives
            var output = new byte[HeaderSizeV2 + payload.Length];
            var outSpan = output.AsSpan();

            outSpan[0] = (byte)(deltaOrder | CrcFlag);
            BinaryPrimitives.WriteInt32LittleEndian(outSpan.Slice(1), len);
            BinaryPrimitives.WriteInt32LittleEndian(outSpan.Slice(5), payload.Length);
            BinaryPrimitives.WriteUInt32LittleEndian(outSpan.Slice(9), Crc32.Compute(payload));
            payload.AsSpan().CopyTo(outSpan.Slice(HeaderSizeV2));

            return output;
        }
        finally
        {
            ArrayPool<long>.Shared.Return(deltaPool);
            ArrayPool<ulong>.Shared.Return(unsignedPool);
        }
    }

    public static byte[] Encode(ReadOnlySpan<int> values)
    {
        var longs = ArrayPool<long>.Shared.Rent(values.Length);
        try
        {
            for (int i = 0; i < values.Length; i++)
                longs[i] = values[i];
            return Encode(longs.AsSpan(0, values.Length));
        }
        finally
        {
            ArrayPool<long>.Shared.Return(longs);
        }
    }

    public static long[] Decode(byte[] data)
    {
        if (data.Length < HeaderSizeLegacy) return [];

        var span = data.AsSpan();

        byte mode = span[0];
        bool hasCrc = (mode & CrcFlag) != 0;
        int deltaOrder = mode & DeltaOrderMask;
        int elementCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(1));
        int payloadSize = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(5));

        if (elementCount <= 0 || payloadSize < 0) return [];
        if (deltaOrder is < 0 or > 2)
            throw new InvalidDataException($"Unsupported delta order '{deltaOrder}'.");

        int headerSize = hasCrc ? HeaderSizeV2 : HeaderSizeLegacy;
        if (data.Length < headerSize) return [];

        int availablePayload = data.Length - headerSize;
        int available = Math.Min(payloadSize, availablePayload);
        if (available < 0) return [];

        var payloadSpan = span.Slice(headerSize, available);
        if (hasCrc)
        {
            if (available != payloadSize)
                throw new InvalidDataException("Integer payload is truncated (CRC mode).");

            uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(9));
            uint actualCrc = Crc32.Compute(payloadSpan);
            if (expectedCrc != actualCrc)
            {
                throw new InvalidDataException(
                    $"Integer payload CRC mismatch. Expected 0x{expectedCrc:X8}, got 0x{actualCrc:X8}.");
            }
        }

        // Pool intermediate arrays
        var unsignedPool = ArrayPool<ulong>.Shared.Rent(elementCount);
        var signedPool = ArrayPool<long>.Shared.Rent(elementCount);
        try
        {
            var unsignedSpan = unsignedPool.AsSpan(0, elementCount);

            if (elementCount < AnsThreshold)
                DecodeVarint(payloadSpan, unsignedSpan);
            else
                DecodeAns(payloadSpan, unsignedSpan);

            // ZigZag decode into pooled long[]
            var signedSpan = signedPool.AsSpan(0, elementCount);
            DeltaEncoder.ZigZagDecode(unsignedSpan, signedSpan);

            // Delta decode — result is the final output, must allocate
            var result = new long[elementCount];
            switch (deltaOrder)
            {
                case 1:
                    DeltaEncoder.DecodeDelta1(signedSpan, result);
                    break;
                case 2:
                    DeltaEncoder.DecodeDelta2(signedSpan, result);
                    break;
                default:
                    signedSpan.CopyTo(result);
                    break;
            }

            return result;
        }
        finally
        {
            ArrayPool<ulong>.Shared.Return(unsignedPool);
            ArrayPool<long>.Shared.Return(signedPool);
        }
    }

    public static int[] DecodeInt32(byte[] data)
    {
        var longs = Decode(data);
        var result = new int[longs.Length];
        for (int i = 0; i < longs.Length; i++)
            result[i] = (int)longs[i];
        return result;
    }

    /// <summary>
    /// Returns the compression ratio: compressed_size / raw_size.
    /// Lower is better. 0.5 = 50% compression.
    /// </summary>
    public static double MeasureRatio(ReadOnlySpan<long> values)
    {
        if (values.Length == 0) return 1.0;
        var compressed = Encode(values);
        double rawSize = values.Length * 8.0;
        return compressed.Length / rawSize;
    }

    private static byte[] EncodeVarint(ReadOnlySpan<ulong> values)
    {
        using var writer = new BitWriter();
        foreach (var v in values)
            writer.WriteVarInt(v);
        return writer.ToArray();
    }

    private static void DecodeVarint(ReadOnlySpan<byte> data, Span<ulong> result)
    {
        var reader = new BitReader(data);
        for (int i = 0; i < result.Length; i++)
            result[i] = reader.ReadVarInt();
    }

    private static byte[] EncodeAns(ReadOnlySpan<ulong> values)
    {
        var layout = BinOptimiser.Optimise(values);
        if (layout.Bins.Length == 0) return [];

        int len = values.Length;
        var binIndices = ArrayPool<int>.Shared.Rent(len);
        var offsets = ArrayPool<ulong>.Shared.Rent(len);
        try
        {
            for (int i = 0; i < len; i++)
            {
                var (idx, off) = layout.Map(values[i]);
                binIndices[i] = idx;
                offsets[i] = off;
            }

            var rawFreqs = new int[layout.Bins.Length];
            for (int i = 0; i < len; i++)
                rawFreqs[binIndices[i]]++;

            var encoder = new AnsEncoder(rawFreqs);
            var ansBytes = encoder.Encode(binIndices.AsSpan(0, len));
            var normFreqs = encoder.GetNormalisedFrequencies();

            // Offsets: for each value, write offset using the bin's OffsetBits
            using var offsetWriter = new BitWriter();
            for (int i = 0; i < len; i++)
            {
                int bits = layout.Bins[binIndices[i]].OffsetBits;
                if (bits > 0)
                    offsetWriter.Write(offsets[i], bits);
            }
            var offsetBytes = offsetWriter.ToArray();

            // Calculate total output size and write with BinaryPrimitives
            int binCount = layout.Bins.Length;
            // Layout: binCount(4) + bins(binCount*16) + freqs(binCount*4) + ansSize(4) + ans + offsetSize(4) + offsets
            int totalSize = 4 + binCount * 16 + binCount * 4 + 4 + ansBytes.Length + 4 + offsetBytes.Length;
            var output = new byte[totalSize];
            var outSpan = output.AsSpan();
            int pos = 0;

            // Bin count + definitions
            BinaryPrimitives.WriteInt32LittleEndian(outSpan.Slice(pos), binCount);
            pos += 4;
            foreach (var bin in layout.Bins)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(outSpan.Slice(pos), bin.LowerBound);
                pos += 8;
                BinaryPrimitives.WriteUInt64LittleEndian(outSpan.Slice(pos), bin.UpperBound);
                pos += 8;
            }

            // Frequency table
            foreach (var f in normFreqs)
            {
                BinaryPrimitives.WriteInt32LittleEndian(outSpan.Slice(pos), f);
                pos += 4;
            }

            // ANS payload
            BinaryPrimitives.WriteInt32LittleEndian(outSpan.Slice(pos), ansBytes.Length);
            pos += 4;
            ansBytes.AsSpan().CopyTo(outSpan.Slice(pos));
            pos += ansBytes.Length;

            // Offset data
            BinaryPrimitives.WriteInt32LittleEndian(outSpan.Slice(pos), offsetBytes.Length);
            pos += 4;
            offsetBytes.AsSpan().CopyTo(outSpan.Slice(pos));

            return output;
        }
        finally
        {
            ArrayPool<int>.Shared.Return(binIndices);
            ArrayPool<ulong>.Shared.Return(offsets);
        }
    }

    private static void DecodeAns(ReadOnlySpan<byte> data, Span<ulong> result)
    {
        if (data.Length < 4)
        {
            result.Clear();
            return;
        }

        int count = result.Length;
        int pos = 0;

        int binCount = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos));
        pos += 4;
        if (binCount <= 0 || binCount > 100_000 || pos + binCount * 16 > data.Length)
        {
            result.Clear();
            return;
        }

        var bins = new BinOptimiser.Bin[binCount];
        for (int i = 0; i < binCount; i++)
        {
            ulong lo = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(pos));
            pos += 8;
            ulong hi = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(pos));
            pos += 8;
            bins[i] = new BinOptimiser.Bin(lo, hi, 0);
        }

        var freqs = new int[binCount];
        for (int i = 0; i < binCount; i++)
        {
            freqs[i] = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos));
            pos += 4;
        }

        int ansSize = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos));
        pos += 4;
        var ansData = data.Slice(pos, Math.Min(ansSize, data.Length - pos)).ToArray();
        pos += ansSize;

        int offsetSize = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos));
        pos += 4;
        var offsetData = data.Slice(pos, Math.Min(offsetSize, data.Length - pos));

        // ANS decode bin indices
        var decoder = new AnsDecoder(freqs);
        var binIndices = decoder.Decode(ansData, count);

        // Reconstruct values from (binIndex, offset)
        var offsetReader = new BitReader(offsetData);
        var layout = new BinOptimiser.BinLayout { Bins = bins };

        for (int i = 0; i < count; i++)
        {
            int binIdx = binIndices[i];
            int bits = bins[binIdx].OffsetBits;
            ulong offset = bits > 0 ? offsetReader.ReadLong(bits) : 0;
            result[i] = layout.Unmap(binIdx, offset);
        }
    }
}
