// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.
#pragma warning disable CS1591

using System.Buffers;

namespace Sharc.Core.Codec;

/// <summary>
/// Delta encoding for integer sequences. Transforms absolute values into
/// consecutive differences, collapsing nearby values to near-zero residuals.
///
/// Provides both allocating (returns new array) and in-place (writes to span)
/// overloads. The hot codec path uses span overloads with ArrayPool buffers.
/// </summary>
public static class DeltaEncoder
{
    // ── In-place span overloads (zero-alloc hot path) ─────────────

    public static void EncodeDelta1(ReadOnlySpan<long> values, Span<long> result)
    {
        if (values.Length == 0) return;
        result[0] = values[0];
        for (int i = 1; i < values.Length; i++)
            result[i] = values[i] - values[i - 1];
    }

    public static void DecodeDelta1(ReadOnlySpan<long> deltas, Span<long> result)
    {
        if (deltas.Length == 0) return;
        result[0] = deltas[0];
        for (int i = 1; i < deltas.Length; i++)
            result[i] = result[i - 1] + deltas[i];
    }

    public static void EncodeDelta2(ReadOnlySpan<long> values, Span<long> result)
    {
        if (values.Length <= 2)
        {
            EncodeDelta1(values, result);
            return;
        }

        // First pass: delta1 into result
        EncodeDelta1(values, result);

        // Second pass: delta of delta, in-place from index 2 onward.
        // Walk backwards so we don't overwrite values we still need.
        for (int i = values.Length - 1; i >= 2; i--)
            result[i] = result[i] - result[i - 1];
    }

    public static void DecodeDelta2(ReadOnlySpan<long> encoded, Span<long> result)
    {
        if (encoded.Length <= 2)
        {
            DecodeDelta1(encoded, result);
            return;
        }

        // Recover delta1
        result[0] = encoded[0];
        result[1] = encoded[1];
        for (int i = 2; i < encoded.Length; i++)
            result[i] = result[i - 1] + encoded[i];

        // Recover original from delta1
        for (int i = 1; i < result.Length; i++)
            result[i] = result[i - 1] + result[i];
    }

    public static void ZigZagEncode(ReadOnlySpan<long> values, Span<ulong> result)
    {
        for (int i = 0; i < values.Length; i++)
            result[i] = (ulong)((values[i] << 1) ^ (values[i] >> 63));
    }

    public static void ZigZagDecode(ReadOnlySpan<ulong> encoded, Span<long> result)
    {
        for (int i = 0; i < encoded.Length; i++)
            result[i] = (long)(encoded[i] >> 1) ^ -(long)(encoded[i] & 1);
    }

    // ── Allocating overloads (convenience / tests) ────────────────

    public static long[] EncodeDelta1(ReadOnlySpan<long> values)
    {
        if (values.Length == 0) return [];
        var result = new long[values.Length];
        EncodeDelta1(values, result);
        return result;
    }

    public static long[] DecodeDelta1(ReadOnlySpan<long> deltas)
    {
        if (deltas.Length == 0) return [];
        var result = new long[deltas.Length];
        DecodeDelta1(deltas, result);
        return result;
    }

    public static long[] EncodeDelta2(ReadOnlySpan<long> values)
    {
        if (values.Length == 0) return [];
        var result = new long[values.Length];
        EncodeDelta2(values, result);
        return result;
    }

    public static long[] DecodeDelta2(ReadOnlySpan<long> encoded)
    {
        if (encoded.Length == 0) return [];
        var result = new long[encoded.Length];
        DecodeDelta2(encoded, result);
        return result;
    }

    public static ulong[] ZigZagEncode(ReadOnlySpan<long> values)
    {
        var result = new ulong[values.Length];
        ZigZagEncode(values, result);
        return result;
    }

    public static long[] ZigZagDecode(ReadOnlySpan<ulong> encoded)
    {
        var result = new long[encoded.Length];
        ZigZagDecode(encoded, result);
        return result;
    }

    // ── Mode selection ────────────────────────────────────────────

    /// <summary>
    /// Analyses a value sequence and returns the recommended delta order.
    /// Uses pooled arrays for the trial encodings.
    /// </summary>
    public static int RecommendOrder(ReadOnlySpan<long> values)
    {
        if (values.Length < 4) return 0;

        var sumAbs0 = SumAbsolute(values);

        var d1 = ArrayPool<long>.Shared.Rent(values.Length);
        try
        {
            EncodeDelta1(values, d1.AsSpan(0, values.Length));
            var sumAbs1 = SumAbsolute(d1.AsSpan(0, values.Length));

            var d2 = ArrayPool<long>.Shared.Rent(values.Length);
            try
            {
                EncodeDelta2(values, d2.AsSpan(0, values.Length));
                var sumAbs2 = SumAbsolute(d2.AsSpan(0, values.Length));

                if (sumAbs2 <= sumAbs1 && sumAbs2 < sumAbs0) return 2;
                if (sumAbs1 < sumAbs0) return 1;
                return 0;
            }
            finally
            {
                ArrayPool<long>.Shared.Return(d2);
            }
        }
        finally
        {
            ArrayPool<long>.Shared.Return(d1);
        }
    }

    private static double SumAbsolute(ReadOnlySpan<long> values)
    {
        double sum = 0;
        for (int i = 0; i < values.Length; i++)
            sum += Math.Abs((double)values[i]);
        return sum;
    }
}
