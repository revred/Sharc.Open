// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.
#pragma warning disable CS1591

using System.Buffers;
using System.Buffers.Binary;

namespace Sharc.Core.Codec;

/// <summary>
/// rANS (range Asymmetric Numeral Systems) encoder.
/// Achieves near-Shannon-entropy compression for symbol sequences
/// given their frequency distribution.
///
/// Uses a stack-based approach: encode processes symbols in reverse,
/// collecting renorm outputs. The final bitstream is written so the
/// decoder can read it forward.
///
/// Table-based rANS with 12-bit precision.
/// Zero-allocation hot path: pools renorm stack and output buffer.
/// </summary>
public sealed class AnsEncoder
{
    /// <summary>Log2 of the frequency table size.</summary>
    internal const int TableLog = 12;

    /// <summary>Total frequency table size (must be power of 2).</summary>
    internal const int TableSize = 1 << TableLog; // 4096

    private readonly int[] _frequencies;
    private readonly int[] _cumulativeFreqs;

    public AnsEncoder(int[] rawFrequencies)
    {
        _frequencies = NormaliseFrequencies(rawFrequencies);
        _cumulativeFreqs = BuildCumulativeTable(_frequencies);
    }

    /// <summary>
    /// Encodes a sequence of symbols into a compressed byte array.
    /// The output format is: [final_state:4 bytes] [renorm_count:4 bytes] [renorm_words: 2 bytes each]
    /// Uses ArrayPool for the renorm stack to avoid List allocation.
    /// </summary>
    public byte[] Encode(ReadOnlySpan<int> symbols)
    {
        if (symbols.Length == 0) return [];

        uint state = TableSize;

        // Pool the renorm stack — worst case each symbol emits one renorm word
        var renormPool = ArrayPool<ushort>.Shared.Rent(symbols.Length);
        int renormCount = 0;

        try
        {
            // Encode in reverse order
            for (int i = symbols.Length - 1; i >= 0; i--)
            {
                int sym = symbols[i];
                int freq = _frequencies[sym];
                int cumFreq = _cumulativeFreqs[sym];

                if (freq == 0) continue;

                // Renormalise: push 16-bit chunks when state gets too large
                uint maxState = (uint)freq << 16;
                while (state >= maxState)
                {
                    // Grow if needed (very rare — multiple renorms per symbol)
                    if (renormCount == renormPool.Length)
                    {
                        var bigger = ArrayPool<ushort>.Shared.Rent(renormPool.Length * 2);
                        renormPool.AsSpan(0, renormCount).CopyTo(bigger);
                        ArrayPool<ushort>.Shared.Return(renormPool);
                        renormPool = bigger;
                    }
                    renormPool[renormCount++] = (ushort)(state & 0xFFFF);
                    state >>= 16;
                }

                // rANS encode step
                state = (state / (uint)freq) * (uint)TableSize + (state % (uint)freq) + (uint)cumFreq;
            }

            // Write output with BinaryPrimitives into a pooled buffer
            // Layout: [state:4] [renormCount:4] [renormWords: renormCount*2]
            int outputSize = 4 + 4 + renormCount * 2;
            var output = new byte[outputSize];
            var span = output.AsSpan();

            BinaryPrimitives.WriteUInt32LittleEndian(span, state);
            BinaryPrimitives.WriteInt32LittleEndian(span.Slice(4), renormCount);

            // Write renorm words in reverse (last pushed = first consumed by decoder)
            int pos = 8;
            for (int i = renormCount - 1; i >= 0; i--)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(pos), renormPool[i]);
                pos += 2;
            }

            return output;
        }
        finally
        {
            ArrayPool<ushort>.Shared.Return(renormPool);
        }
    }

    public int[] GetNormalisedFrequencies() => (int[])_frequencies.Clone();

    internal static int[] NormaliseFrequencies(int[] rawFrequencies)
    {
        long total = 0;
        foreach (var f in rawFrequencies) total += f;
        if (total == 0) return new int[rawFrequencies.Length];

        var normalised = new int[rawFrequencies.Length];
        int remaining = TableSize;

        for (int i = 0; i < rawFrequencies.Length; i++)
        {
            if (rawFrequencies[i] > 0)
            {
                normalised[i] = Math.Max(1, (int)(rawFrequencies[i] * (long)TableSize / total));
                remaining -= normalised[i];
            }
        }

        // Distribute remainder to largest frequency symbol
        if (remaining > 0)
        {
            int maxIdx = 0;
            for (int i = 1; i < rawFrequencies.Length; i++)
                if (rawFrequencies[i] > rawFrequencies[maxIdx]) maxIdx = i;
            normalised[maxIdx] += remaining;
        }
        else if (remaining < 0)
        {
            while (remaining < 0)
            {
                int maxIdx = 0;
                for (int i = 1; i < normalised.Length; i++)
                    if (normalised[i] > normalised[maxIdx]) maxIdx = i;
                if (normalised[maxIdx] > 1) { normalised[maxIdx]--; remaining++; }
                else break;
            }
        }

        return normalised;
    }

    private static int[] BuildCumulativeTable(int[] frequencies)
    {
        var cumulative = new int[frequencies.Length];
        int sum = 0;
        for (int i = 0; i < frequencies.Length; i++)
        {
            cumulative[i] = sum;
            sum += frequencies[i];
        }
        return cumulative;
    }
}
