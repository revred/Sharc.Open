// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.
#pragma warning disable CS1591

using System.Buffers;
using System.Buffers.Binary;

namespace Sharc.Core.Codec;

/// <summary>
/// rANS decoder. Reads the output of <see cref="AnsEncoder"/> and
/// recovers the original symbol sequence.
///
/// Input format: [final_state:4 bytes] [renorm_count:4 bytes] [renorm_words: 2 bytes each]
/// Zero-allocation hot path: uses BinaryPrimitives for reads, pools renormWords.
/// </summary>
public sealed class AnsDecoder
{
    private const int TableLog = AnsEncoder.TableLog;
    private const int TableSize = AnsEncoder.TableSize;
    private const int TableMask = TableSize - 1;

    private readonly int[] _frequencies;
    private readonly int[] _cumulativeFreqs;
    private readonly int[] _symbolLookup;

    public AnsDecoder(int[] normalisedFrequencies)
    {
        _frequencies = normalisedFrequencies;
        _cumulativeFreqs = BuildCumulativeTable(_frequencies);
        _symbolLookup = BuildSymbolLookup(_frequencies);
    }

    /// <summary>
    /// Decodes <paramref name="symbolCount"/> symbols from the compressed data.
    /// Uses BinaryPrimitives for header reads and ArrayPool for renormWords.
    /// </summary>
    public int[] Decode(byte[] data, int symbolCount)
    {
        if (symbolCount == 0 || data.Length < 8) return [];

        var span = data.AsSpan();

        uint state = BinaryPrimitives.ReadUInt32LittleEndian(span);
        int renormCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4));

        if (renormCount < 0 || 8 + renormCount * 2 > data.Length)
            return new int[symbolCount];

        // Pool the renormWords array
        var renormWords = ArrayPool<ushort>.Shared.Rent(renormCount);
        try
        {
            int offset = 8;
            for (int i = 0; i < renormCount; i++)
            {
                renormWords[i] = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(offset));
                offset += 2;
            }

            int renormIdx = 0;
            var symbols = new int[symbolCount];

            for (int i = 0; i < symbolCount; i++)
            {
                // Decode symbol from state
                int slot = (int)(state & TableMask);
                int sym = _symbolLookup[slot];
                symbols[i] = sym;

                int freq = _frequencies[sym];
                int cumFreq = _cumulativeFreqs[sym];

                // rANS decode step
                state = (uint)freq * (state >> TableLog) + (state & (uint)TableMask) - (uint)cumFreq;

                // Renormalise: absorb 16-bit words when state gets too small
                while (state < (uint)TableSize && renormIdx < renormCount)
                {
                    state = (state << 16) | renormWords[renormIdx++];
                }
            }

            return symbols;
        }
        finally
        {
            ArrayPool<ushort>.Shared.Return(renormWords);
        }
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

    private static int[] BuildSymbolLookup(int[] frequencies)
    {
        var lookup = new int[TableSize];
        int pos = 0;
        for (int sym = 0; sym < frequencies.Length; sym++)
        {
            for (int j = 0; j < frequencies[sym]; j++)
            {
                if (pos < TableSize)
                    lookup[pos++] = sym;
            }
        }
        return lookup;
    }
}
