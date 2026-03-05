// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.
#pragma warning disable CS1591

namespace Sharc.Core.Codec;

/// <summary>
/// Computes CRC-32 (IEEE 802.3, polynomial 0xEDB88320).
/// Used to protect bit-packed topology payloads against silent corruption.
/// </summary>
public static class Crc32
{
    private static readonly uint[] Table = BuildTable();

    public static uint Compute(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
        for (int i = 0; i < data.Length; i++)
        {
            crc = (crc >> 8) ^ Table[(crc ^ data[i]) & 0xFF];
        }

        return ~crc;
    }

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < table.Length; i++)
        {
            uint value = i;
            for (int bit = 0; bit < 8; bit++)
            {
                if ((value & 1) != 0)
                {
                    value = 0xEDB88320u ^ (value >> 1);
                }
                else
                {
                    value >>= 1;
                }
            }

            table[i] = value;
        }

        return table;
    }
}
