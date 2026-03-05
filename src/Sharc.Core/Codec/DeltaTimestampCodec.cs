// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

namespace Sharc.Core.Codec;

/// <summary>
/// Layer 3: Delta timestamp encoding. Converts ISO 8601 timestamps to
/// varint offsets from a base epoch, reducing 20-byte strings to 1-3 byte varints.
///
/// After delta encoding, timestamps become integers that feed into
/// Layer 0 (the bloodline integer codec).
/// </summary>
public static class DeltaTimestampCodec
{
    /// <summary>Default base epoch for delta encoding.</summary>
    public static readonly DateTime DefaultBase = new(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// Encodes a list of ISO 8601 timestamps as delta-seconds from the base.
    /// Returns the deltas as long[] suitable for integer column compression.
    /// </summary>
    public static long[] Encode(IReadOnlyList<string> timestamps, DateTime? baseEpoch = null)
    {
        var epochBase = baseEpoch ?? DefaultBase;
        var deltas = new long[timestamps.Count];

        for (int i = 0; i < timestamps.Count; i++)
        {
            if (DateTime.TryParse(timestamps[i], out var dt))
            {
                deltas[i] = (long)(dt.ToUniversalTime() - epochBase).TotalSeconds;
            }
            // else: 0 delta for unparseable timestamps
        }

        return deltas;
    }

    /// <summary>
    /// Decodes delta-seconds back to ISO 8601 timestamps.
    /// </summary>
    public static string[] Decode(ReadOnlySpan<long> deltas, DateTime? baseEpoch = null)
    {
        var epochBase = baseEpoch ?? DefaultBase;
        var result = new string[deltas.Length];

        for (int i = 0; i < deltas.Length; i++)
        {
            result[i] = epochBase.AddSeconds(deltas[i]).ToString("O");
        }

        return result;
    }

    /// <summary>
    /// Fully encodes timestamps: delta from base → integer column codec.
    /// Returns compressed bytes.
    /// </summary>
    public static byte[] CompressTimestamps(IReadOnlyList<string> timestamps, DateTime? baseEpoch = null)
    {
        var deltas = Encode(timestamps, baseEpoch);
        return IntegerColumnCodec.Encode(deltas);
    }

    /// <summary>
    /// Fully decodes compressed timestamps back to ISO 8601 strings.
    /// </summary>
    public static string[] DecompressTimestamps(byte[] data, DateTime? baseEpoch = null)
    {
        var deltas = IntegerColumnCodec.Decode(data);
        return Decode(deltas, baseEpoch);
    }
}
