// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.
#pragma warning disable CS1591

namespace Sharc.Core.Codec;

/// <summary>
/// Bit-level input stream. Reads variable-width integers from a byte buffer.
/// Zero-allocation: operates directly on a ReadOnlySpan or byte[] without wrapping objects.
/// Counterpart to <see cref="BitWriter"/>.
/// </summary>
public ref struct BitReader
{
    private readonly ReadOnlySpan<byte> _buffer;
    private int _bytePos;
    private int _bitPos;

    /// <summary>Total bits read so far.</summary>
    public long BitsRead { get; private set; }

    /// <summary>True if all bits have been consumed.</summary>
    public bool IsExhausted => _bytePos >= _buffer.Length;

    public BitReader(ReadOnlySpan<byte> buffer)
    {
        _buffer = buffer;
    }

    /// <summary>
    /// Reads <paramref name="bitCount"/> bits and returns them as a uint (LSB first).
    /// </summary>
    public uint Read(int bitCount)
    {
        if (bitCount <= 0 || bitCount > 32) return 0;

        uint result = 0;
        int resultPos = 0;
        BitsRead += bitCount;

        while (bitCount > 0 && _bytePos < _buffer.Length)
        {
            var available = 8 - _bitPos;
            var toRead = Math.Min(available, bitCount);

            var bits = (uint)((_buffer[_bytePos] >> _bitPos) & ((1 << toRead) - 1));
            result |= bits << resultPos;

            _bitPos += toRead;
            resultPos += toRead;
            bitCount -= toRead;

            if (_bitPos == 8)
            {
                _bytePos++;
                _bitPos = 0;
            }
        }

        return result;
    }

    /// <summary>
    /// Reads <paramref name="bitCount"/> bits as a ulong.
    /// </summary>
    public ulong ReadLong(int bitCount)
    {
        if (bitCount <= 32)
            return Read(bitCount);

        ulong low = Read(32);
        ulong high = Read(bitCount - 32);
        return low | (high << 32);
    }

    /// <summary>
    /// Reads a variable-length encoded unsigned integer (matching BitWriter.WriteVarInt).
    /// </summary>
    public ulong ReadVarInt()
    {
        ulong result = 0;
        int shift = 0;
        uint b;

        do
        {
            b = Read(8);
            result |= (ulong)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0 && shift < 64);

        return result;
    }
}
