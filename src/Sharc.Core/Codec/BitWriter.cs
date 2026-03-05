// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.
#pragma warning disable CS1591

using System.Buffers;

namespace Sharc.Core.Codec;

/// <summary>
/// Bit-level output stream. Packs variable-width integers into a byte buffer.
/// Uses ArrayPool to avoid heap allocation on the write path.
/// Caller must call <see cref="Dispose"/> or use a using block to return the buffer.
/// </summary>
public sealed class BitWriter : IDisposable
{
    private byte[] _buffer;
    private int _pos;
    private int _currentByte;
    private int _bitsInCurrent;

    public BitWriter(int initialCapacity = 256)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
    }

    /// <summary>Number of bytes written so far (includes partial current byte).</summary>
    public int ByteCount => _pos + (_bitsInCurrent > 0 ? 1 : 0);

    /// <summary>Total bits written.</summary>
    public long BitsWritten { get; private set; }

    /// <summary>
    /// Writes the lowest <paramref name="bitCount"/> bits of <paramref name="value"/>.
    /// </summary>
    public void Write(uint value, int bitCount)
    {
        if (bitCount <= 0 || bitCount > 32) return;

        // Mask to the requested bits. Special-case 32: (1u << 32) overflows to 1 in C#.
        var masked = bitCount == 32 ? value : value & ((1u << bitCount) - 1);
        BitsWritten += bitCount;

        while (bitCount > 0)
        {
            var space = 8 - _bitsInCurrent;
            var toWrite = Math.Min(space, bitCount);

            _currentByte |= (int)((masked & ((1u << toWrite) - 1)) << _bitsInCurrent);
            _bitsInCurrent += toWrite;
            masked >>= toWrite;
            bitCount -= toWrite;

            if (_bitsInCurrent == 8)
            {
                EnsureCapacity(1);
                _buffer[_pos++] = (byte)_currentByte;
                _currentByte = 0;
                _bitsInCurrent = 0;
            }
        }
    }

    /// <summary>
    /// Writes a 64-bit value using the lowest <paramref name="bitCount"/> bits.
    /// </summary>
    public void Write(ulong value, int bitCount)
    {
        if (bitCount <= 32)
        {
            Write((uint)value, bitCount);
            return;
        }

        Write((uint)(value & 0xFFFFFFFF), 32);
        Write((uint)(value >> 32), bitCount - 32);
    }

    /// <summary>
    /// Writes a variable-length encoded unsigned integer.
    /// Uses 7-bit groups with continuation bit (like protobuf varint).
    /// </summary>
    public void WriteVarInt(ulong value)
    {
        while (value >= 0x80)
        {
            Write((uint)(value & 0x7F) | 0x80, 8);
            value >>= 7;
        }
        Write((uint)value, 8);
    }

    /// <summary>
    /// Returns a span over the written bytes. Flushes partial byte first.
    /// Valid only while this BitWriter is alive (before Dispose).
    /// </summary>
    public ReadOnlySpan<byte> WrittenSpan
    {
        get
        {
            Flush();
            return _buffer.AsSpan(0, _pos);
        }
    }

    /// <summary>
    /// Flushes any partial byte and returns a new byte array (allocates).
    /// Prefer <see cref="WrittenSpan"/> + copy to caller-owned buffer when possible.
    /// </summary>
    public byte[] ToArray()
    {
        Flush();
        var result = new byte[_pos];
        _buffer.AsSpan(0, _pos).CopyTo(result);
        return result;
    }

    /// <summary>Resets the writer to empty state without returning the buffer.</summary>
    public void Reset()
    {
        _pos = 0;
        _currentByte = 0;
        _bitsInCurrent = 0;
        BitsWritten = 0;
    }

    public void Dispose()
    {
        if (_buffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = [];
        }
    }

    private void Flush()
    {
        if (_bitsInCurrent > 0)
        {
            EnsureCapacity(1);
            _buffer[_pos++] = (byte)_currentByte;
            _currentByte = 0;
            _bitsInCurrent = 0;
        }
    }

    private void EnsureCapacity(int additional)
    {
        if (_pos + additional <= _buffer.Length) return;

        int newSize = Math.Max(_buffer.Length * 2, _pos + additional);
        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        _buffer.AsSpan(0, _pos).CopyTo(newBuffer);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = newBuffer;
    }
}
