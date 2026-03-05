// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using BenchmarkDotNet.Attributes;
using Sharc.Core.Codec;
using System;
using System.Runtime.InteropServices;
using System.Text;

namespace Sharc.Benchmarks.Micro;

/// <summary>
/// Micro-benchmarks for new compression codecs from Phase 11.
/// Measures throughput, memory allocation (must be 0 B), and compression ratio.
/// </summary>
[BenchmarkCategory("Micro", "Compression", "Codec")]
[MemoryDiagnoser]
[DisassemblyDiagnoser(maxDepth: 3)]
public class CodecBenchmarks
{
    private const int ElementCount = 65536; // 64K elements to simulate realistic bulk array
    
    // Arrays representing raw data
    private long[] _randomInts = null!;
    private long[] _sequentialInts = null!;
    private long[] _clusteredInts = null!;
    
    // Arrays for buffering during benchmarks to ensure 0 B steady-state allocations
    private byte[] _encodeBuffer = null!;
    private long[] _decodeBuffer = null!;
    
    // Pre-encoded arrays to measure decode speed
    private byte[] _encodedRandom = null!;
    private byte[] _encodedSequential = null!;
    private byte[] _encodedClustered = null!;

    private int[] _ansData = null!;
    private int[] _ansDecodeBuffer = null!;
    private byte[] _encodedAns = null!;

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(42);
        
        _randomInts = new long[ElementCount];
        _sequentialInts = new long[ElementCount];
        _clusteredInts = new long[ElementCount];
        
        long seq = 1000000;
        long clusterBase = 50000;
        
        for (int i = 0; i < ElementCount; i++)
        {
            _randomInts[i] = rng.NextInt64(0, 1000000000); // 1B range
            _sequentialInts[i] = seq++;
            
            if (i % 1000 == 0)
                clusterBase += rng.Next(10000, 50000);
            _clusteredInts[i] = clusterBase + rng.Next(-100, 100);
        }
        
        // Worst-case buffer sizes
        _encodeBuffer = new byte[ElementCount * sizeof(long) * 2];
        _decodeBuffer = new long[ElementCount];
        
        // Pre-encode for decoding tests
        _encodedRandom = IntegerColumnCodec.Encode(_randomInts);
        
        _encodedSequential = IntegerColumnCodec.Encode(_sequentialInts);
        
        _encodedClustered = IntegerColumnCodec.Encode(_clusteredInts);
        
        // Output compression ratios for information
        Console.WriteLine($"[Setup] Random      Ratio: {(double)(ElementCount * 8) / _encodedRandom.Length:F2}x");
        Console.WriteLine($"[Setup] Sequential  Ratio: {(double)(ElementCount * 8) / _encodedSequential.Length:F2}x");
        Console.WriteLine($"[Setup] Clustered   Ratio: {(double)(ElementCount * 8) / _encodedClustered.Length:F2}x");
    }

    private static int[] BuildFreqs(int[] data)
    {
        var freqs = new int[256];
        foreach (var b in data) freqs[b]++;
        return freqs;
    }

    // --- IntegerColumnCodec ---

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Codec", "Encode", "Integer")]
    public byte[] IntegerCodec_Encode_Random()
    {
        return IntegerColumnCodec.Encode(_randomInts);
    }
    
    [Benchmark]
    [BenchmarkCategory("Codec", "Encode", "Integer")]
    public byte[] IntegerCodec_Encode_Sequential()
    {
        return IntegerColumnCodec.Encode(_sequentialInts);
    }

    [Benchmark]
    [BenchmarkCategory("Codec", "Encode", "Integer")]
    public byte[] IntegerCodec_Encode_Clustered()
    {
        return IntegerColumnCodec.Encode(_clusteredInts);
    }

    [Benchmark]
    [BenchmarkCategory("Codec", "Decode", "Integer")]
    public long[] IntegerCodec_Decode_Random()
    {
        return IntegerColumnCodec.Decode(_encodedRandom);
    }

    [Benchmark]
    [BenchmarkCategory("Codec", "Decode", "Integer")]
    public long[] IntegerCodec_Decode_Sequential()
    {
        return IntegerColumnCodec.Decode(_encodedSequential);
    }

    [Benchmark]
    [BenchmarkCategory("Codec", "Decode", "Integer")]
    public long[] IntegerCodec_Decode_Clustered()
    {
        return IntegerColumnCodec.Decode(_encodedClustered);
    }

    // --- DeltaEncoder ---

    [Benchmark]
    [BenchmarkCategory("Codec", "Delta", "Encode")]
    public void DeltaEncoder_Encode()
    {
        // Delta modifies in place
        // Copy to buffer to avoid progressive distortion over benchmark iterations
        _sequentialInts.AsSpan().CopyTo(_decodeBuffer); 
        DeltaEncoder.EncodeDelta1(_sequentialInts.AsSpan(), _decodeBuffer.AsSpan());
    }

    [Benchmark]
    [BenchmarkCategory("Codec", "Delta", "Decode")]
    public void DeltaEncoder_Decode()
    {
        _sequentialInts.AsSpan().CopyTo(_decodeBuffer);
        DeltaEncoder.DecodeDelta1(_sequentialInts.AsSpan(), _decodeBuffer.AsSpan());
    }
    
    // --- CRC32 ---
    
    [Benchmark]
    [BenchmarkCategory("Codec", "Integrity", "CRC32")]
    public uint Crc32_Compute()
    {
        return Crc32.Compute(_encodedRandom);
    }
}
