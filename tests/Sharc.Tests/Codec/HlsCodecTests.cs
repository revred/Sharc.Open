// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Security.Cryptography;
using Sharc.Core.Codec;
using Xunit;

namespace Sharc.Tests.Codec;

public class HlsCodecTests
{
    [Fact]
    public void Split_And_Reassemble_ByteIdentical()
    {
        var rng = new Random(42);
        var original = new byte[50_000]; // 50KB test file
        rng.NextBytes(original);

        var tmpDir = Path.Combine(Path.GetTempPath(), $"hls_test_{Guid.NewGuid():N}");
        var inputPath = Path.Combine(tmpDir, "test.dat");
        var outputPath = Path.Combine(tmpDir, "reassembled.dat");

        try
        {
            Directory.CreateDirectory(tmpDir);
            File.WriteAllBytes(inputPath, original);

            // Split into 20KB chunks → 3 chunks expected
            var manifestPath = HlsWriter.Split(inputPath, tmpDir, chunkMaxBytes: 20_000);

            Assert.True(File.Exists(manifestPath));
            Assert.True(File.Exists(Path.Combine(tmpDir, "test.dat.001.hls")));
            Assert.True(File.Exists(Path.Combine(tmpDir, "test.dat.002.hls")));
            Assert.True(File.Exists(Path.Combine(tmpDir, "test.dat.003.hls")));

            // Reassemble
            HlsReader.Reassemble(manifestPath, outputPath);

            var reassembled = File.ReadAllBytes(outputPath);
            Assert.Equal(original.Length, reassembled.Length);
            Assert.Equal(original, reassembled);
        }
        finally
        {
            if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
        }
    }

    [Fact]
    public void Manifest_ParsesCorrectly()
    {
        var rng = new Random(99);
        var original = new byte[100_000];
        rng.NextBytes(original);

        var tmpDir = Path.Combine(Path.GetTempPath(), $"hls_manifest_{Guid.NewGuid():N}");
        var inputPath = Path.Combine(tmpDir, "payload.rak");

        try
        {
            Directory.CreateDirectory(tmpDir);
            File.WriteAllBytes(inputPath, original);

            var manifestPath = HlsWriter.Split(inputPath, tmpDir, chunkMaxBytes: 40_000);
            var manifest = HlsReader.ReadManifest(manifestPath);

            Assert.Equal(HlsManifest.CurrentVersion, manifest.Version);
            Assert.Equal(100_000, manifest.OriginalSize);
            Assert.Equal(40_000, manifest.ChunkMaxSize);
            Assert.Equal(3, manifest.ChunkCount);
            Assert.Equal("payload.rak", manifest.OriginalFileName);
            Assert.Equal(32, manifest.OriginalSha256.Length);
            Assert.Equal(3, manifest.ChunkSha256.Length);

            // Verify original hash matches
            byte[] expectedHash = SHA256.HashData(original);
            Assert.Equal(expectedHash, manifest.OriginalSha256);
        }
        finally
        {
            if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
        }
    }

    [Fact]
    public void CorruptedChunk_ThrowsOnReassembly()
    {
        var original = new byte[30_000];
        new Random(7).NextBytes(original);

        var tmpDir = Path.Combine(Path.GetTempPath(), $"hls_corrupt_{Guid.NewGuid():N}");
        var inputPath = Path.Combine(tmpDir, "data.bin");
        var outputPath = Path.Combine(tmpDir, "output.bin");

        try
        {
            Directory.CreateDirectory(tmpDir);
            File.WriteAllBytes(inputPath, original);

            var manifestPath = HlsWriter.Split(inputPath, tmpDir, chunkMaxBytes: 15_000);

            // Corrupt chunk 1 data (byte after header)
            var chunk1Path = Path.Combine(tmpDir, "data.bin.001.hls");
            var chunk1 = File.ReadAllBytes(chunk1Path);
            chunk1[HlsManifest.ChunkHeaderSize + 5] ^= 0xFF;
            File.WriteAllBytes(chunk1Path, chunk1);

            Assert.Throws<InvalidDataException>(() =>
                HlsReader.Reassemble(manifestPath, outputPath));
        }
        finally
        {
            if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
        }
    }

    [Fact]
    public void SingleChunk_WhenFileUnderLimit()
    {
        var original = new byte[5_000];
        new Random(1).NextBytes(original);

        var tmpDir = Path.Combine(Path.GetTempPath(), $"hls_single_{Guid.NewGuid():N}");
        var inputPath = Path.Combine(tmpDir, "small.db");
        var outputPath = Path.Combine(tmpDir, "output.db");

        try
        {
            Directory.CreateDirectory(tmpDir);
            File.WriteAllBytes(inputPath, original);

            var manifestPath = HlsWriter.Split(inputPath, tmpDir, chunkMaxBytes: 20_000);
            var manifest = HlsReader.ReadManifest(manifestPath);

            Assert.Equal(1, manifest.ChunkCount);
            Assert.True(File.Exists(Path.Combine(tmpDir, "small.db.001.hls")));

            HlsReader.Reassemble(manifestPath, outputPath);
            Assert.Equal(original, File.ReadAllBytes(outputPath));
        }
        finally
        {
            if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
        }
    }
}
