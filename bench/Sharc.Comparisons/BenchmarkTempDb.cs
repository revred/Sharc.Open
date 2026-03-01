// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Threading;

namespace Sharc.Comparisons;

/// <summary>
/// Provides lock-resistant temp database paths and file helpers for benchmark runs.
/// </summary>
internal static class BenchmarkTempDb
{
    private static readonly string RootDirectory =
        Path.Combine(Path.GetTempPath(), "sharc_comparisons_temp");

    internal static string CreatePath(string namePrefix)
    {
        ArgumentException.ThrowIfNullOrEmpty(namePrefix);
        Directory.CreateDirectory(RootDirectory);
        return Path.Combine(
            RootDirectory,
            $"{namePrefix}_{Environment.ProcessId}_{Guid.NewGuid():N}.db");
    }

    internal static byte[] ReadAllBytesWithRetry(string path, int maxAttempts = 6)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        if (maxAttempts < 1) maxAttempts = 1;

        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return File.ReadAllBytes(path);
            }
            catch (IOException) when (attempt < maxAttempts)
            {
                Thread.Sleep(20 * attempt);
            }
        }
    }

    internal static void TryDelete(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return;

        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (IOException)
        {
            // Best effort cleanup.
        }
        catch (UnauthorizedAccessException)
        {
            // Best effort cleanup.
        }
    }
}
