// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Text.Json;
using Sharc.Repo;
using Sharc.Repo.Cli;
using Sharc.Repo.Data;
using Xunit;

namespace Sharc.Repo.Tests.Cli;

[Collection("CLI")]
public sealed class TraceSeedCommandTests : IDisposable
{
    private readonly string _tempRoot;
    private readonly string _savedCwd;

    public TraceSeedCommandTests()
    {
        _tempRoot = Path.Combine(Path.GetTempPath(), $"sharc_trace_seed_{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempRoot);
        Directory.CreateDirectory(Path.Combine(_tempRoot, ".git"));

        _savedCwd = Directory.GetCurrentDirectory();
        Directory.SetCurrentDirectory(_tempRoot);

        InitCommand.Run(Array.Empty<string>());
        SeedWorkspace(Path.Combine(_tempRoot, ".sharc", RepoLocator.WorkspaceFileName));
    }

    public void Dispose()
    {
        Directory.SetCurrentDirectory(_savedCwd);
        try { Directory.Delete(_tempRoot, recursive: true); } catch { }
        GC.SuppressFinalize(this);
    }

    [Fact]
    public void Run_DefaultPath_WritesSeedWithEdgesAndSignals()
    {
        int exitCode = TraceSeedCommand.Run(Array.Empty<string>());

        Assert.Equal(0, exitCode);

        string seedPath = Path.Combine(_tempRoot, ".sharc", "trace-seed.json");
        Assert.True(File.Exists(seedPath));

        using var doc = JsonDocument.Parse(File.ReadAllText(seedPath));
        JsonElement root = doc.RootElement;

        JsonElement edges = root.GetProperty("edges");
        Assert.True(edges.GetArrayLength() >= 2);

        JsonElement signals = root.GetProperty("signals");
        Assert.True(signals.TryGetProperty("src/Auth/TokenService.cs", out JsonElement tokenSignals));

        double runtimeFrequency = tokenSignals.GetProperty("runtimeFrequency").GetDouble();
        Assert.True(runtimeFrequency > 0.5);
    }

    [Fact]
    public void Run_CustomOutput_RespectsEdgeThreshold()
    {
        string baselinePath = Path.Combine(_tempRoot, "baseline-seed.json");
        int baselineExitCode = TraceSeedCommand.Run(
        [
            "--out", baselinePath,
        ]);
        Assert.Equal(0, baselineExitCode);

        int baselineEdgeCount;
        using (var baselineDoc = JsonDocument.Parse(File.ReadAllText(baselinePath)))
        {
            baselineEdgeCount = baselineDoc.RootElement.GetProperty("edges").GetArrayLength();
        }

        string outPath = Path.Combine(_tempRoot, "custom-seed.json");
        int exitCode = TraceSeedCommand.Run(
        [
            "--out", outPath,
            "--min-cochange", "2",
        ]);

        Assert.Equal(0, exitCode);
        Assert.True(File.Exists(outPath));

        using var doc = JsonDocument.Parse(File.ReadAllText(outPath));
        JsonElement edges = doc.RootElement.GetProperty("edges");

        Assert.True(edges.GetArrayLength() <= baselineEdgeCount);
        Assert.DoesNotContain(edges.EnumerateArray(), static e =>
            string.Equals(e.GetProperty("sourcePath").GetString(), "src/SingleTouch.cs", StringComparison.Ordinal));
    }

    private static void SeedWorkspace(string workspacePath)
    {
        using var db = SharcDatabase.Open(workspacePath, new SharcOpenOptions { Writable = true });
        using var writer = new WorkspaceWriter(db);

        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        long c1 = writer.WriteCommit(new GitCommitRecord("c1", "dev", "dev@example.com", now - 300, "Auth baseline"));
        writer.WriteFileChange(new GitFileChangeRecord(c1, "src/Auth/AuthHandler.cs", 12, 1));
        writer.WriteFileChange(new GitFileChangeRecord(c1, "src/Auth/TokenService.cs", 8, 2));

        long c2 = writer.WriteCommit(new GitCommitRecord("c2", "dev", "dev@example.com", now - 200, "Auth hardening"));
        writer.WriteFileChange(new GitFileChangeRecord(c2, "src/Auth/TokenService.cs", 5, 1));
        writer.WriteFileChange(new GitFileChangeRecord(c2, "tests/Auth/TokenServiceTests.cs", 15, 0));

        long c3 = writer.WriteCommit(new GitCommitRecord("c3", "dev", "dev@example.com", now - 100, "Controller sync"));
        writer.WriteFileChange(new GitFileChangeRecord(c3, "src/Auth/AuthHandler.cs", 3, 0));
        writer.WriteFileChange(new GitFileChangeRecord(c3, "src/Auth/TokenService.cs", 6, 1));

        long c4 = writer.WriteCommit(new GitCommitRecord("c4", "dev", "dev@example.com", now - 50, "Isolated touch"));
        writer.WriteFileChange(new GitFileChangeRecord(c4, "src/SingleTouch.cs", 2, 0));
    }
}
