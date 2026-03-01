// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using Sharc.Repo.Trace;

namespace Sharc.Repo.Cli;

/// <summary>
/// Exports a co-change impact graph seed for Sharc.Trace.
/// </summary>
public static class TraceSeedCommand
{
    public static int Run(string[] args)
    {
        string? sharcDir = RepoLocator.FindSharcDir();

        string workspacePath = sharcDir != null
            ? Path.Combine(sharcDir, RepoLocator.WorkspaceFileName)
            : Path.Combine(Directory.GetCurrentDirectory(), RepoLocator.WorkspaceFileName);

        string outputPath = sharcDir != null
            ? Path.Combine(sharcDir, "trace-seed.json")
            : Path.Combine(Directory.GetCurrentDirectory(), "trace-seed.json");

        int minCoChange = 1;
        int maxEdgesPerTarget = 64;

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--workspace" when i + 1 < args.Length:
                    workspacePath = args[++i];
                    break;
                case "--out" when i + 1 < args.Length:
                    outputPath = args[++i];
                    break;
                case "--min-cochange" when i + 1 < args.Length && int.TryParse(args[++i], out int parsedMin):
                    minCoChange = parsedMin;
                    break;
                case "--max-edges-per-target" when i + 1 < args.Length && int.TryParse(args[++i], out int parsedMax):
                    maxEdgesPerTarget = parsedMax;
                    break;
                case "--help":
                    PrintHelp();
                    return 0;
                default:
                    Console.Error.WriteLine($"Unknown option: {args[i]}");
                    PrintHelp();
                    return 1;
            }
        }

        try
        {
            var builder = new TraceSeedBuilder();
            TraceSeedDocument seed = builder.Build(workspacePath, minCoChange, maxEdgesPerTarget);

            string fullOutputPath = Path.GetFullPath(outputPath);
            TraceSeedBuilder.Write(fullOutputPath, seed);

            Console.WriteLine($"Wrote trace seed: {fullOutputPath}");
            Console.WriteLine($"Edges: {seed.Edges.Count}, Signals: {seed.Signals.Count}");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Failed to build trace seed: {ex.Message}");
            return 1;
        }
    }

    private static void PrintHelp()
    {
        Console.WriteLine("Usage: sharc trace-seed [options]");
        Console.WriteLine();
        Console.WriteLine("Builds .sharc/trace-seed.json for Sharc.Trace blast-radius tooling.");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --workspace <path>             Path to workspace.arc");
        Console.WriteLine("  --out <path>                   Output JSON path (default: .sharc/trace-seed.json)");
        Console.WriteLine("  --min-cochange <n>             Minimum co-change count to keep an edge (default: 1)");
        Console.WriteLine("  --max-edges-per-target <n>     Cap outgoing edges per target path (default: 64)");
    }
}
