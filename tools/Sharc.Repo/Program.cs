// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Sharc.Repo.Cli;

namespace Sharc.Repo;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length == 0 || args[0] is "--help" or "-h")
        {
            PrintUsage();
            return 0;
        }

        try
        {
            return args[0] switch
            {
                "init" => InitCommand.Run(args[1..]),
                "update" => UpdateCommand.Run(args[1..]),
                "status" => StatusCommand.Run(args[1..]),
                "config" => ConfigCommand.Run(args[1..]),
                "note" => NoteCommand.Run(args[1..]),
                "annotate" => AnnotateCommand.Run(args[1..]),
                "decide" => DecideCommand.Run(args[1..]),
                "set" => SetCommand.Run(args[1..]),
                "get" => GetCommand.Run(args[1..]),
                "query" => QueryCommand.Run(args[1..]),
                "trace-seed" => TraceSeedCommand.Run(args[1..]),
                "scan" => ScanCommand.Run(args[1..]),
                "feature" => FeatureCommand.Run(args[1..]),
                "gaps" => GapsCommand.Run(args[1..]),
                "serve" => await RunServe(args[1..]),
                _ => Error($"Unknown command: {args[0]}")
            };
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
            return 1;
        }
    }

    private static async Task<int> RunServe(string[] args)
    {
        var sharcDir = RepoLocator.FindSharcDir();
        if (sharcDir == null)
        {
            Console.Error.WriteLine("Not initialized. Run 'sharc init' first.");
            return 1;
        }

        Environment.SetEnvironmentVariable("SHARC_WORKSPACE",
            Path.Combine(sharcDir, RepoLocator.WorkspaceFileName));
        Environment.SetEnvironmentVariable("SHARC_CONFIG",
            Path.Combine(sharcDir, RepoLocator.ConfigFileName));

        var builder = Host.CreateApplicationBuilder(args);
        builder.Logging.AddConsole(options =>
        {
            options.LogToStandardErrorThreshold = LogLevel.Trace;
        });

        builder.Services
            .AddMcpServer(options =>
            {
                options.ServerInfo = new()
                {
                    Name = "sharc-repo",
                    Version = "1.0.0"
                };
            })
            .WithStdioServerTransport()
            .WithToolsFromAssembly();

        await builder.Build().RunAsync();
        return 0;
    }

    private static void PrintUsage()
    {
        Console.WriteLine("sharc: Repo-local context store with trust layer");
        Console.WriteLine();
        Console.WriteLine("Usage: sharc <command> [options]");
        Console.WriteLine();
        Console.WriteLine("Commands:");
        Console.WriteLine("  init       Initialize .sharc/ at the git repo root");
        Console.WriteLine("  update     Index git history into workspace");
        Console.WriteLine("  status     Show workspace status");
        Console.WriteLine("  config     Read/write configuration");
        Console.WriteLine("  note       Add a free-form note");
        Console.WriteLine("  annotate   Add a file annotation");
        Console.WriteLine("  decide     Record an architectural decision");
        Console.WriteLine("  set        Set a context key-value");
        Console.WriteLine("  get        Read context key-value entries");
        Console.WriteLine("  query      Query workspace tables");
        Console.WriteLine("  trace-seed Build .sharc/trace-seed.json for Sharc.Trace");
        Console.WriteLine("  scan       Scan codebase into knowledge graph");
        Console.WriteLine("  feature    Feature management (list, show, add, link)");
        Console.WriteLine("  gaps       Analyze knowledge graph for coverage gaps");
        Console.WriteLine("  serve      Launch MCP stdio server");
        Console.WriteLine("  --help     Show this help message");
    }

    private static int Error(string message)
    {
        Console.Error.WriteLine(message);
        PrintUsage();
        return 1;
    }
}
