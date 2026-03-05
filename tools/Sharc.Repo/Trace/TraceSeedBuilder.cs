// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Text.Json;
using Sharc.Core.Context;

namespace Sharc.Repo.Trace;

/// <summary>
/// Seed payload consumed by Sharc.Trace.
/// </summary>
public sealed record TraceSeedDocument(
    IReadOnlyList<ImpactEdge> Edges,
    IReadOnlyDictionary<string, ImpactSignals> Signals);

/// <summary>
/// Builds a bootstrap impact graph from workspace commit/file-change history.
/// </summary>
public sealed class TraceSeedBuilder
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public TraceSeedDocument Build(
        string workspacePath,
        int minCoChange = 1,
        int maxEdgesPerTarget = 64)
    {
        if (string.IsNullOrWhiteSpace(workspacePath))
            throw new ArgumentException("Workspace path is required.", nameof(workspacePath));

        if (!File.Exists(workspacePath))
            throw new FileNotFoundException("Workspace database not found.", workspacePath);

        if (minCoChange < 1)
            minCoChange = 1;
        if (maxEdgesPerTarget < 1)
            maxEdgesPerTarget = 1;

        using var db = SharcDatabase.Open(workspacePath, new SharcOpenOptions { Writable = false });

        var commits = LoadCommitChanges(db);
        if (commits.Count == 0)
            return new TraceSeedDocument([], new Dictionary<string, ImpactSignals>(StringComparer.OrdinalIgnoreCase));

        var pathStats = BuildPathStats(commits);
        var edges = BuildEdges(commits, minCoChange, maxEdgesPerTarget);
        var signals = BuildSignals(pathStats);

        return new TraceSeedDocument(edges, signals);
    }

    public static void Write(string outputPath, TraceSeedDocument document)
    {
        ArgumentNullException.ThrowIfNull(document);

        if (string.IsNullOrWhiteSpace(outputPath))
            throw new ArgumentException("Output path is required.", nameof(outputPath));

        string? parent = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(parent))
            Directory.CreateDirectory(parent);

        string json = JsonSerializer.Serialize(document, JsonOptions);
        File.WriteAllText(outputPath, json);
    }

    private static Dictionary<long, HashSet<string>> LoadCommitChanges(SharcDatabase db)
    {
        var commits = new Dictionary<long, HashSet<string>>();
        var rows = new List<RawFileChangeRow>();

        using var reader = db.CreateReader("file_changes");
        int fieldCount = reader.FieldCount;

        while (reader.Read())
        {
            var ints = new long?[fieldCount];
            var strings = new string?[fieldCount];

            for (int i = 0; i < fieldCount; i++)
            {
                if (reader.IsNull(i))
                    continue;

                try { ints[i] = reader.GetInt64(i); } catch { }
                try { strings[i] = reader.GetString(i); } catch { }
            }

            rows.Add(new RawFileChangeRow(ints, strings));
        }

        if (rows.Count == 0)
            return commits;

        int pathColumn = DetectPathColumn(rows, fieldCount);
        int commitColumn = DetectCommitColumn(rows, fieldCount, pathColumn);

        long fallbackCommit = 1;
        foreach (RawFileChangeRow row in rows)
        {
            string path = NormalizePath(row.Strings[pathColumn] ?? string.Empty);
            if (string.IsNullOrWhiteSpace(path))
                continue;

            long commitId = row.Ints[commitColumn] ?? fallbackCommit++;

            if (!commits.TryGetValue(commitId, out var set))
            {
                set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                commits[commitId] = set;
            }

            set.Add(path);
        }

        return commits;
    }

    private static int DetectPathColumn(IReadOnlyList<RawFileChangeRow> rows, int fieldCount)
    {
        int bestColumn = 0;
        int bestScore = -1;

        for (int column = 0; column < fieldCount; column++)
        {
            int score = 0;
            foreach (RawFileChangeRow row in rows)
            {
                string? value = row.Strings[column];
                if (string.IsNullOrWhiteSpace(value))
                    continue;

                string path = value.Replace('\\', '/');
                if (path.Contains('/') ||
                    path.EndsWith(".cs", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".ts", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".md", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".json", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".yml", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase))
                {
                    score++;
                }
            }

            if (score > bestScore)
            {
                bestScore = score;
                bestColumn = column;
            }
        }

        return bestColumn;
    }

    private static int DetectCommitColumn(IReadOnlyList<RawFileChangeRow> rows, int fieldCount, int pathColumn)
    {
        int bestColumn = 0;
        int bestRepeatCount = -1;
        int bestValueCount = -1;

        for (int column = 0; column < fieldCount; column++)
        {
            if (column == pathColumn)
                continue;

            var values = new List<long>(rows.Count);
            foreach (RawFileChangeRow row in rows)
            {
                if (row.Ints[column].HasValue)
                    values.Add(row.Ints[column]!.Value);
            }

            if (values.Count == 0)
                continue;

            int unique = values.Distinct().Count();
            int repeats = values.Count - unique;

            if (repeats > bestRepeatCount ||
                (repeats == bestRepeatCount && values.Count > bestValueCount))
            {
                bestRepeatCount = repeats;
                bestValueCount = values.Count;
                bestColumn = column;
            }
        }

        return bestColumn;
    }

    private static Dictionary<string, PathStats> BuildPathStats(IReadOnlyDictionary<long, HashSet<string>> commits)
    {
        var stats = new Dictionary<string, PathStats>(StringComparer.OrdinalIgnoreCase);

        foreach (var files in commits.Values)
        {
            var tests = files.Where(IsTestPath).ToList();
            bool hasTests = tests.Count > 0;

            foreach (string path in files)
            {
                if (!stats.TryGetValue(path, out var current))
                    current = new PathStats();

                current.TouchCount++;

                if (hasTests && !IsTestPath(path))
                    current.CoChangedWithTests += tests.Count;

                stats[path] = current;
            }
        }

        return stats;
    }

    private static IReadOnlyList<ImpactEdge> BuildEdges(
        IReadOnlyDictionary<long, HashSet<string>> commits,
        int minCoChange,
        int maxEdgesPerTarget)
    {
        var byTarget = new Dictionary<string, Dictionary<string, int>>(StringComparer.OrdinalIgnoreCase);

        foreach (var files in commits.Values)
        {
            if (files.Count < 2)
                continue;

            // Stable ordering ensures deterministic output.
            var ordered = files.OrderBy(static p => p, StringComparer.OrdinalIgnoreCase).ToArray();

            foreach (string target in ordered)
            {
                if (!byTarget.TryGetValue(target, out var sourceWeights))
                {
                    sourceWeights = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                    byTarget[target] = sourceWeights;
                }

                foreach (string source in ordered)
                {
                    if (source.Equals(target, StringComparison.OrdinalIgnoreCase))
                        continue;

                    sourceWeights.TryGetValue(source, out int existing);
                    sourceWeights[source] = existing + 1;
                }
            }
        }

        var edges = new List<ImpactEdge>();

        foreach (var target in byTarget.Keys.OrderBy(static p => p, StringComparer.OrdinalIgnoreCase))
        {
            var sourceWeights = byTarget[target];
            foreach (var pair in sourceWeights
                .Where(p => p.Value >= minCoChange)
                .OrderByDescending(static p => p.Value)
                .ThenBy(static p => p.Key, StringComparer.OrdinalIgnoreCase)
                .Take(maxEdgesPerTarget))
            {
                edges.Add(new ImpactEdge(pair.Key, target));
            }
        }

        return edges;
    }

    private static IReadOnlyDictionary<string, ImpactSignals> BuildSignals(
        IReadOnlyDictionary<string, PathStats> pathStats)
    {
        int maxTouches = pathStats.Count == 0 ? 1 : pathStats.Max(static p => p.Value.TouchCount);
        var signals = new Dictionary<string, ImpactSignals>(StringComparer.OrdinalIgnoreCase);

        foreach (var path in pathStats.Keys.OrderBy(static p => p, StringComparer.OrdinalIgnoreCase))
        {
            PathStats stats = pathStats[path];

            double runtimeFrequency = Clamp01((double)stats.TouchCount / maxTouches);

            double coverage = IsTestPath(path)
                ? 0.95
                : Clamp01((double)stats.CoChangedWithTests / Math.Max(1, stats.TouchCount * 3));

            if (coverage == 0)
                coverage = 0.25;

            double architectureRisk = EstimateArchitectureRisk(path, coverage, runtimeFrequency);

            signals[path] = new ImpactSignals(
                Coverage: coverage,
                RuntimeFrequency: runtimeFrequency,
                ArchitectureRisk: architectureRisk);
        }

        return signals;
    }

    private static double EstimateArchitectureRisk(string path, double coverage, double runtimeFrequency)
    {
        if (IsTestPath(path))
            return 0.10;

        double risk = 0.30;
        if (path.Contains("/src/", StringComparison.OrdinalIgnoreCase))
            risk += 0.15;
        if (path.Contains("/core/", StringComparison.OrdinalIgnoreCase) ||
            path.EndsWith("/program.cs", StringComparison.OrdinalIgnoreCase) ||
            path.EndsWith("/startup.cs", StringComparison.OrdinalIgnoreCase) ||
            path.Contains("/schema/", StringComparison.OrdinalIgnoreCase))
            risk += 0.20;

        if (runtimeFrequency >= 0.75)
            risk += 0.10;

        risk -= coverage * 0.20;
        return Clamp01(risk);
    }

    private static bool IsTestPath(string path)
    {
        return path.Contains("/tests/", StringComparison.OrdinalIgnoreCase) ||
               path.EndsWith("Tests.cs", StringComparison.OrdinalIgnoreCase) ||
               path.EndsWith(".Tests.cs", StringComparison.OrdinalIgnoreCase);
    }

    private static string NormalizePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return string.Empty;

        return path.Replace('\\', '/').Trim();
    }

    private static double Clamp01(double value)
    {
        if (value < 0) return 0;
        if (value > 1) return 1;
        return value;
    }

    private sealed class PathStats
    {
        public int TouchCount { get; set; }

        public int CoChangedWithTests { get; set; }
    }

    private sealed record RawFileChangeRow(long?[] Ints, string?[] Strings);
}
