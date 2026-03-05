#pragma warning disable CS1591

// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

namespace Sharc.Core.Context;

/// <summary>
/// Minimal public implementation: 1-hop dependency impact only.
/// </summary>
public sealed class DirectDependencyAnalyzer : IImpactAnalyzer
{
    private readonly IImpactGraph _graph;

    public DirectDependencyAnalyzer(IImpactGraph graph)
    {
        _graph = graph;
    }

    public ImpactReport CalculateImpact(string targetPath, ImpactOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(targetPath))
            throw new ArgumentException("Target path is required.", nameof(targetPath));

        int maxResults = options?.MaxResults > 0 ? options.MaxResults : 100;

        var direct = _graph.TraverseDependents(targetPath, maxDepth: 1);
        var impacted = new List<ImpactedFile>(direct.Count);

        foreach (var node in direct)
        {
            var signals = _graph.GetSignals(node.Path);
            double score = 1.0 - Clamp01(signals.Coverage);

            impacted.Add(new ImpactedFile(
                node.Path,
                node.Depth,
                Clamp01(signals.Coverage),
                Clamp01(signals.RuntimeFrequency),
                Clamp01(signals.ArchitectureRisk),
                score,
                "Direct dependency (1-hop)"));
        }

        impacted.Sort(static (left, right) =>
        {
            int byScore = right.CompositeScore.CompareTo(left.CompositeScore);
            if (byScore != 0) return byScore;

            return StringComparer.OrdinalIgnoreCase.Compare(left.Path, right.Path);
        });

        if (impacted.Count > maxResults)
            impacted = impacted.Take(maxResults).ToList();

        double aggregate = impacted.Count == 0
            ? 0
            : impacted.Average(static i => i.CompositeScore);

        return new ImpactReport(
            targetPath,
            MaxDepth: 1,
            Files: impacted,
            AggregateRiskScore: aggregate,
            SafetyEnvelope: $"Direct dependency envelope for '{targetPath}' ({impacted.Count} impacted file(s)).",
            GeneratedAtUtc: DateTimeOffset.UtcNow);
    }

    private static double Clamp01(double value)
    {
        if (value < 0) return 0;
        if (value > 1) return 1;
        return value;
    }
}

#pragma warning restore CS1591

