#pragma warning disable CS1591

// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

namespace Sharc.Core.Context;

public enum ContextNodeKind
{
    File = 0,
    Symbol = 1,
    Test = 2,
    Doc = 3,
    Edge = 4,
    Decision = 5,
}

public sealed record ContextNode(
    ContextNodeKind Kind,
    string Path,
    int LineStart = 1,
    int LineEnd = 1,
    string? Symbol = null,
    string? Excerpt = null,
    double Confidence = 1.0,
    int TokenEstimate = 0,
    bool Stale = false,
    string? CommitSha = null,
    DateTimeOffset? LastModifiedUtc = null,
    IReadOnlyDictionary<string, object?>? Metadata = null);

public sealed record ContextScope(
    IReadOnlyList<ContextNode> Candidates,
    int TokenBudget = 8000,
    int MaxResults = 20,
    string? ActivePath = null,
    IReadOnlySet<string>? PinnedPaths = null,
    bool IncludeZeroScore = false);

public sealed record ImpactOptions(
    int MaxDepth = 3,
    int MaxResults = 100,
    double CoverageWeight = 0.35,
    double RuntimeWeight = 0.35,
    double ArchitectureWeight = 0.30);

public sealed record ImpactSignals(
    double Coverage,
    double RuntimeFrequency,
    double ArchitectureRisk);

public sealed record ImpactTraversalNode(
    string Path,
    int Depth);

public sealed record ImpactedFile(
    string Path,
    int Depth,
    double Coverage,
    double RuntimeFrequency,
    double ArchitectureRisk,
    double CompositeScore,
    string Reason);

public sealed record ImpactReport(
    string TargetPath,
    int MaxDepth,
    IReadOnlyList<ImpactedFile> Files,
    double AggregateRiskScore,
    string SafetyEnvelope,
    DateTimeOffset GeneratedAtUtc);

public sealed record ImpactEdge(
    string SourcePath,
    string TargetPath);

#pragma warning restore CS1591

