#pragma warning disable CS1591

namespace Sharc.Base.Context;

public interface IContextNode
{
    string Path { get; }
    int LineStart { get; }
    int LineEnd { get; }
    string? Symbol { get; }
    string? Excerpt { get; }
    double Confidence { get; }
    int TokenEstimate { get; }
    bool Stale { get; }
}

public interface IContextScope
{
    IReadOnlyList<IContextNode> Candidates { get; }
    int TokenBudget { get; }
    int MaxResults { get; }
    string? ActivePath { get; }
    IReadOnlySet<string>? PinnedPaths { get; }
    bool IncludeZeroScore { get; }
}

public interface IImpactOptions
{
    int MaxDepth { get; }
    int MaxResults { get; }
    double CoverageWeight { get; }
    double RuntimeWeight { get; }
    double ArchitectureWeight { get; }
}

public interface IImpactSignals
{
    double Coverage { get; }
    double RuntimeFrequency { get; }
    double ArchitectureRisk { get; }
}

public interface IImpactTraversalNode
{
    string Path { get; }
    int Depth { get; }
}

public interface IImpactedFile
{
    string Path { get; }
    int Depth { get; }
    double Coverage { get; }
    double RuntimeFrequency { get; }
    double ArchitectureRisk { get; }
    double CompositeScore { get; }
    string Reason { get; }
}

public interface IImpactReport
{
    string TargetPath { get; }
    int MaxDepth { get; }
    IReadOnlyList<IImpactedFile> Files { get; }
    double AggregateRiskScore { get; }
    string SafetyEnvelope { get; }
    DateTimeOffset GeneratedAtUtc { get; }
}

public interface IContextRanker
{
    IEnumerable<IContextNode> Rank(string query, IContextScope scope);
}

public interface IImpactGraph
{
    IReadOnlyList<IImpactTraversalNode> TraverseDependents(string targetPath, int maxDepth);
    IImpactSignals GetSignals(string path);
}

public interface IImpactAnalyzer
{
    IImpactReport CalculateImpact(string targetPath, IImpactOptions? options = null);
}

#pragma warning restore CS1591
