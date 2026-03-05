#pragma warning disable CS1591

// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

namespace Sharc.Core.Context;

/// <summary>
/// Minimal public implementation: keyword scoring over candidate nodes.
/// </summary>
public sealed class KeywordContextRanker : IContextRanker
{
    private static readonly char[] TokenSeparators =
    [
        ' ', '\t', '\r', '\n', '.', ',', ';', ':', '(', ')', '[', ']', '{', '}', '/', '\\', '-', '_', '"', '\''
    ];

    public IEnumerable<ContextNode> Rank(string query, ContextScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);

        if (scope.Candidates.Count == 0)
            return [];

        int maxResults = scope.MaxResults > 0 ? scope.MaxResults : 20;
        int budget = scope.TokenBudget > 0 ? scope.TokenBudget : int.MaxValue;

        string[] tokens = Tokenize(query);
        if (tokens.Length == 0)
            return scope.Candidates.Take(Math.Min(scope.Candidates.Count, maxResults));

        var scored = new List<(ContextNode Node, double Score)>(scope.Candidates.Count);
        foreach (var candidate in scope.Candidates)
        {
            double score = Score(candidate, tokens, scope.ActivePath, scope.PinnedPaths);
            if (score <= 0 && !scope.IncludeZeroScore)
                continue;

            int estimate = candidate.TokenEstimate > 0
                ? candidate.TokenEstimate
                : EstimateTokens(candidate.Excerpt ?? candidate.Path);

            double confidence = Math.Min(1.0, score / (tokens.Length * 3.5));
            var node = candidate with
            {
                TokenEstimate = estimate,
                Confidence = confidence,
            };

            scored.Add((node, score));
        }

        scored.Sort(static (left, right) =>
        {
            int byScore = right.Score.CompareTo(left.Score);
            if (byScore != 0) return byScore;

            int byPath = StringComparer.OrdinalIgnoreCase.Compare(left.Node.Path, right.Node.Path);
            if (byPath != 0) return byPath;

            return left.Node.LineStart.CompareTo(right.Node.LineStart);
        });

        var results = new List<ContextNode>(Math.Min(maxResults, scored.Count));
        int used = 0;
        foreach (var item in scored)
        {
            if (results.Count >= maxResults)
                break;

            if (results.Count > 0 && used + item.Node.TokenEstimate > budget)
                break;

            results.Add(item.Node);
            used += item.Node.TokenEstimate;
        }

        if (results.Count == 0 && scored.Count > 0)
            results.Add(scored[0].Node);

        return results;
    }

    private static string[] Tokenize(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            return [];

        return query
            .Split(TokenSeparators, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static int EstimateTokens(string value)
    {
        if (string.IsNullOrEmpty(value))
            return 1;

        return Math.Max(1, (value.Length + 3) / 4);
    }

    private static double Score(
        ContextNode candidate,
        IReadOnlyList<string> tokens,
        string? activePath,
        IReadOnlySet<string>? pinnedPaths)
    {
        double score = 0;

        foreach (var token in tokens)
        {
            if (candidate.Path.Contains(token, StringComparison.OrdinalIgnoreCase))
                score += 2.0;

            if (!string.IsNullOrWhiteSpace(candidate.Symbol) &&
                candidate.Symbol.Contains(token, StringComparison.OrdinalIgnoreCase))
                score += 1.25;

            if (!string.IsNullOrWhiteSpace(candidate.Excerpt) &&
                candidate.Excerpt.Contains(token, StringComparison.OrdinalIgnoreCase))
                score += 0.75;
        }

        if (!string.IsNullOrWhiteSpace(activePath) &&
            candidate.Path.Equals(activePath, StringComparison.OrdinalIgnoreCase))
            score += 2.0;

        if (pinnedPaths != null && pinnedPaths.Contains(candidate.Path))
            score += 1.0;

        if (candidate.Kind == ContextNodeKind.Test)
            score += 0.25;

        return score;
    }
}

#pragma warning restore CS1591

