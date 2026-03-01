// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using Sharc.Core.Context;

namespace Sharc.Context;

/// <summary>
/// Naive keyword-based context ranker for the open-source Sharc package.
/// Matches query terms against file paths and content using simple string matching.
/// For production use, replace with a proprietary ranker via DI.
/// </summary>
public sealed class KeywordContextRanker : IContextRanker
{
    private readonly SharcDatabase _db;

    /// <summary>
    /// Initializes a new keyword ranker backed by the specified database.
    /// </summary>
    public KeywordContextRanker(SharcDatabase db)
    {
        _db = db ?? throw new ArgumentNullException(nameof(db));
    }

    /// <inheritdoc />
    public IEnumerable<ContextNode> Rank(string query, ContextScope scope)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(scope);

        // Naive: tokenize query into lowercase keywords, match against file paths
        var keywords = query.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (keywords.Length == 0)
            return [];

        var results = new List<ContextNode>();
        int tokenBudget = scope.TokenBudget;

        // Query the files table for keyword matches in file paths
        using var reader = _db.CreateReader("files", "path");
        while (reader.Read())
        {
            var path = reader.GetString(0);
            if (path is null)
                continue;

            int matchCount = 0;
            foreach (var keyword in keywords)
            {
                if (path.Contains(keyword, StringComparison.OrdinalIgnoreCase))
                    matchCount++;
            }

            if (matchCount == 0)
                continue;

            double confidence = (double)matchCount / keywords.Length;
            int estimatedTokens = path.Length / 4; // rough estimate

            if (tokenBudget - estimatedTokens < 0)
                break;

            tokenBudget -= estimatedTokens;

            results.Add(new ContextNode(
                Kind: ContextNodeKind.File,
                Path: path,
                Excerpt: path,
                Confidence: confidence,
                TokenEstimate: estimatedTokens));
        }

        return results.OrderByDescending(n => n.Confidence);
    }
}
