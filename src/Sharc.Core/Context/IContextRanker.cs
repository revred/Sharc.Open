// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

namespace Sharc.Core.Context;

/// <summary>
/// Ranks context nodes by relevance to a natural-language query.
/// The default open-source implementation uses keyword matching.
/// Proprietary implementations may use multi-hop graph traversal,
/// runtime metrics, and LLM-specific token budgeting.
/// </summary>
public interface IContextRanker
{
    /// <summary>
    /// Returns context nodes ranked by relevance to the query,
    /// respecting the specified token budget.
    /// </summary>
    /// <param name="query">Natural-language query describing the task.</param>
    /// <param name="scope">Scoping parameters (budget, mode, path filter).</param>
    /// <returns>Ranked context nodes with confidence and provenance.</returns>
    IEnumerable<ContextNode> Rank(string query, ContextScope scope);
}
