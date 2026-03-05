// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

namespace Sharc.Core.Context;

/// <summary>
/// Calculates the downstream impact (blast radius) of modifying a file or symbol.
/// The default open-source implementation performs a single-hop dependency lookup.
/// Proprietary implementations may use multi-hop traversal weighted by test coverage
/// and runtime execution metrics.
/// </summary>
public interface IImpactAnalyzer
{
    /// <summary>
    /// Analyzes the downstream impact of modifying the target path or symbol.
    /// </summary>
    /// <param name="targetPath">File path or symbol identifier to analyze.</param>
    /// <param name="options">Analysis options (max depth, revision filter).</param>
    /// <returns>Impact report with risk scoring and test recommendations.</returns>
    ImpactReport CalculateImpact(string targetPath, ImpactOptions? options = null);
}
