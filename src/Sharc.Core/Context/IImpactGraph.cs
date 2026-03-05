#pragma warning disable CS1591

// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

namespace Sharc.Core.Context;

/// <summary>
/// Supplies dependency traversal and optional risk signals for impact analysis.
/// </summary>
public interface IImpactGraph
{
    IReadOnlyList<ImpactTraversalNode> TraverseDependents(string targetPath, int maxDepth);

    ImpactSignals GetSignals(string path);
}

#pragma warning restore CS1591

