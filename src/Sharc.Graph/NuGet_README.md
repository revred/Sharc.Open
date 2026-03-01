# Sharc.Graph

**Graph reasoning and trust layer for the Sharc database engine.**

High-performance graph storage and cryptographic trust for AI context space engineering, overlaid on standard SQLite files.

## Features

- **31x Faster Traversal**: 2-hop BFS in 2.6 us vs SQLite's 82 us via zero-alloc cursor BFS.
- **O(log N) Seeks**: B-tree backed node and edge stores with SeekFirst cursors.
- **Trust Ledger**: Hash-chained, ECDSA-signed audit trails for data provenance.
- **Agent Identity**: Cryptographic registry for attributing every mutation to a specific agent.
- **Token Efficiency**: Precise, context-rich subgraphs for LLMs — reduce token waste by 62-133x.

## Quick Start

```csharp
using Sharc;
using Sharc.Graph;

using var db = SharcDatabase.Open("context.db");
var graph = SharcContextGraph.Create(db);

// Traverse relationships
var result = graph.Traverse(nodeKey, new TraversalPolicy
{
    MaxDepth = 2,
    Direction = TraversalDirection.Both
});

foreach (var node in result.Nodes)
    Console.WriteLine($"Found: {node.Record.Key}");

// Verify ledger integrity
bool trusted = graph.Ledger.VerifyIntegrity();
```

[Full Documentation](https://github.com/revred/Sharc) | [Live Arena](https://revred.github.io/Sharc.Open/)
