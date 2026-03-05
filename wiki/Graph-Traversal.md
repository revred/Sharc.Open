# Graph Traversal

Sharc includes a high-performance graph engine for navigating semantic networks. Optimized for zero-allocation traversals backed by SQLite B-trees.

## Setup

```csharp
using Sharc;
using Sharc.Core;
using Sharc.Core.BTree;
using Sharc.Core.Format;
using Sharc.Core.IO;
using Sharc.Graph;
using Sharc.Graph.Model;
using Sharc.Graph.Schema;

// From a database file
byte[] dbBytes = File.ReadAllBytes("graph.db");
var pageSource = new MemoryPageSource(dbBytes);
var header = DatabaseHeader.Parse(pageSource.GetPage(1));
var reader = new BTreeReader(pageSource, header);

using var graph = new SharcContextGraph(reader, new NativeSchemaAdapter());
graph.Initialize();
```

## BFS Traversal

The primary API for graph exploration. Uses a two-phase BFS: edge-only discovery (Phase 1) followed by batch node lookup (Phase 2) for optimal page cache locality.

```csharp
var policy = new TraversalPolicy
{
    Direction = TraversalDirection.Outgoing,
    MaxDepth = 3,
    MaxFanOut = 10,
};

var result = graph.Traverse(new NodeKey(12345), policy);

foreach (var node in result.Nodes)
{
    Console.WriteLine($"Node {node.Record.Id} at depth {node.Depth}");
}
```

## TraversalPolicy

Controls BFS behavior:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Direction` | `TraversalDirection` | `Outgoing` | `Outgoing`, `Incoming`, or `Both` |
| `MaxDepth` | `int?` | `null` | Maximum hops from start node |
| `MaxFanOut` | `int?` | `null` | Maximum edges per node (hub capping) |
| `Kind` | `RelationKind?` | `null` | Filter to specific edge type |
| `TargetTypeFilter` | `int?` | `null` | Only return nodes of this type |
| `MaxTokens` | `int?` | `null` | Stop when token budget is exhausted |
| `Timeout` | `TimeSpan?` | `null` | Maximum traversal time |
| `MinWeight` | `float?` | `null` | Minimum edge weight threshold |
| `StopAtKey` | `NodeKey` | `default` | Stop when this node is reached |
| `IncludePaths` | `bool` | `false` | Track full path from start to each node |
| `IncludeData` | `bool` | `true` | Include JSON data in `GraphRecord` |

### Examples

**Incoming edges (reverse traversal):**
```csharp
var policy = new TraversalPolicy
{
    Direction = TraversalDirection.Incoming,
    MaxDepth = 1,
    IncludeData = false,  // Skip JSON for performance
};
var result = graph.Traverse(new NodeKey(500), policy);
```

**Bidirectional with hub capping:**
```csharp
var policy = new TraversalPolicy
{
    Direction = TraversalDirection.Both,
    MaxDepth = 2,
    MaxFanOut = 20,  // Limit to 20 edges per node
};
```

**Token-budgeted expansion:**
```csharp
var policy = new TraversalPolicy
{
    Direction = TraversalDirection.Outgoing,
    MaxDepth = 5,
    MaxTokens = 4096,  // Stop when total tokens exceed budget
};
// Nodes are added in BFS order, so closer nodes are prioritized
```

**Path tracking:**
```csharp
var policy = new TraversalPolicy
{
    Direction = TraversalDirection.Outgoing,
    MaxDepth = 4,
    IncludePaths = true,
};
var result = graph.Traverse(new NodeKey(1), policy);

foreach (var node in result.Nodes)
{
    if (node.Path != null)
    {
        string pathStr = string.Join(" -> ", node.Path.Select(k => k.Value));
        Console.WriteLine($"Path to {node.Record.Id}: {pathStr}");
    }
}
```

**Stop-at-target:**
```csharp
var policy = new TraversalPolicy
{
    Direction = TraversalDirection.Outgoing,
    MaxDepth = 10,
    StopAtKey = new NodeKey(targetId),
};
// BFS stops as soon as targetId is dequeued
```

## Zero-Allocation Edge Cursor

For raw edge iteration without node materialization (fastest path):

```csharp
using var cursor = graph.GetEdgeCursor(new NodeKey(1));

while (cursor.MoveNext())
{
    long origin = cursor.OriginKey;
    long target = cursor.TargetKey;
    int kind = cursor.Kind;
    float weight = cursor.Weight;
}
```

### Cursor Reset (Multi-Hop)

Reuse the same cursor for multi-hop traversal without allocation:

```csharp
using var cursor = graph.GetEdgeCursor(new NodeKey(1));

// Hop 1: collect neighbors
var hop1 = new HashSet<long>();
while (cursor.MoveNext())
    hop1.Add(cursor.TargetKey);

// Hop 2: iterate from each neighbor
int hop2Count = 0;
foreach (var target in hop1)
{
    cursor.Reset(target);
    while (cursor.MoveNext())
        hop2Count++;
}
```

This pattern is **31x faster** than equivalent SQLite queries (2.60us vs 81.55us) with only 928 bytes allocated.

## Single Node Lookup

```csharp
GraphRecord? node = graph.GetNode(new NodeKey(42));
if (node.HasValue)
{
    Console.WriteLine($"Type: {node.Value.TypeId}, Tokens: {node.Value.Tokens}");
}
```

## Graph Types

### NodeKey

```csharp
var key = new NodeKey(12345);   // From integer
long value = key.Value;          // Back to integer

// ASCII encoding for 48-bit identifiers
string ascii = key.ToAscii();
var restored = NodeKey.FromAscii(ascii);
```

### TraversalDirection

```csharp
TraversalDirection.Outgoing  // Origin -> Target (default)
TraversalDirection.Incoming  // Target -> Origin
TraversalDirection.Both      // Both directions
```

### RelationKind

Semantic edge types for knowledge graphs:

| Kind | Value | Category |
|------|-------|----------|
| `Contains` | 10 | Structural |
| `Defines` | 11 | Structural |
| `Imports` | 12 | Dependency |
| `Inherits` | 13 | Dependency |
| `Implements` | 14 | Dependency |
| `Calls` | 15 | Flow |
| `Instantiates` | 16 | Flow |
| `Reads` | 17 | Flow |
| `Writes` | 18 | Flow |
| `Addresses` | 19 | Contextual |
| `Explains` | 20 | Contextual |
| `MentionedIn` | 21 | Contextual |
| `RefersTo` | 30 | Session |
| `Follows` | 31 | Session |

### GraphResult

```csharp
var result = graph.Traverse(startKey, policy);

result.Nodes         // IReadOnlyList<TraversalNode>
result.Nodes.Count   // Number of nodes found
```

### TraversalNode

```csharp
foreach (var node in result.Nodes)
{
    node.Record  // GraphRecord — the node data
    node.Depth   // int — hops from start
    node.Path    // IReadOnlyList<NodeKey>? — full path (if IncludePaths=true)
}
```

## Cypher Query Language

Full tokenizer, parser, compiler, and executor pipeline for graph pattern matching:

```csharp
// One-shot Cypher query
var result = graph.Cypher(
    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN b.name");

foreach (var node in result.Nodes)
    Console.WriteLine(node.Record.Id);
```

### PreparedCypher

Pre-compile a Cypher query for repeated execution:

```csharp
using var cypher = graph.PrepareCypher(
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b");

// Execute from all start points
var allResults = cypher.Execute();

// Execute from a specific start node
var fromAlice = cypher.Execute(new NodeKey(aliceId));

// Execute between two specific nodes
var path = cypher.Execute(new NodeKey(aliceId), new NodeKey(bobId));
```

## GraphWriter

Full read/write graph operations for creating and modifying knowledge graphs:

```csharp
using var writer = new GraphWriter(sharcWriter, schema, ledger, signer);

// Intern — create or find a node by kind + name
NodeKey alice = writer.Intern("alice-001", new NodeKey(1),
    ConceptKind.Person, jsonData: "{\"role\":\"engineer\"}");

NodeKey bob = writer.Intern("bob-002", new NodeKey(2),
    ConceptKind.Person, jsonData: "{\"role\":\"manager\"}");

// Link — create a typed, weighted, directional edge
long edgeId = writer.Link("edge-001", alice, bob,
    RelationKind.Knows, weight: 0.9f);

// Remove — delete a node and its edges
bool removed = writer.Remove(new NodeKey(oldNodeId));

// Unlink — delete a specific edge by its row ID
bool unlinked = writer.Unlink(edgeId);
```

### GraphWriter Methods

| Method | Signature | Description |
|:---|:---|:---|
| `Intern` | `Intern(id, key, kind, jsonData, nodeAlias?, tokens?)` | Create or find a node |
| `Link` | `Link(id, origin, target, kind, jsonData, weight)` | Create a directional edge |
| `Remove` | `Remove(key)` | Delete a node and all its edges |
| `Unlink` | `Unlink(edgeRowId)` | Delete a single edge |

## Graph Algorithms

Graph algorithms are available internally through `SharcContextGraph`:

### Shortest Path

```csharp
var path = graph.ShortestPath(
    new NodeKey(startId),
    new NodeKey(endId),
    new TraversalPolicy { MaxDepth = 10 });

if (path != null)
{
    Console.WriteLine($"Path length: {path.Count} hops");
    foreach (var nodeKey in path)
        Console.WriteLine($"  -> {nodeKey.Value}");
}
```

### PageRank

Identify influential nodes in the graph:

```csharp
var ranks = PageRankComputer.Compute(graph, iterations: 20, dampingFactor: 0.85);
foreach (var (nodeId, rank) in ranks.OrderByDescending(r => r.Value).Take(10))
    Console.WriteLine($"Node {nodeId}: rank {rank:F4}");
```

### Degree Centrality

Find the most connected nodes:

```csharp
var centrality = DegreeCentralityComputer.Compute(graph);
foreach (var result in centrality.OrderByDescending(r => r.TotalDegree).Take(10))
    Console.WriteLine($"Node {result.Key}: in={result.InDegree}, out={result.OutDegree}");
```

### Topological Sort

Dependency ordering for DAGs:

```csharp
var sorted = TopologicalSortComputer.Compute(graph, kind: RelationKind.Imports);
foreach (var nodeKey in sorted)
    Console.WriteLine(nodeKey.Value);
```

## Performance

| Operation | Sharc | SQLite | Speedup |
|-----------|-------|--------|---------|
| 2-hop BFS (5K nodes, 15K edges) | 2.60 us | 81.55 us | **31x** |
| Single node seek | 3.4 us | 24.3 us | **7.1x** |
| Node scan (5K) | 1,027 us | 2,853 us | **2.8x** |
| Edge scan (5K) | 2,268 us | 7,673 us | **3.4x** |

The cursor-based BFS allocates only **928 bytes** compared to SQLite's 2.8 KB.
