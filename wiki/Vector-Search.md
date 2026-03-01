# Vector Search

Sharc.Vector provides SIMD-accelerated vector similarity search with zero-copy metadata pre-filtering. Added in v1.1.6-rc1.

## Installation

```bash
dotnet add package Sharc.Vector
```

## HnswIndex

The core vector index using Hierarchical Navigable Small World graphs:

```csharp
using Sharc.Vector;

var index = new HnswIndex(dimensions: 384, metric: DistanceMetric.Cosine);
```

### Properties

| Property | Type | Description |
| :--- | :--- | :--- |
| `Count` | `int` | Number of vectors in the index |
| `Dimensions` | `int` | Vector dimensionality |
| `Metric` | `DistanceMetric` | Configured distance function |
| `Config` | `HnswConfig` | HNSW graph configuration |
| `HasPendingMutations` | `bool` | Whether mutations await merge |
| `Version` | `long` | Mutation counter for cache invalidation |

### Upsert (Add or Update)

```csharp
// Insert a new vector or update an existing one
index.Upsert(rowId: 1, new float[] { 0.1f, 0.2f, 0.3f, ... });
index.Upsert(rowId: 2, new float[] { 0.4f, 0.5f, 0.6f, ... });

// Update — same rowId replaces the vector
index.Upsert(rowId: 1, new float[] { 0.11f, 0.22f, 0.33f, ... });
```

### Delete

```csharp
bool deleted = index.Delete(rowId: 1);  // Tombstone deletion
```

### Search

```csharp
var results = index.Search(queryVector, k: 10);

foreach (var match in results)
    Console.WriteLine($"ID: {match.RowId}, Distance: {match.Distance}");

// With custom ef (exploration factor) for accuracy/speed tradeoff
var precise = index.Search(queryVector, k: 10, ef: 200);
```

### Merge Pending Mutations

After batch inserts/updates, merge mutations into the base graph:

```csharp
index.MergePendingMutations();
```

## VectorQuery

Fluent query builder combining vector search with relational filters:

```csharp
var query = db.VectorQuery("documents");

var results = query
    .UseIndex(index)
    .Where(FilterStar.Column("category").Eq("science"))
    .NearestTo(queryVector, k: 10, "id", "title", "body");

foreach (var match in results)
    Console.WriteLine($"{match.Distance:F4}: {match.Reader.GetString(1)}");
```

### VectorQuery Methods

| Method | Description |
| :--- | :--- |
| `UseIndex(index)` | Attach an HNSW index |
| `Where(filter)` | Add metadata pre-filter |
| `ClearFilters()` | Remove all filters |
| `WithAgent(agent)` | Apply agent entitlements |
| `WithRowEvaluator(eval)` | Custom row evaluation |
| `NearestTo(vector, k, columns)` | K nearest neighbors |
| `WithinDistance(vector, maxDist, columns)` | All vectors within distance |

### Distance-Based Search

```csharp
// Find all vectors within a distance threshold
var nearby = query
    .UseIndex(index)
    .WithinDistance(queryVector, maxDistance: 0.5f, "id", "title");
```

## HybridQuery

Combine vector similarity with text relevance using Reciprocal Rank Fusion (RRF):

```csharp
var hybrid = db.HybridQuery("documents");

var results = hybrid
    .UseIndex(index)
    .Where(FilterStar.Column("active").Eq(1L))
    .Search(queryVector, queryText: "machine learning", k: 20, "id", "title", "body");

foreach (var match in results)
    Console.WriteLine($"Score: {match.FusedScore:F4}, {match.Reader.GetString(1)}");
```

## Distance Metrics

| Metric | Use Case |
| :--- | :--- |
| `Cosine` | Text embeddings, normalized vectors |
| `Euclidean` | Spatial data, image features |
| `DotProduct` | Pre-normalized vectors, maximum inner product |

## HnswConfig

Tune the HNSW graph parameters:

| Parameter | Default | Description |
| :--- | ---: | :--- |
| `M` | 16 | Max neighbors per node (higher = more accurate, more memory) |
| `EfConstruction` | 200 | Exploration factor during build (higher = better graph quality) |
| `EfSearch` | 50 | Exploration factor during search (higher = more accurate, slower) |
| `MaxLevels` | auto | Maximum HNSW levels |

## Integration with Sharc

```csharp
using var db = SharcDatabase.Open("knowledge.db");

// Use PreparedReader for zero-allocation lookups from search results
using var prepared = db.PrepareReader("documents", "id", "title", "body");

var results = index.Search(queryEmbedding, k: 5);
foreach (var match in results)
{
    if (prepared.Seek(match.RowId))
        Console.WriteLine($"{match.Distance:F4}: {prepared.GetString(1)}");
}
```

## Thread Safety

- `Search()` is thread-safe after construction — multiple threads can query concurrently
- `Upsert()` and `Delete()` require external synchronization
- `MergePendingMutations()` should be called from a single thread

## See Also

- [docs/VECTOR_SEARCH.md](../docs/VECTOR_SEARCH.md) — Full vector search guide with RAG patterns
- [Performance Guide](Performance-Guide) — Benchmark results
- [AI Agent Reference](AI-Agent-Reference) — Agent memory patterns
