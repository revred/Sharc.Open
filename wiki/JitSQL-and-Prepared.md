# JitSQL & Prepared Queries

## JitQuery

`JitQuery` builds and executes queries programmatically with zero-allocation filter composition:

```csharp
var jit = db.Jit("users");
```

### Adding Filters

```csharp
jit.Where(FilterStar.Column("age").Gte(18L));
jit.Where(FilterStar.Column("status").Eq("active"));
// Filters are AND-composed
```

### Executing

```csharp
using var reader = jit.Query();
while (reader.Read())
    Console.WriteLine($"{reader.GetInt64(0)}: {reader.GetString(1)}");
```

### TopK with Custom Scoring

Find the K best-scoring rows without materializing the full result set:

```csharp
// With a reusable scorer class
sealed class DistanceScorer(double cx, double cy) : IRowScorer
{
    public double Score(IRowAccessor row)
    {
        double dx = row.GetDouble(0) - cx;
        double dy = row.GetDouble(1) - cy;
        return Math.Sqrt(dx * dx + dy * dy);
    }
}

var jit = db.Jit("points");
jit.Where(FilterStar.Column("x").Between(cx - r, cx + r));
using var reader = jit.TopK(20, new DistanceScorer(cx, cy), "x", "y", "id");
```

```csharp
// With a lambda scorer (one-off)
using var reader = jit.TopK(10,
    row => Math.Abs(row.GetDouble(0) - target),
    "value", "label");
```

- `TopK()` is a terminal method — executes immediately
- Only K rows are materialized; rejected rows incur zero allocation
- Results are sorted ascending by score (best first)

### JitQuery over Views and Layers

```csharp
// Query a registered view/layer
var jit = db.Jit(myLayer);
jit.Where(FilterStar.Column("active").Eq(1L));
using var reader = jit.Query();
```

## PreparedReader

Zero-allocation cursor reuse for repeated point lookups:

```csharp
using var prepared = db.PrepareReader("users");

// Each Seek reuses the same cursor — zero allocation per call
if (prepared.Seek(42))
    Console.WriteLine(prepared.GetString(1));

if (prepared.Seek(99))
    Console.WriteLine(prepared.GetString(1));

// With column projection
using var projected = db.PrepareReader("users", "id", "name");
```

This is the fastest read path in Sharc — **609x faster** than SQLite at **38ns** per seek with **0 B** allocation.

## PreparedQuery

Pre-compile a SQL query for zero-overhead repeated execution:

```csharp
using var prepared = db.Prepare("SELECT id, name FROM users WHERE age > $minAge");

// Execute with different parameter sets
var young = new Dictionary<string, object> { ["$minAge"] = 18L };
using var r1 = prepared.Execute(young);

var senior = new Dictionary<string, object> { ["$minAge"] = 65L };
using var r2 = prepared.Execute(senior);

// Execute without parameters
using var r3 = prepared.Execute();
```

## Execution Hints

Route queries through optimized execution paths using SQL comments:

| Hint | Effect |
| :--- | :--- |
| `/*+ CACHED */` | Use cached compiled query plan |
| `/*+ JIT */` | Use JIT-compiled execution path |

```csharp
using var reader = db.Query("/*+ CACHED */ SELECT * FROM users WHERE active = 1");
```

## FilterStar

Predicate builder for type-safe, composable filters:

```csharp
// Simple comparisons
FilterStar.Column("age").Gte(18L)
FilterStar.Column("status").Eq("active")
FilterStar.Column("name").Like("A%")

// Range
FilterStar.Column("x").Between(0.0, 100.0)

// Composition
FilterStar.And(
    FilterStar.Column("age").Gte(18L),
    FilterStar.Column("status").Eq("active")
)
```

## See Also

- [Reading Data](Reading-Data) — Core reader API
- [Querying Data](Querying-Data) — SQL query syntax
- [Performance Guide](Performance-Guide) — Zero-allocation patterns
