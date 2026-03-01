# Querying Data

## Sharq Query Language

Sharc supports a SQL-like query language called Sharq for structured data access:

```csharp
using var db = SharcDatabase.Open("mydata.db");

using var reader = db.Query("SELECT id, name FROM users WHERE age > 18");
while (reader.Read())
{
    long id = reader.GetInt64(0);
    string name = reader.GetString(1);
}
```

## Parameterized Queries

Prevent injection and enable plan caching with parameters:

```csharp
var parameters = new Dictionary<string, object>
{
    ["$minAge"] = 18L,
    ["$status"] = "active"
};

using var reader = db.Query(parameters,
    "SELECT id, name FROM users WHERE age > $minAge AND status = $status");
```

## Agent-Scoped Queries

Enforce read entitlements by passing an `AgentInfo`:

```csharp
// Agent can only read tables/columns allowed by their ReadScope
using var reader = db.Query("SELECT * FROM sensitive_data", agent);
// Throws UnauthorizedAccessException if ReadScope denies access
```

See [Trust Layer](Trust-Layer) for agent setup.

## Query Plan Caching

Sharc caches compiled query plans (filter delegates, projection arrays, table metadata) for repeated queries with the same structure. The cache is keyed by `QueryIntent`, so parameterized queries benefit from plan reuse automatically.

## Streaming Top-K with Custom Scoring

Use `JitQuery.TopK()` to find the K best-scoring rows without materializing the entire result set. The scorer runs on each row after all filters have been applied. Rows that score worse than the current worst in the bounded heap are never materialized, keeping memory at O(K).

Lower scores rank higher (natural for distance-based scoring).

### With a reusable scorer class

```csharp
// Implement IRowScorer for reusable, stateful scoring
sealed class DistanceScorer(double cx, double cy) : IRowScorer
{
    public double Score(IRowAccessor row)
    {
        double dx = row.GetDouble(0) - cx;
        double dy = row.GetDouble(1) - cy;
        return Math.Sqrt(dx * dx + dy * dy);
    }
}

// Filter first (B-tree accelerated), then score survivors
var jit = db.Jit("points");
jit.Where(FilterStar.Column("x").Between(cx - r, cx + r));
jit.Where(FilterStar.Column("y").Between(cy - r, cy + r));
using var reader = jit.TopK(20, new DistanceScorer(cx, cy), "x", "y", "id");

while (reader.Read())
{
    double x = reader.GetDouble(0);
    double y = reader.GetDouble(1);
    long id = reader.GetInt64(2);
}
```

### With a lambda scorer

```csharp
// One-off scoring without implementing IRowScorer
using var reader = jit.TopK(10,
    row => Math.Abs(row.GetDouble(0) - target),
    "value", "label");
```

### Key points

- `TopK()` is a **terminal method** (like `Query()`) that executes immediately
- Composes with all existing filters: `FilterStar`, `IRowAccessEvaluator`, `WithLimit`
- Only K rows are materialized; rejected rows incur zero allocation
- Results are sorted ascending by score (best first)

See the [`PrimeExample`](../samples/PrimeExample/) sample for a complete spatial query walkthrough.

## JOIN Queries

Sharc supports INNER, LEFT, RIGHT, FULL OUTER, and CROSS joins:

```csharp
using var reader = db.Query(
    "SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.total > 100");
while (reader.Read())
    Console.WriteLine($"{reader.GetString(0)}: {reader.GetDouble(1)}");
```

Supported join types: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`.

## Compound Queries (UNION / INTERSECT / EXCEPT)

Combine multiple result sets:

```csharp
// UNION ALL — all rows from both queries
using var reader = db.Query(
    "SELECT name FROM employees UNION ALL SELECT name FROM contractors");

// UNION — deduplicated rows
using var reader2 = db.Query(
    "SELECT name FROM employees UNION SELECT name FROM contractors");

// INTERSECT — rows in both queries
using var reader3 = db.Query(
    "SELECT name FROM employees INTERSECT SELECT name FROM contractors");

// EXCEPT — rows in first but not second
using var reader4 = db.Query(
    "SELECT name FROM employees EXCEPT SELECT name FROM contractors");
```

Compound queries support final `ORDER BY` and `LIMIT` clauses.

## GROUP BY and Aggregates

```csharp
using var reader = db.Query(
    "SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM users GROUP BY dept ORDER BY cnt DESC");
while (reader.Read())
    Console.WriteLine($"{reader.GetString(0)}: {reader.GetInt64(1)} employees, avg ${reader.GetDouble(2):F2}");
```

Supported aggregate functions: `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)`.

## Common Table Expressions (Cotes)

```csharp
using var reader = db.Query(
    "WITH active AS (SELECT id, name FROM users WHERE active = 1) " +
    "SELECT * FROM active WHERE id > 100");

// Cotes with compound queries
using var reader2 = db.Query(
    "WITH team_a AS (SELECT name FROM users WHERE dept = 'A'), " +
    "     team_b AS (SELECT name FROM users WHERE dept = 'B') " +
    "SELECT * FROM team_a UNION ALL SELECT * FROM team_b");
```

## ORDER BY, LIMIT, OFFSET

```csharp
using var reader = db.Query(
    "SELECT id, name FROM users ORDER BY name ASC LIMIT 20 OFFSET 40");
```

Multi-column sort and streaming TopN heap are used for efficient ordering.

## Prepared Queries

Pre-compile a query for zero-overhead repeated execution:

```csharp
using var prepared = db.Prepare("SELECT id, name FROM users WHERE age > $minAge");

// Execute multiple times with different parameters
var young = new Dictionary<string, object> { ["$minAge"] = 18L };
using var r1 = prepared.Execute(young);

var senior = new Dictionary<string, object> { ["$minAge"] = 65L };
using var r2 = prepared.Execute(senior);

// Or execute without parameters
using var r3 = prepared.Execute();
```

## Execution Hints

Route queries through cached or JIT-compiled execution paths:

```csharp
// Use cached plan
using var reader = db.Query("/*+ CACHED */ SELECT * FROM users WHERE active = 1");

// Use JIT execution
using var reader2 = db.Query("/*+ JIT */ SELECT * FROM users WHERE age > 30");
```

## Supported Syntax

Sharq supports:
- `SELECT` with column projection and `SELECT *`
- `WHERE` with comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`), `LIKE`, `IN`, `BETWEEN`, `IS NULL`
- `AND` / `OR` logical operators
- `JOIN` — INNER, LEFT, RIGHT, FULL OUTER, CROSS
- `UNION ALL` / `UNION` / `INTERSECT` / `EXCEPT`
- `GROUP BY` with `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `ORDER BY` with multi-column sort and streaming TopN
- `LIMIT` / `OFFSET`
- `WITH` (Common Table Expressions / Cotes)
- Parameterized values (`$param`)
- `TopK` with custom scoring (via `JitQuery.TopK()`)
- Table and column name resolution (case-insensitive)
