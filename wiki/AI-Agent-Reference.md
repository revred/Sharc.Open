# Sharc — AI Agent Quick Reference

> This page is optimized for AI coding assistants (Claude, Codex, Gemini, Copilot).
> It contains complete, copy-pasteable patterns with all required imports and no ambiguity.

## Package Installation

```bash
dotnet add package Sharc          # Core: read, write, query, schema
dotnet add package Sharc.Crypto   # Optional: AES-256-GCM encryption
dotnet add package Sharc.Graph    # Optional: graph traversal engine
```

**Target frameworks:** .NET 8.0+

---

## CRITICAL RULES

1. **Always `using` dispose** — `SharcDatabase`, `SharcDataReader`, `SharcWriter`, `SharcWriteTransaction`, `SharcContextGraph`, and `IEdgeCursor` are all `IDisposable`.
2. **Not thread-safe** — `SharcDatabase` and all readers/writers must be used from a single thread.
3. **Forward-only reader** — `SharcDataReader.Read()` moves forward only. Call `Seek()` for random access.
4. **Column ordinals are zero-based** and match the projection order, not the table schema order.
5. **ColumnValue serial types** — Use `1` for integers, `7` for doubles, `13` for text, `12` for blobs. Sharc computes the actual serial type from data length.
6. **Graph API:** `GetEdges()` and `GetIncomingEdges()` are **deprecated**. Use `GetEdgeCursor()` or `Traverse()`.

---

## Pattern: Open Database and Read All Rows

```csharp
using Sharc;

using var db = SharcDatabase.Open("mydata.db");
using var reader = db.CreateReader("users");

while (reader.Read())
{
    long id = reader.GetInt64(0);
    string name = reader.GetString(1);
    string email = reader.GetString(2);
}
```

## Pattern: Open In-Memory Database

```csharp
using Sharc;

byte[] dbBytes = File.ReadAllBytes("mydata.db");
using var db = SharcDatabase.OpenMemory(dbBytes);
// Use identically to file-backed database
```

## Pattern: Column Projection (Read Only Specific Columns)

```csharp
using Sharc;

using var db = SharcDatabase.Open("mydata.db");
// Only "id" and "email" are decoded; all other columns are skipped at byte level
using var reader = db.CreateReader("users", "id", "email");

while (reader.Read())
{
    long id = reader.GetInt64(0);       // ordinal 0 = first projected column ("id")
    string email = reader.GetString(1); // ordinal 1 = second projected column ("email")
}
```

## Pattern: Point Lookup by Rowid

```csharp
using Sharc;

using var db = SharcDatabase.Open("mydata.db");
using var reader = db.CreateReader("users");

if (reader.Seek(42))  // O(log N) B-tree seek to rowid 42
{
    string name = reader.GetString(1);
}
```

## Pattern: Filtered Scan

```csharp
using Sharc;

using var db = SharcDatabase.Open("mydata.db");
using var reader = db.CreateReader("users",
    new SharcFilter("age", SharcOperator.GreaterOrEqual, 18L),
    new SharcFilter("status", SharcOperator.Equal, "active"));

while (reader.Read())
{
    // Only rows matching ALL filters (AND semantics)
    long id = reader.GetInt64(0);
    string name = reader.GetString(1);
}
```

## Pattern: Sharq Query with Parameters

```csharp
using Sharc;

using var db = SharcDatabase.Open("mydata.db");

var parameters = new Dictionary<string, object>
{
    ["$minAge"] = 18L,
    ["$status"] = "active"
};

using var reader = db.Query(parameters,
    "SELECT id, name FROM users WHERE age > $minAge AND status = $status");

while (reader.Read())
{
    long id = reader.GetInt64(0);
    string name = reader.GetString(1);
}
```

## Pattern: Insert a Single Row

```csharp
using Sharc;

using var writer = SharcWriter.Open("mydata.db");

long rowId = writer.Insert("users",
    ColumnValue.FromInt64(1, 42),                         // integer column
    ColumnValue.Text(13, "Alice"u8.ToArray()),            // text column
    ColumnValue.Text(13, "alice@example.com"u8.ToArray()) // text column
);
// rowId is the auto-assigned rowid
```

## Pattern: Batch Insert (Multiple Rows, Single Transaction)

```csharp
using Sharc;

using var writer = SharcWriter.Open("mydata.db");

var records = new[]
{
    new[] { ColumnValue.FromInt64(1, 1), ColumnValue.Text(13, "Alice"u8.ToArray()) },
    new[] { ColumnValue.FromInt64(1, 2), ColumnValue.Text(13, "Bob"u8.ToArray()) },
    new[] { ColumnValue.FromInt64(1, 3), ColumnValue.Text(13, "Carol"u8.ToArray()) },
};

long[] rowIds = writer.InsertBatch("users", records);
```

## Pattern: Update and Delete

```csharp
using Sharc;

using var writer = SharcWriter.Open("mydata.db");

// Update row with rowid 42
bool updated = writer.Update("users", 42,
    ColumnValue.FromInt64(1, 42),
    ColumnValue.Text(13, "Alice Updated"u8.ToArray()),
    ColumnValue.Text(13, "new@example.com"u8.ToArray())
);

// Delete row with rowid 42
bool deleted = writer.Delete("users", 42);
```

## Pattern: Explicit Transaction

```csharp
using Sharc;

using var writer = SharcWriter.Open("mydata.db");
using var tx = writer.BeginTransaction();

long id1 = tx.Insert("users",
    ColumnValue.FromInt64(1, 1),
    ColumnValue.Text(13, "Alice"u8.ToArray()));

long id2 = tx.Insert("logs",
    ColumnValue.FromInt64(1, id1),
    ColumnValue.Text(13, "Created user"u8.ToArray()));

tx.Commit();  // Atomic: both inserts succeed or neither does
// If tx.Dispose() is called without Commit(), all changes are rolled back
```

## Pattern: Schema Inspection

```csharp
using Sharc;

using var db = SharcDatabase.Open("mydata.db");

foreach (var table in db.Schema.Tables)
{
    Console.WriteLine($"Table: {table.Name}");
    foreach (var col in table.Columns)
        Console.WriteLine($"  {col.Name}: {col.DeclaredType} (PK={col.IsPrimaryKey})");
}
```

## Pattern: Encrypted Database

```csharp
using Sharc;

var options = new SharcOpenOptions
{
    Encryption = new SharcEncryptionOptions
    {
        Password = "your-password",
        // Defaults: Argon2id KDF + AES-256-GCM cipher
    }
};

using var db = SharcDatabase.Open("secure.db", options);
// API is identical — reads are transparently decrypted
using var reader = db.CreateReader("users");
```

## Pattern: Graph BFS Traversal

```csharp
using Sharc.Core;
using Sharc.Core.BTree;
using Sharc.Core.Format;
using Sharc.Core.IO;
using Sharc.Graph;
using Sharc.Graph.Model;
using Sharc.Graph.Schema;

// Setup: load database into graph engine
byte[] dbBytes = File.ReadAllBytes("graph.db");
var pageSource = new MemoryPageSource(dbBytes);
var header = DatabaseHeader.Parse(pageSource.GetPage(1));
var btreeReader = new BTreeReader(pageSource, header);

using var graph = new SharcContextGraph(btreeReader, new NativeSchemaAdapter());
graph.Initialize();

// Traverse: 2-hop outgoing BFS from node 1
var policy = new TraversalPolicy
{
    Direction = TraversalDirection.Outgoing,
    MaxDepth = 2,
    MaxFanOut = 20,
};

var result = graph.Traverse(new NodeKey(1), policy);

foreach (var node in result.Nodes)
{
    Console.WriteLine($"Node {node.Record.Id} at depth {node.Depth}");
}
```

## Pattern: Zero-Allocation Edge Cursor

```csharp
using Sharc.Graph;
using Sharc.Graph.Model;

// Fastest path: edge iteration without node materialization
using var cursor = graph.GetEdgeCursor(new NodeKey(1));

var neighbors = new HashSet<long>();
while (cursor.MoveNext())
{
    neighbors.Add(cursor.TargetKey);
}

// Reuse cursor for multi-hop (no new allocation)
int hop2Count = 0;
foreach (var target in neighbors)
{
    cursor.Reset(target);
    while (cursor.MoveNext())
        hop2Count++;
}
```

## Pattern: Programmatic View

```csharp
using Sharc;
using Sharc.Views;

using var db = SharcDatabase.Open("mydata.db");

var view = ViewBuilder
    .From("users")
    .Select("id", "name")
    .Where(row => row.GetInt64(0) > 18)
    .Named("adults")
    .Build();

db.RegisterView(view);

using var cursor = db.OpenView("adults");
while (cursor.MoveNext())
{
    long id = cursor.GetInt64(0);
    string name = cursor.GetString(1);
}
```

## Pattern: Agent-Scoped Access Control

```csharp
using Sharc;
using Sharc.Trust;
using Sharc.Core.Trust;

using var db = SharcDatabase.Create("trusted.db");
var registry = new AgentRegistry(db);

var signer = new SharcSigner("agent-007");
var agent = new AgentInfo(
    AgentId: "agent-007",
    Class: AgentClass.Local,
    PublicKey: signer.GetPublicKey(),
    AuthorityCeiling: 100,
    WriteScope: "users,logs",
    ReadScope: "*",
    ValidityStart: DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
    ValidityEnd: DateTimeOffset.UtcNow.AddDays(30).ToUnixTimeSeconds(),
    ParentAgent: "",
    CoSignRequired: false,
    Signature: Array.Empty<byte>()
);
registry.RegisterAgent(agent);

// Scoped query — enforces ReadScope
using var reader = db.Query("SELECT * FROM users", agent);

// Scoped write — enforces WriteScope
using var writer = SharcWriter.From(db);
writer.Insert(agent, "users", ColumnValue.FromInt64(1, 1), ColumnValue.Text(13, "Alice"u8.ToArray()));
```

---

## ColumnValue Quick Reference

| What you want to store | Factory method | Example |
|------------------------|---------------|---------|
| NULL | `ColumnValue.Null()` | `ColumnValue.Null()` |
| Integer | `ColumnValue.FromInt64(1, value)` | `ColumnValue.FromInt64(1, 42)` |
| Float | `ColumnValue.FromDouble(7, value)` | `ColumnValue.FromDouble(7, 3.14)` |
| Text | `ColumnValue.Text(13, utf8Bytes)` | `ColumnValue.Text(13, "hello"u8.ToArray())` |
| Blob | `ColumnValue.Blob(12, bytes)` | `ColumnValue.Blob(12, new byte[] { 1, 2, 3 })` |
| GUID/UUID | `ColumnValue.FromGuid(guid)` | `ColumnValue.FromGuid(Guid.NewGuid())` |
| FIX128 decimal | `ColumnValue.FromDecimal(value)` | `ColumnValue.FromDecimal(1234567890.12345678m)` |

## SharcOperator Quick Reference

| Operator | SQL | Usage |
|----------|-----|-------|
| `SharcOperator.Equal` | `=` | `new SharcFilter("col", SharcOperator.Equal, value)` |
| `SharcOperator.NotEqual` | `!=` | `new SharcFilter("col", SharcOperator.NotEqual, value)` |
| `SharcOperator.LessThan` | `<` | `new SharcFilter("col", SharcOperator.LessThan, value)` |
| `SharcOperator.GreaterThan` | `>` | `new SharcFilter("col", SharcOperator.GreaterThan, value)` |
| `SharcOperator.LessOrEqual` | `<=` | `new SharcFilter("col", SharcOperator.LessOrEqual, value)` |
| `SharcOperator.GreaterOrEqual` | `>=` | `new SharcFilter("col", SharcOperator.GreaterOrEqual, value)` |

## TraversalPolicy Quick Reference

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Direction` | `TraversalDirection` | `Outgoing` | `Outgoing`, `Incoming`, or `Both` |
| `MaxDepth` | `int?` | `null` (unlimited) | Maximum hops from start |
| `MaxFanOut` | `int?` | `null` (unlimited) | Max edges explored per node |
| `Kind` | `RelationKind?` | `null` (all) | Filter to specific edge type |
| `TargetTypeFilter` | `int?` | `null` (all) | Only return nodes of this type ID |
| `MaxTokens` | `int?` | `null` (unlimited) | Stop when token budget exceeded |
| `Timeout` | `TimeSpan?` | `null` (unlimited) | Maximum traversal wall time |
| `MinWeight` | `float?` | `null` (all) | Minimum edge weight (0.0-1.0) |
| `StopAtKey` | `NodeKey` | `default` | Stop BFS when this node is found |
| `IncludePaths` | `bool` | `false` | Track paths from start to each node |
| `IncludeData` | `bool` | `true` | Include JSON data payload |

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Forgetting `using` on `SharcDatabase` | Always wrap in `using var db = ...` |
| Using `GetEdges()` | Use `GetEdgeCursor()` or `Traverse()` — `GetEdges` is deprecated |
| Reading column by table ordinal after projection | Use projection ordinal (0 = first projected column) |
| Calling `reader.GetString(0)` on a NULL column | Check `reader.IsNull(0)` first |
| Not calling `graph.Initialize()` | Required before any graph operations |
| Using `SharcWriter.Open()` for read-only access | Use `SharcDatabase.Open()` instead |
| Multiple threads sharing `SharcDatabase` | Create separate instances per thread |
| Not calling `tx.Commit()` | Dispose without Commit rolls back all changes |
