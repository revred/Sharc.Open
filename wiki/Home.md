# Sharc API Documentation

Sharc is a high-performance, pure managed C# library that reads and writes SQLite database files from disk and in-memory buffers. Zero native dependencies. 2-609x faster than Microsoft.Data.Sqlite.
It includes strict typed 128-bit column support for `GUID`/`UUID` and `FIX128` (`decimal`, 28-29 significant digits).

## Quick Start

```csharp
using Sharc;

// Open a database
using var db = SharcDatabase.Open("mydata.db");

// Read data
using var reader = db.CreateReader("users");
while (reader.Read())
{
    long id = reader.GetInt64(0);
    string name = reader.GetString(1);
}
```

## API Reference

| Page | Description |
|------|-------------|
| [Opening Databases](Opening-Databases) | `SharcDatabase.Open`, `OpenMemory`, `Create`, options |
| [Reading Data](Reading-Data) | `SharcDataReader`, typed accessors, seek, projection |
| [Querying Data](Querying-Data) | SQL queries, JOINs, UNION, GROUP BY, CTEs, prepared queries |
| [Writing Data](Writing-Data) | `SharcWriter`, insert/update/delete/upsert, `PreparedWriter`, transactions |
| [Schema Inspection](Schema-Inspection) | `SharcSchema`, `TableInfo`, `ColumnInfo`, `IndexInfo` |
| [Graph Traversal](Graph-Traversal) | `SharcContextGraph`, BFS, Cypher, `GraphWriter`, algorithms |
| [Encryption](Encryption) | AES-256-GCM, Argon2id KDF, encrypted databases |
| [Trust Layer](Trust-Layer) | Agent registry, ledger, ECDSA attestation, entitlements |
| [Views](Views) | Programmatic views, SubViews, SQLite view auto-promotion, SQL integration |
| [JitSQL & Prepared](JitSQL-and-Prepared) | JitQuery, PreparedReader, execution hints, FilterStar |
| [Vector Search](Vector-Search) | `HnswIndex`, `VectorQuery`, `HybridQuery`, SIMD distance |
| [Performance Guide](Performance-Guide) | Zero-allocation patterns, benchmarks, best practices |
| [AI Agent Reference](AI-Agent-Reference) | Complete copy-paste patterns for LLM coding assistants |
| [Release History](Release-History) | NuGet versions, changelogs (1.0.0-beta → 1.2.80) |

## Installation

```bash
dotnet add package Sharc          # Core: read + write + query
dotnet add package Sharc.Crypto   # AES-256-GCM encryption
dotnet add package Sharc.Graph    # Graph traversal engine
dotnet add package Sharc.Vector   # HNSW similarity search
```

## Architecture

```
SharcDatabase → SharcDataReader     (read path)
SharcWriter → SharcWriteTransaction (write path)
SharcContextGraph → Traverse/Cursor (graph path)
AgentRegistry + LedgerManager       (trust path)
```

All operations go through the B-tree layer, which reads SQLite pages directly from `IPageSource` (file, memory, or encrypted). No native SQLite bindings are involved.

## Additional Resources

| Guide | Description |
| ----- | ----------- |
| [JitSQL Cross-Language](../docs/JITSQL_CROSS_LANGUAGE.md) | JitSQL patterns for JS/TS/Python/Go developers |
| [Graph DB Comparison](../docs/GRAPH_DB_COMPARISON.md) | Sharc vs SurrealDB, ArangoDB, Neo4j |
| [Vector Search Guide](../docs/VECTOR_SEARCH.md) | Embedding storage, RAG, semantic cache patterns |
| [Alternatives](../docs/ALTERNATIVES.md) | Honest comparison vs SQLite, LiteDB, DuckDB |
| [Samples](../samples/) | 13 runnable sample projects including GUID/FIX128 typed value usage |
