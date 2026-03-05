# Sharc

**Sharc reads SQLite files 2-609x faster than Managed Sqlite, in pure C#, with zero native dependencies.**

[![Live Arena](https://img.shields.io/badge/Live_Arena-Run_Benchmarks-blue?style=for-the-badge)](https://revred.github.io/Sharc.Open/)
[![NuGet](https://img.shields.io/nuget/v/Sharc.svg?style=for-the-badge)](https://www.nuget.org/packages/Sharc/)
[![Tests](https://img.shields.io/badge/tests-3%2C686_passing-brightgreen?style=for-the-badge)]()
[![License](https://img.shields.io/badge/license-MIT-green?style=for-the-badge)](LICENSE)

---

| **Speed** | **Size** | **Trust** | **Graph & AI** |
| :--- | :--- | :--- | :--- |
| **387x faster** indexed WHERE | **~52 KB** engine footprint | **ECDSA** agent attestation | **Cypher** query language |
| **609x faster** B-tree seeks | **Zero** native dependencies | **AES-256-GCM** encryption | **PageRank** / centrality / topo-sort |
| **13.8x faster** graph seeks | WASM / Mobile / IoT ready | **Tamper-evident** audit ledger | **Cross-arc** distributed sync |
| **~0 B** per-row read allocation | SQL query pipeline built-in | JOIN / UNION / INTERSECT / EXCEPT / Cote | **GraphWriter** — full read/write graph |

---

## When to Use Sharc

| Your Problem | Solution |
| :--- | :--- |
| Need to read/write SQLite **without native DLLs** | `dotnet add package Sharc` — pure managed C# |
| SQLite P/Invoke is **too slow** for point lookups | Sharc: **38ns** vs 23,227ns (**609x** faster) |
| Need an embedded DB for **Blazor WASM** | Sharc: **~40KB**, no Emscripten, no special headers |
| Need **AI agent memory** with audit trail | Built-in ECDSA attestation + hash-chain ledger |
| Need **graph traversal** over relational data | 2-hop BFS: **4.5x** faster than SQLite recursive CTE |
| Need **vector similarity search** for RAG | SIMD-accelerated cosine/euclidean, zero-copy, metadata pre-filter |
| Need **zero GC pressure** on hot read paths | 0 B per-row allocation via `Span<T>` |

**Not a fit?** See [When NOT to Use Sharc](docs/WHEN_NOT_TO_USE.md) — we're honest about limitations.

---

## Install

```bash
dotnet add package Sharc            # Core read/write engine
dotnet add package Sharc.Crypto     # AES-256-GCM encryption (optional)
dotnet add package Sharc.Graph      # Graph + Cypher + algorithms (optional)
dotnet add package Sharc.Vector     # Vector similarity search (optional)
dotnet add package Sharc.Arc        # Cross-arc diff, sync, distributed fragments (optional)
```

## Quick Start

```csharp
using Sharc;

using var db = SharcDatabase.Open("mydata.db");

// Scan a table
using var reader = db.CreateReader("users");
while (reader.Read())
    Console.WriteLine($"{reader.GetInt64(0)}: {reader.GetString(1)}");

// Point lookup in < 1 microsecond
if (reader.Seek(42))
    Console.WriteLine($"Found: {reader.GetString(1)}");

// SQL queries — SELECT, WHERE, ORDER BY, GROUP BY, UNION, Cote
using var results = db.Query(
    "SELECT dept, COUNT(*) AS cnt FROM users WHERE age > 25 GROUP BY dept ORDER BY cnt DESC LIMIT 10");
while (results.Read())
    Console.WriteLine($"{results.GetString(0)}: {results.GetInt64(1)}");
```

[**Full Getting Started Guide**](docs/GETTING_STARTED.md)

---

## Capabilities

| Feature | Package | Details |
| :--- | :--- | :--- |
| Cypher graph queries | `Sharc.Graph` | Full tokenizer → parser → compiler → executor pipeline |
| Graph algorithms | `Sharc.Graph` | PageRank, degree centrality, topological sort, shortest path |
| GraphWriter (read/write) | `Sharc.Graph` | Intern, Link, Remove, Unlink |
| Cross-arc distributed sync | `Sharc.Arc` | Portable `.arc` fragments, delta sync, hash-chain verification |
| Multi-arc fusion | `Sharc.Arc` | Query across fragments with source provenance |
| Vector similarity search | `Sharc.Vector` | SIMD-accelerated cosine/euclidean, metadata pre-filter |
| Row-level entitlements | `Sharc` | Agent-scoped table/column access control, zero cost when off |
| GUID/UUID native type | `Sharc` | Zero-alloc merged Int64 pair or BLOB(16) encoding |
| FIX128 / DECIMAL128 | `Sharc` | 28-29 digit precision via canonical 16-byte payload |
| AES-256-GCM encryption | `Sharc.Crypto` | Argon2id KDF, HKDF-SHA256 |
| CSV → Arc ingestion | `Sharc.Arc` | `CsvArcImporter` for data import |
| Change event bus | `Sharc` | Subscribe to graph mutation events |

See [Integration Recipes](docs/INTEGRATION_RECIPES.md) for code examples of each capability.

---

## Headline Numbers

> BenchmarkDotNet v0.15.8 | .NET 10.0.2 | Windows 11 | Intel i7-11800H

### Core Engine (CreateReader API - zero-copy B-tree)

| Operation | Sharc | SQLite | Speedup | Sharc Alloc | SQLite Alloc |
| :--- | ---: | ---: | ---: | ---: | ---: |
| Point lookup (prepared) | **38.14 ns** | 23,226.52 ns | **609x** | **0 B** | 728 B |
| Batch 6 lookups | **626.14 ns** | 122,401.32 ns | **195x** | **0 B** | 3,712 B |
| Random lookup | **217.71 ns** | 23,415.67 ns | **108x** | **0 B** | 832 B |
| Engine load | **192.76 ns** | 22,663.42 ns | **118x** | 1,592 B | 1,160 B |
| Schema read | **2,199.88 ns** | 25,058.57 ns | **11.4x** | 5,032 B | 2,536 B |
| Sequential scan (5K rows) | **875.95 us** | 5,630.27 us | **6.4x** | 1,411,576 B | 1,412,320 B |
| WHERE filter | **261.73 us** | 541.54 us | **2.1x** | **0 B** | 720 B |
| NULL scan | **148.75 us** | 727.66 us | **4.9x** | **0 B** | 688 B |
| GC pressure scan | **156.31 us** | 766.46 us | **4.9x** | **0 B** | 688 B |
| Int index seek | **1.036 us** | 31.589 us | **30.5x** | 1,272 B | 872 B |
| Graph seek (single) | **7.071 us** | 70.553 us | **10.0x** | 888 B | 648 B |
| Graph seek (batch 6) | **14.767 us** | 203.713 us | **13.8x** | 3,224 B | 3,312 B |
| Graph BFS 2-hop | **45.59 us** | 205.67 us | **4.5x** | 800 B | 2,952 B |

### Query Pipeline (Query API - full SQL roundtrip)

> 2,500 rows/table. Compound queries use two tables with 500 overlapping rows.

| Operation | Sharc | SQLite | Speedup |
| :--- | ---: | ---: | ---: |
| `SELECT * FROM users_a` | **595.3 us** | 730.2 us | **1.23x** |
| `SELECT WHERE age > 30` | **900.4 us** | 1,085.8 us | **1.21x** |
| `UNION ALL (2x2500 rows)` | **2,431.0 us** | 2,873.7 us | **1.18x** |
| `UNION (dedup)` | 1,942.2 us | 1,940.6 us | ~1.00x |
| `GROUP BY + COUNT + AVG` | 1,706.0 us | **553.1 us** | 0.32x |
| `INTERSECT` | 3,316.1 us | **1,317.7 us** | 0.40x |
| `EXCEPT` | 3,413.6 us | **1,193.0 us** | 0.35x |

> Core engine read paths: 2.1x to 609x faster with hot paths at 0 B managed allocation. Query pipeline has optimization targets in sort-heavy and set-heavy shapes.

[**Full Benchmark Results**](docs/BENCHMARKS.md) | [**Run the Live Arena**](https://revred.github.io/Sharc.Open/)

---

## Why Sharc Exists

AI agents don't need a SQL engine -- they need targeted, trusted context. Sharc delivers:

1. **Precision Retrieval**: Point lookups in 38ns (609x faster) reduce token waste.
2. **Cryptographic Provenance**: A built-in trust layer verifies who contributed what data.
3. **Graph Reasoning**: O(log N) relationship traversal for context mapping.

---

## Documentation

| Guide | Description |
| :--- | :--- |
| [Getting Started](docs/GETTING_STARTED.md) | Zero to working code in 5 minutes |
| [API Quick Reference](docs/API_QUICK_REFERENCE.md) | The 10 operations you'll use most |
| [Integration Recipes](docs/INTEGRATION_RECIPES.md) | Copy-paste patterns for Blazor, AI agents, graph, encryption |
| [Benchmarks](docs/BENCHMARKS.md) | Full comparison with SQLite plus execution-tier breakdowns |
| [Architecture](docs/ARCHITECTURE.md) | How Sharc achieves zero-allocation reads |
| [Cookbook](docs/COOKBOOK.md) | 15 recipes for common patterns |
| [Vector Search](docs/VECTOR_SEARCH.md) | Embedding storage, similarity search, RAG patterns |
| [When NOT to Use](docs/WHEN_NOT_TO_USE.md) | Honest limitations |
| [FAQ](docs/FAQ.md) | Common questions answered |
| [Migration Guide](docs/MIGRATION.md) | Switching from Microsoft.Data.Sqlite |
| [API Wiki](wiki/Home.md) | Full API reference with copy-paste patterns |

## Build & Test

```bash
dotnet build                                            # Build everything
dotnet test                                             # Run all 3,682 tests
dotnet run -c Release --project bench/Sharc.Benchmarks  # Run benchmarks
```

## Release Rule

PRs into `main` are treated as release PRs and must include:

- `README.md` updates for user-facing package/API changes
- `CHANGELOG.md` release notes under `## [1.2.<PR_NUMBER>] - YYYY-MM-DD`
- NuGet package staging in `artifacts/nuget` (ignored folder) before publish

## Project Structure

```text
src/
  Sharc/                    Public API + Write Engine + Trust Layer
  Sharc.Core/             B-Tree, Records, Page I/O, Primitives
  Sharc.Query/              SQL pipeline: parser, compiler, executor
  Sharc.Crypto/             AES-256-GCM encryption, Argon2id KDF, HKDF-SHA256
  Sharc.Graph/              Graph engine: Cypher, PageRank, GraphWriter, algorithms
  Sharc.Graph.Surface/      Graph interfaces and models
  Sharc.Vector/             SIMD-accelerated vector similarity search
  Sharc.Arc/                Cross-arc: ArcUri, ArcResolver, ArcDiffer, fragment sync
  Sharc.Arena.Wasm/         Live benchmark arena (Blazor WASM)
tests/                      3,686 tests across 10 projects
  Sharc.Tests/              Core unit tests
  Sharc.IntegrationTests/   End-to-end tests
  Sharc.Query.Tests/        Query pipeline tests
  Sharc.Graph.Tests.Unit/   Graph + Cypher + algorithm tests
  Sharc.Graph.Tests.Perf/   Graph performance benchmarks
  Sharc.Arc.Tests/          Cross-arc diff + sync tests
  Sharc.Archive.Tests/      Archive tool tests
  Sharc.Vector.Tests/       Vector similarity tests
  Sharc.Repo.Tests/         Repository + MCP tool tests
  Sharc.Index.Tests/        Index CLI tests
  Sharc.Context.Tests/      MCP context tests
bench/
  Sharc.Benchmarks/         BenchmarkDotNet suite (Sharc vs SQLite)
  Sharc.Comparisons/        Graph + query benchmarks
samples/
  ApiComparison/            Sharc vs SQLite end-to-end timing comparison
  BasicRead/                Minimal read example
  BrowserOpfs/              Browser OPFS interop and storage portability patterns
  BulkInsert/               Transactional batch insert
  UpsertDeleteWhere/        Upsert and predicate delete workflows
  FilterAndProject/         Column projection + filtering
  PointLookup/              B-tree Seek performance demo
  VectorSearch/             Embedding storage and nearest-neighbor lookup
  EncryptedRead/            AES-256-GCM encrypted database read
  ContextGraph/             Graph traversal example
  TrustComplex/             Agent trust layer demo
  README.md                 Sample index and run instructions
  run-all.csx               C# script to build/run all samples
tools/
  Sharc.Archive/            Conversation archiver (schema + sync protocol)
  Sharc.Repo/               AI agent repository (annotations + decisions + MCP)
  Sharc.Context/            MCP Context Server
  Sharc.Index/              Git history → SQLite CLI
  Sharc.Debug/              Debug utilities
docs/                       Architecture, benchmarks, cookbook, FAQ, migration guides
PRC/                        Architecture decisions, specs, execution plans
```

## Current Limitations

- **Query pipeline materializes results** -- Cotes allocate managed arrays. Set operations use pooled IndexSet with ArrayPool storage (~1.4 KB)
- **Single-writer** -- one writer at a time; no WAL-mode concurrent writes
- **JOIN support** -- INNER, LEFT, RIGHT, FULL OUTER, and CROSS joins via hash join strategy
- **No virtual tables** -- FTS5, R-Tree not supported

Sharc is a **complement** to SQLite, not a replacement. See [When NOT to Use Sharc](docs/WHEN_NOT_TO_USE.md).

## License

MIT License. See [LICENSE](LICENSE) for details.

---

Crafted through AI-assisted engineering by **[Ram Kumar Revanur](https://www.linkedin.com/in/revodoc/)**.
