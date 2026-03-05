# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.PR-NUM] - 2026-02-26

### Fixed
- NuGet publish pipeline now packs and publishes `Sharc.Core` together with the `Sharc` meta package dependency chain.
- `CreateReader(..., SharcFilter[] filters)` now uses index planning for sargable legacy filters instead of always forcing a table scan.

### Changed
- NuGet package staging moved to `artifacts/nuget` (ignored folder) to avoid root-level package artifacts.
- Release PR policy formalized: PRs to `main` must include `README.md` and `CHANGELOG.md` updates.

## [1.0.0-beta.1] - 2026-02-16

### Core Engine
- **B-tree reader** with zero-copy `ReadOnlySpan<byte>` page traversal (61x faster seeks vs SQLite)
- **Sequential scans** with lazy column decode (4x faster, 568 B allocation for 2,500 rows)
- **Point lookups** via `Seek()` in < 1 microsecond (392 ns)
- **Batch seeks** — 6 consecutive seeks in 1,940 ns (66x faster)
- **Page I/O** — File, Memory, Memory-mapped, and Cached page sources
- **LRU page cache** with configurable capacity (default 2,000 pages)
- **WHERE filtering** via `SharcFilter` and closure-compiled `FilterStar` predicates
- **Column projection** — skip decoding unwanted columns
- **WAL read support** — read databases with active WAL files
- **Overflow page assembly** using `ArrayPool<byte>` (zero steady-state allocation)

### SQL Query Pipeline
- **Full SQL parser and compiler** for SELECT statements
- **WHERE** with comparison, LIKE, IN, BETWEEN, IS NULL operators
- **ORDER BY** with multi-column sort and streaming TopN heap
- **GROUP BY** with COUNT, SUM, AVG, MIN, MAX aggregates (streaming hash aggregator)
- **LIMIT / OFFSET** with streaming execution
- **UNION ALL / UNION / INTERSECT / EXCEPT** with pooled open-addressing hash maps
- **Cotes** (Common Table Expressions) with lazy evaluation
- **Parameterized queries** via `$param` syntax
- **Query plan caching** for repeated execution

### Write Engine
- **INSERT** with B-tree page splits and cell overflow
- **ACID transactions** with rollback journal
- **RecordEncoder** and **CellBuilder** for SQLite-compatible record encoding

### Encryption (Sharc.Crypto)
- **AES-256-GCM** page-level encryption
- **Argon2id** key derivation (memory-hard, GPU-resistant)
- **SharcKeyHandle** for secure key lifecycle management

### Graph Storage (Sharc.Graph)
- **ConceptStore** and **RelationStore** with B-tree backing
- **SeekFirst** O(log N) cursor for graph traversal
- **2-hop BFS** in 2.6 us (31x faster than SQLite-based traversal via zero-alloc cursor BFS)
- **Node seek** in 1,475 ns (14.5x faster)

### Trust Layer
- **AgentRegistry** — ECDSA self-attestation for AI agent identity
- **LedgerManager** — hash-chain tamper-evident audit log
- **ReputationEngine** — trust scoring based on agent behavior
- **Co-signatures** — multi-party authorization
- **Governance** — configurable trust policies

### Infrastructure
- **2,669 tests** across 6 test projects (unit, integration, query, graph, index, context)
- **Live Arena** — Blazor WASM benchmark app at [revred.github.io/Sharc](https://revred.github.io/Sharc.Open/)
- **BenchmarkDotNet suite** — Sharc vs SQLite comparisons
- **MCP Context Server** — Model Context Protocol integration
- **CI/CD** — GitHub Actions (build, test, publish, deploy)
- **SourceLink** — debugger support for NuGet packages
- **Central package management** via Directory.Packages.props
- **.NET 10.0** target with trimming and AOT compatibility
