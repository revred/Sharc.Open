# Release History

All published NuGet releases of Sharc.

## [1.2.80] - 2026-02-27

**PR #80** — Dev merge to main (release packaging)

### Changes
- Release packaging for v1.2.77 content to NuGet

---

## [1.2.77] - 2026-02-27

**PR #79** — Security hardening, CREATE INDEX Phase 2, context engine, knowledge graph

### Security & Robustness
- Bounds checks, overflow guards, disposal safety, and input validation across Core, Graph, Vector, and API layers
- 7 new hardening/robustness test files

### CREATE INDEX on Non-Empty Tables (Phase 2)
- `IndexBTreePopulator`: sorted bulk-insert with proper SQLite type ordering
- `CellBuilder`: index interior cell support (page type 0x02)
- `BTreePageRewriter`: index leaf/interior page builders

### Context Engine DI Seams
- `IContextRanker` + `IImpactAnalyzer` interfaces in `Sharc.Core.Context`
- `KeywordContextRanker` + `DirectDependencyAnalyzer` naive implementations in `Sharc.Context`
- `SchemaCache` for opportunistic schema caching on `OpenMemory`

### Knowledge Graph (Sharc.Repo)
- `KnowledgeReader`/`KnowledgeWriter`: CRUD for features, edges, file purposes, dependencies
- `CodebaseScanner`: 4-phase scan (features, purposes, edges, deps)
- `FeatureCommand`, `GapsCommand`, `ScanCommand` CLI commands
- `RepoKnowledgeTool` MCP endpoint
- `DocScanner` + `FeatureCatalog` for automated knowledge extraction
- 16 new test files covering all knowledge graph operations

---

## [1.2.65] - 2026-02-26

**PRs #60–#65** — Vector.Next, Arc.Polish, Zero.Hash

### Vector.Next (PR #64)
- Ship vector-next planner with `VectorQuery` builder API
- `FIX128` / `DECIMAL128` typed column support (28-29 digit precision)
- HNSW perf gate stabilization for CI

### Arc.Polish (PR #62)
- LRU cache eviction fixes
- Documentation truth sync across all packages

### Zero.Hash (PR #60)
- `DestructiveProbe` rename for clarity
- Shape benchmarks and kernel micro-benchmarks
- Provenance hardening

### Infrastructure
- Fix NuGet publish order to avoid broken `Sharc` restores

---

## [1.2.59] - 2026-02-25

**PR #59** — CI/CD and build quality

### Changes
- PR-number versioning scheme (`1.2.<PR_NUMBER>`)
- Merge-to-main NuGet publish automation
- Resolve all 31 build warnings (treat warnings as errors)

---

## [1.1.6-rc1] - 2026-02-24

### Vector Search (Sharc.Vector)
- WP1-6 TDD slice for HNSW vector index
- SIMD-accelerated distance functions (cosine, euclidean, dot product)
- `HnswIndex<T>` with Add, Search, Update, Delete, Compact
- `HybridQuery` — RRF fusion of vector + text relevance
- Trust-layer integration for vector search

### V-Series Optimizations
- Hot-path optimizations across B-tree and record layers

---

## [1.1.5-beta] - 2026-02-23

### Security
- SEC-001/002/003 fixes
- Entitlement hardening

### Arc Extensions
- Excel, Google Sheets, MindMap, Gantt importers
- Lossless Excel capture

### Features
- `FusedArcContext` — query across multiple `.arc` fragments
- `CsvArcImporter` — CSV to `.arc` import
- Row-level entitlements (table/column/wildcard enforcement)

---

## [1.1.2-beta] - 2026-02-21

### Graph Engine (Sharc.Graph)
- Cypher query language (tokenizer → parser → compiler → executor)
- Graph algorithms: PageRank, degree centrality, topological sort, shortest path
- `GraphWriter` — Intern, Link, Remove, Unlink
- `ShortestPath`, `GetContext` methods
- Cross-arc: `ArcDiffer`, `ArcResolver`, `FragmentSyncProtocol`

### Write Engine Enhancements
- UPDATE and DELETE operations
- Upsert (`INSERT OR REPLACE`) support
- `DeleteWhere` with filter predicates

### Trust Layer
- Agent registry with ECDSA self-attestation
- Hash-chain tamper-evident ledger
- Reputation scoring
- Co-signatures and governance

---

## [1.0.0-beta.1] - 2026-02-16

### Initial Release
- B-tree reader with zero-copy `ReadOnlySpan<byte>` page traversal
- Sequential scans with lazy column decode
- Point lookups via `Seek()` (< 1 microsecond)
- Page I/O: File, Memory, Mmap, Cached
- WHERE filtering via `SharcFilter` and `FilterStar`
- Column projection (skip unwanted columns)
- WAL read support
- Full SQL parser and compiler (SELECT, WHERE, ORDER BY, GROUP BY, LIMIT, UNION, Cotes)
- Write engine (INSERT with B-tree splits, ACID transactions)
- AES-256-GCM encryption with Argon2id KDF
- Graph storage with ConceptStore/RelationStore
- Agent trust layer (ECDSA attestation, hash-chain ledger)
- 2,669 tests across 6 test projects
- Live Arena (Blazor WASM benchmark app)
- MCP Context Server

---

## Version Scheme

Sharc uses `1.2.<PR_NUMBER>` versioning: each PR merged to `main` produces a NuGet release. The PR number serves as the patch version, providing a direct link between code changes and published packages.

## NuGet Packages

| Package | Description |
|:---|:---|
| [`Sharc`](https://www.nuget.org/packages/Sharc/) | Core read/write engine |
| [`Sharc.Core`](https://www.nuget.org/packages/Sharc.Core/) | B-tree, records, page I/O primitives |
| [`Sharc.Query`](https://www.nuget.org/packages/Sharc.Query/) | SQL pipeline (parser, compiler, executor) |
| [`Sharc.Crypto`](https://www.nuget.org/packages/Sharc.Crypto/) | AES-256-GCM encryption |
| [`Sharc.Graph`](https://www.nuget.org/packages/Sharc.Graph/) | Graph engine + Cypher + algorithms |
| [`Sharc.Graph.Surface`](https://www.nuget.org/packages/Sharc.Graph.Surface/) | Graph interfaces and models |
| [`Sharc.Vector`](https://www.nuget.org/packages/Sharc.Vector/) | SIMD-accelerated vector search |
| [`Sharc.Arc`](https://www.nuget.org/packages/Sharc.Arc/) | Cross-arc diff, sync, fragments |
