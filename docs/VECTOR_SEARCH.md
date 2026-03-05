# Vector Search with Sharc

> Sharc supports vector similarity search over embeddings stored as BLOB columns in
> standard SQLite tables. The vector layer is SIMD-accelerated, zero-copy, and uses
> the same JitQuery infrastructure for metadata pre-filtering and cursor reuse.

---

## How It Works

Vectors (float arrays from any embedding model) are stored as BLOB columns in regular
SQLite tables. Sharc reads the BLOB bytes directly from cached B-tree pages, reinterprets
them as `ReadOnlySpan<float>` with zero copy, and feeds them into SIMD-accelerated distance
functions. The entire hot path — from page cache to distance score — allocates 0 bytes.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Sharc Vector Pipeline                        │
│                                                                      │
│  Embedding Model (OpenAI, MiniLM, etc.)                              │
│       │                                                              │
│       ▼                                                              │
│  float[] → BlobVectorCodec.Encode() → BLOB column in SQLite table    │
│                                                                      │
│  Query time:                                                         │
│  GetBlobSpan() → MemoryMarshal.Cast → TensorPrimitives.Cosine()     │
│  ────────────── zero copy ────────── SIMD (AVX2/512) ──────────     │
│                                                                      │
│  Result: top-K (rowid, distance) pairs                               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Store Embeddings

Embeddings from any model (OpenAI, Sentence Transformers, Cohere, etc.) are stored
as BLOB columns. The table is a regular SQLite table — any SQLite tool can read it.

```sql
-- Create table (via SQLite or SharcWriter)
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT,
    category TEXT,
    embedding BLOB NOT NULL    -- float[] encoded as IEEE 754 LE bytes
);
```

```csharp
// Insert with Sharc
using var writer = SharcWriter.Open("knowledge.db");
using var tx = writer.BeginTransaction();

float[] embedding = embeddingModel.Encode("Quantum computers use qubits...");
byte[] blob = BlobVectorCodec.Encode(embedding);

tx.Insert("documents",
    ColumnValue.FromInt64(4, 1),
    ColumnValue.Text(/* serial type */, Encoding.UTF8.GetBytes("Intro to Quantum Computing")),
    ColumnValue.Text(/* serial type */, Encoding.UTF8.GetBytes("Quantum computers use qubits...")),
    ColumnValue.Text(/* serial type */, Encoding.UTF8.GetBytes("science")),
    ColumnValue.Blob(blob));

tx.Commit();
```

### 2. Search by Similarity

```csharp
using var db = SharcDatabase.Open("knowledge.db");
using var vq = db.Vector("documents", "embedding", DistanceMetric.Cosine);

// Find 10 most similar documents
float[] query = embeddingModel.Encode("How do quantum computers work?");
var results = vq.NearestTo(query, k: 10, "title", "category");

foreach (var match in results.Matches)
    Console.WriteLine($"[{match.Distance:F3}] Row {match.RowId}");
```

### 3. Pre-Filter by Metadata

Apply WHERE-style filters **before** distance computation to narrow the search space.
Only rows passing the metadata filter are distance-computed — saving CPU on irrelevant rows.

```csharp
using var vq = db.Vector("documents", "embedding", DistanceMetric.Cosine);

// Only search science documents
vq.Where(FilterStar.Column("category").Eq("science"));
var results = vq.NearestTo(queryVec, k: 5);
```

### 4. Threshold Search (Within Distance)

```csharp
// Find all documents within cosine distance 0.3
var results = vq.WithinDistance(queryVec, maxDistance: 0.3f, "title");
```

---

## Distance Metrics

| Metric | Best For | Range | Lower = More Similar? |
| :--- | :--- | :--- | :--- |
| **Cosine** | Text embeddings (OpenAI, MiniLM) | [0, 2] | Yes |
| **Euclidean** | Spatial/geometric data | [0, +∞) | Yes |
| **DotProduct** | Normalized vectors, recommendations | (-∞, +∞) | No (higher = more similar) |

All metrics delegate to `System.Numerics.Tensors.TensorPrimitives` for SIMD-accelerated
computation with automatic hardware dispatch (SSE2 → AVX2 → AVX-512).

---

## Compared to Dedicated Vector Databases

| Dimension | Sharc.Vector | Pinecone / Weaviate | pgvector | ChromaDB |
| :--- | :--- | :--- | :--- | :--- |
| **Deployment** | In-process (~250 KB) | Cloud / server | PostgreSQL extension | Server (Python) |
| **Index type** | Flat scan + HNSW | HNSW + quantization | IVFFlat / HNSW | HNSW |
| **Latency (10K vectors)** | ~5-10 ms | ~10-50 ms + network | ~5-20 ms | ~20-50 ms |
| **Scale** | ≤100K vectors | Billions | Millions | Millions |
| **Metadata filter** | FilterStar (compiled) | Built-in | SQL WHERE | Built-in |
| **Storage format** | Standard SQLite | Proprietary | PostgreSQL | Proprietary |
| **Dependencies** | 1 (TensorPrimitives) | Cloud SDK | PostgreSQL | Python runtime |
| **WASM** | Yes (~40 KB) | No | No | No |
| **Cost** | Free (MIT) | $$$/month | Free (self-hosted) | Free (self-hosted) |

### When to Use Sharc.Vector

- **≤1M vectors** — HNSW index for ANN search, flat scan fallback for small datasets
- **Embedded/edge/mobile** — no server process, no network round-trip
- **RAG with metadata** — pre-filter by category/date/author before similarity search
- **AI agent context** — agent memory store with semantic retrieval
- **WASM deployment** — vector search in the browser (~40 KB)
- **Zero infrastructure** — no Pinecone bill, no PostgreSQL setup

### When to Use a Dedicated Vector DB

- **>1M vectors** — quantization and distributed sharding needed at scale
- **Real-time index updates** — continuous embedding ingestion at high throughput
- **Multi-tenant** — isolation, access control, per-tenant quotas
- **Distributed** — data spans multiple nodes with replication

---

## Performance Expectations

Flat scan performance on a single core (no parallelism):

| Dataset Size | Dimensions | Metric | Expected Latency |
| :--- | :--- | :--- | :--- |
| 1K vectors | 384 (MiniLM) | Cosine | ~0.5-1 ms |
| 10K vectors | 384 | Cosine | ~5-10 ms |
| 10K vectors | 1536 (OpenAI) | Cosine | ~15-30 ms |
| 100K vectors | 384 | Cosine | ~50-100 ms |
| 1M vectors | 384 | Cosine | ~500 ms-1s |

**Allocation per search:** ~1.5 KB (top-K heap + result list). Per-row distance: 0 B.

---

## Architecture

The vector layer composes on top of existing Sharc infrastructure:

```
Sharc.Vector (new)          Sharc (existing)
─────────────────           ──────────────────
VectorQuery                 JitQuery (scan + filter)
BlobVectorCodec             GetBlobSpan() (zero-copy BLOB access)
VectorDistanceFunctions     TensorPrimitives (SIMD distance)
VectorTopKHeap              N/A (new, ~60 lines)
VectorCache                 CachedPageSource (same LRU pattern)
```

The vector column is stored as a regular BLOB in a regular SQLite table.
No schema extensions, no custom page formats, no magic. Any SQLite tool
(DB Browser, sqlite3 CLI, Microsoft.Data.Sqlite) can still open and query the file.

---

## Common Patterns

### RAG (Retrieval-Augmented Generation)

```csharp
// 1. Encode the user's question
float[] queryVec = embeddingModel.Encode(userQuestion);

// 2. Find relevant documents
using var vq = db.Vector("documents", "embedding");
var results = vq.NearestTo(queryVec, k: 5, "content");

// 3. Build context for LLM
var context = string.Join("\n\n", results.Matches.Select(m => {
    using var r = db.CreateReader("documents", "content");
    r.Seek(m.RowId);
    return r.GetString(0);
}));

// 4. Generate answer
string answer = llm.Complete($"Context:\n{context}\n\nQuestion: {userQuestion}");
```

### Semantic Cache

```csharp
// Cache LLM responses by semantic similarity
using var vq = db.Vector("llm_cache", "question_embedding", DistanceMetric.Cosine);

float[] questionVec = embeddingModel.Encode(userQuestion);
var cached = vq.NearestTo(questionVec, k: 1);

if (cached.Count > 0 && cached[0].Distance < 0.05f)
{
    // Cache hit — return stored response
    using var r = db.CreateReader("llm_cache", "response");
    r.Seek(cached[0].RowId);
    return r.GetString(0);
}

// Cache miss — generate and store
string response = llm.Complete(userQuestion);
// ... insert (userQuestion, questionVec, response) into llm_cache
return response;
```

### Agent Memory with Semantic Retrieval

```csharp
// AI agent stores observations with embeddings
using var vq = db.Vector("agent_memory", "embedding");
vq.Where(FilterStar.Column("agent_id").Eq(agentId));
vq.Where(FilterStar.Column("timestamp").Gt(cutoffTime));

float[] contextVec = embeddingModel.Encode(currentTask);
var relevant = vq.NearestTo(contextVec, k: 10, "observation", "timestamp");

// Use retrieved memories as context for agent's next action
```

---

## Encoding Guide

### Supported Embedding Models

Any model that outputs `float[]` works. The BLOB format is IEEE 754 little-endian:

| Model | Dimensions | BLOB Size | Notes |
| :--- | :--- | :--- | :--- |
| `all-MiniLM-L6-v2` | 384 | 1,536 B | Fast, good for semantic search |
| `text-embedding-3-small` (OpenAI) | 1536 | 6,144 B | High quality, reasonable size |
| `text-embedding-3-large` (OpenAI) | 3072 | 12,288 B | Highest quality |
| `nomic-embed-text` | 768 | 3,072 B | Open source, good balance |
| `BGE-small-en-v1.5` | 384 | 1,536 B | Open source, fast |

### Encoding/Decoding

```csharp
// Encode: float[] → byte[] (for INSERT)
float[] embedding = model.Encode("Hello world");
byte[] blob = BlobVectorCodec.Encode(embedding);

// Decode: byte[] → float[] (for distance computation, done automatically by VectorQuery)
ReadOnlySpan<float> decoded = BlobVectorCodec.Decode(blobBytes);

// Get dimensions from BLOB size
int dims = BlobVectorCodec.GetDimensions(blobBytes.Length); // 1536 bytes → 384 dims
```

---

## Limitations

1. **HNSW index** — built-in HNSW for ANN search (no IVF/PQ). Flat scan for small datasets; HNSW for larger ones.
2. **Single-threaded search** — no parallel distance computation (yet).
3. **Incremental mutations** — HNSW supports Upsert/Delete with periodic MergePendingMutations.
4. **Float32 only** — no float16/int8 quantization (yet).
5. **Separate project** — requires `Sharc.Vector` package (adds `System.Numerics.Tensors` dependency).

These limitations are by design — Sharc.Vector targets embedded/edge workloads where
simplicity and zero infrastructure matter more than billion-vector scale.
