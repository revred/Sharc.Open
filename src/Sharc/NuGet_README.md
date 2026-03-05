# Sharc

**Read and write SQLite files 2-66x faster than Microsoft.Data.Sqlite, in pure C#, with zero native dependencies.**

Sharc is a high-performance, zero-allocation SQLite format reader and writer with a built-in SQL query pipeline, designed for AI context space engineering, edge computing, and high-throughput data access.

## Key Features

- **61x Faster Seeks**: B-tree point lookups in 392 ns vs SQLite's 24,011 ns.
- **9.2x Faster Scans**: Full table reads with lazy column decode (568 B allocation for 2,500 rows).
- **SQL Pipeline**: SELECT, WHERE, ORDER BY, GROUP BY, UNION, INTERSECT, EXCEPT, Cotes.
- **Zero Allocation**: Hot paths use `ReadOnlySpan<byte>` and `stackalloc` — zero GC pressure.
- **Pure C#**: No native DLLs. Runs anywhere .NET runs (Windows, Linux, macOS, WASM, IoT).
- **~250 KB Footprint**: 40x smaller than the standard SQLite bundle.
- **Cryptographic Trust**: ECDSA agent attestation and tamper-evident audit ledgers (via `Sharc.Graph`).
- **Encryption**: AES-256-GCM + Argon2id support (via `Sharc.Crypto`).
- **Native 128-bit Types**: `GUID`/`UUID` and `FIX128`/`DECIMAL128` (`decimal`, 28-29 significant digits).
- **Strict Typed Accessors**: `GetGuid()` only for `GUID`/`UUID`; `GetDecimal()` only for `FIX128`/`DECIMAL*`.

## Quick Start

```csharp
using Sharc;

// Open a database
using var db = SharcDatabase.Open("mydata.db");

// Scan a table
using var reader = db.CreateReader("users");
while (reader.Read())
    Console.WriteLine($"{reader.GetInt64(0)}: {reader.GetString(1)}");

// Point lookup in < 1 microsecond
if (reader.Seek(42))
    Console.WriteLine($"Found: {reader.GetString(1)}");

// SQL queries
using var results = db.Query(
    "SELECT dept, COUNT(*) FROM users WHERE age > 25 GROUP BY dept ORDER BY dept");
while (results.Read())
    Console.WriteLine($"{results.GetString(0)}: {results.GetInt64(1)}");

// Typed 128-bit values
using var typed = db.CreateReader("accounts");
while (typed.Read())
{
    Guid accountId = typed.GetGuid(1);   // GUID/UUID column
    decimal balance = typed.GetDecimal(2); // FIX128/DECIMAL* column
}
```

## When to Use Sharc

Sharc is optimized for **point lookups**, **structured AI context**, and **embedded scenarios**. It is a complement to SQLite, not a full replacement. Use Sharc when performance, allocation, or zero-dependency deployment is critical.

[Full Documentation & Benchmarks](https://github.com/revred/Sharc) | [Live Arena](https://revred.github.io/Sharc.Open/)
