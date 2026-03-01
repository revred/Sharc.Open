# Writing Data

## SharcWriter

`SharcWriter` provides typed write operations with ACID transaction support.

### Opening for Writes

```csharp
using var writer = SharcWriter.Open("mydata.db");
```

Or wrap an existing database:

```csharp
var options = new SharcOpenOptions { Writable = true };
using var db = SharcDatabase.Open("mydata.db", options);
using var writer = SharcWriter.From(db);
```

### Insert (Auto-Commit)

Each call is a single atomic transaction:

```csharp
long rowId = writer.Insert("users",
    ColumnValue.FromInt64(1, 42),                    // integer column
    ColumnValue.Text(13, "Alice"u8.ToArray()),       // text column
    ColumnValue.Text(13, "alice@example.com"u8.ToArray())
);
// rowId is the assigned rowid
```

### Batch Insert

Multiple records in a single transaction (much faster than individual inserts):

```csharp
var records = new[]
{
    new[] { ColumnValue.FromInt64(1, 1), ColumnValue.Text(13, "Alice"u8.ToArray()) },
    new[] { ColumnValue.FromInt64(1, 2), ColumnValue.Text(13, "Bob"u8.ToArray()) },
    new[] { ColumnValue.FromInt64(1, 3), ColumnValue.Text(13, "Carol"u8.ToArray()) },
};

long[] rowIds = writer.InsertBatch("users", records);
```

### Update

```csharp
bool updated = writer.Update("users", rowId,
    ColumnValue.FromInt64(1, 42),
    ColumnValue.Text(13, "Alice Updated"u8.ToArray()),
    ColumnValue.Text(13, "new@example.com"u8.ToArray())
);
```

### Delete

```csharp
bool deleted = writer.Delete("users", rowId);
```

## Explicit Transactions

Group multiple operations into a single atomic unit:

```csharp
using var writer = SharcWriter.Open("mydata.db");
using var tx = writer.BeginTransaction();

long id1 = tx.Insert("users", /* values */);
long id2 = tx.Insert("logs", /* values */);
tx.Delete("temp", oldId);

tx.Commit();  // All-or-nothing
// If Dispose() is called without Commit(), changes are rolled back
```

## Agent-Scoped Writes

Enforce write entitlements:

```csharp
// Agent must have WriteScope that includes "users" table
long rowId = writer.Insert(agent, "users", /* values */);

// Or with transactions
using var tx = writer.BeginTransaction(agent);
tx.Insert("users", /* values */);   // Enforced per-operation
tx.Commit();
```

Throws `UnauthorizedAccessException` if the agent's `WriteScope` denies access to the table or columns.

## ColumnValue

Construct values for inserts and updates:

| Factory Method | Description |
|---------------|-------------|
| `ColumnValue.Null()` | SQL NULL |
| `ColumnValue.FromInt64(serialType, value)` | Integer (serial type: 1, 2, 3, 4, 6, or 8) |
| `ColumnValue.FromDouble(serialType, value)` | Floating point (serial type: 7) |
| `ColumnValue.Text(serialType, utf8Bytes)` | UTF-8 text (serial type: odd >= 13) |
| `ColumnValue.Blob(serialType, bytes)` | Binary data (serial type: even >= 12) |
| `ColumnValue.FromGuid(guid)` | GUID/UUID value (BLOB(16) or merged `__hi`/`__lo`) |
| `ColumnValue.FromDecimal(value)` | FIX128/DECIMAL128 value (exact 28-29 digits; canonical or merged `__dhi`/`__dlo`) |

> **Serial type hint:** For text, use `13` for short strings. For blobs, use `12`. Sharc computes the actual serial type from the data length.

### 128-bit Schema Conventions

```sql
CREATE TABLE docs (
    id INTEGER PRIMARY KEY,
    doc_guid UUID NOT NULL,
    amount FIX128 NOT NULL
);

CREATE TABLE merged_values (
    id INTEGER PRIMARY KEY,
    owner__hi INTEGER NOT NULL,
    owner__lo INTEGER NOT NULL,
    total__dhi INTEGER NOT NULL,
    total__dlo INTEGER NOT NULL
);
```

## Upsert (Insert or Replace)

Insert a row or replace it if the rowid already exists:

```csharp
// Insert if rowid 42 doesn't exist, replace if it does
long rowId = writer.Upsert("users", 42,
    ColumnValue.FromInt64(1, 42),
    ColumnValue.Text(13, "Alice Updated"u8.ToArray()),
    ColumnValue.Text(13, "alice@example.com"u8.ToArray())
);
```

## Bulk Delete with Filter

Delete all rows matching a predicate:

```csharp
// Delete all inactive users
int deletedCount = writer.DeleteWhere("users",
    FilterStar.Column("active").Eq(0L));

Console.WriteLine($"Removed {deletedCount} inactive users");
```

## PreparedWriter

Zero-overhead repeated writes with thread-safe cursor reuse:

```csharp
using var prepared = writer.PrepareWriter("users");

// Insert multiple rows — cursor is reused, no per-call allocation
long id1 = prepared.Insert(
    ColumnValue.FromInt64(1, 1),
    ColumnValue.Text(13, "Alice"u8.ToArray()));

long id2 = prepared.Insert(
    ColumnValue.FromInt64(1, 2),
    ColumnValue.Text(13, "Bob"u8.ToArray()));

// Update and delete also supported
prepared.Update(id1,
    ColumnValue.FromInt64(1, 1),
    ColumnValue.Text(13, "Alice V2"u8.ToArray()));

prepared.Delete(id2);
```

## CREATE INDEX

Create indexes on existing tables with data (Phase 2 — v1.2.77+):

```csharp
writer.CreateIndex("idx_users_email", "users", "email");
writer.CreateIndex("idx_users_age", "users", "age", unique: false);
```

Uses `IndexBTreePopulator` for sorted bulk-insert with proper SQLite type ordering.

## Vacuum

Compact the database by reclaiming free pages:

```csharp
writer.Vacuum();
```

## Reading from a Writer

Access the underlying database for read operations:

```csharp
using var writer = SharcWriter.Open("mydata.db");
using var reader = writer.Database.CreateReader("users");
```
