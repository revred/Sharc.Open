# Reading Data

## SharcDataReader

`SharcDataReader` is a forward-only, read-only cursor over table rows. It decodes records directly from B-tree page bytes with zero intermediate copies.

### Basic Table Scan

```csharp
using var db = SharcDatabase.Open("mydata.db");
using var reader = db.CreateReader("users");

while (reader.Read())
{
    long id = reader.GetInt64(0);
    string name = reader.GetString(1);
    string email = reader.GetString(2);
}
```

### Column Projection

Decode only the columns you need. Unneeded columns are skipped at the byte level:

```csharp
// Only decode "id" and "email" — other columns are not materialized
using var reader = db.CreateReader("users", "id", "email");

while (reader.Read())
{
    long id = reader.GetInt64(0);       // "id" is ordinal 0
    string email = reader.GetString(1); // "email" is ordinal 1
}
```

### Point Lookup (Seek)

O(log N) binary search directly on the B-tree. Sub-microsecond for databases up to millions of rows:

```csharp
using var reader = db.CreateReader("users");

if (reader.Seek(42))   // Find rowid 42
{
    string name = reader.GetString(1);
}
```

### Index-Based Seek

```csharp
using var reader = db.CreateReader("users");

if (reader.SeekIndex("idx_users_email", "alice@example.com"))
{
    long id = reader.GetInt64(0);
}
```

### Row Count

```csharp
long count = db.GetRowCount("users");
```

## Typed Accessors

Use typed getters for zero-boxing access:

| Method | Return Type | Description |
|--------|-------------|-------------|
| `GetInt64(ordinal)` | `long` | 64-bit signed integer |
| `GetInt32(ordinal)` | `int` | 32-bit signed integer |
| `GetDouble(ordinal)` | `double` | IEEE 754 64-bit float |
| `GetString(ordinal)` | `string` | UTF-8 decoded string |
| `GetBlob(ordinal)` | `byte[]` | Binary data (allocates) |
| `GetBlobSpan(ordinal)` | `ReadOnlySpan<byte>` | Binary data (zero-copy) |
| `GetUtf8Span(ordinal)` | `ReadOnlySpan<byte>` | Raw UTF-8 bytes (zero-copy) |
| `GetGuid(ordinal)` | `Guid` | Strict: column must be declared `GUID`/`UUID`; supports BLOB(16) or merged `__hi`/`__lo` |
| `GetDecimal(ordinal)` | `decimal` | Strict: column must be declared `FIX128`/`DECIMAL128`/`DECIMAL`; 28-29 digits |
| `IsNull(ordinal)` | `bool` | Check for NULL |
| `GetValue(ordinal)` | `object` | Boxed value (slower, for dynamic access) |
| `GetColumnType(ordinal)` | `SharcColumnType` | Runtime type of current value |

### 128-bit Column Conventions

- GUID logical merge: `name__hi` + `name__lo`
- Decimal logical merge: `name__dhi` + `name__dlo`

### Zero-Copy Access

For performance-critical paths, use span-based accessors to avoid allocation:

```csharp
while (reader.Read())
{
    ReadOnlySpan<byte> utf8Name = reader.GetUtf8Span(1);   // No string allocation
    ReadOnlySpan<byte> blobData = reader.GetBlobSpan(2);    // No byte[] allocation
}
```

## PreparedReader

Zero-allocation cursor reuse for repeated point lookups:

```csharp
using var prepared = db.PrepareReader("users");

// Seek by primary key — reuses the same cursor, zero allocation per seek
if (prepared.Seek(42))
    Console.WriteLine(prepared.GetString(1));

if (prepared.Seek(99))
    Console.WriteLine(prepared.GetString(1));
```

With column projection:

```csharp
using var prepared = db.PrepareReader("users", "id", "name");
```

## Pagination (AfterRowId)

Resume a scan after a specific rowid for efficient pagination:

```csharp
using var reader = db.CreateReader("users");
long lastRowId = 0;
int pageSize = 50;

// Page 1
reader.AfterRowId(lastRowId);
int count = 0;
while (reader.Read() && count < pageSize)
{
    lastRowId = reader.RowId;
    count++;
}

// Page 2 — continues from where page 1 left off
reader.AfterRowId(lastRowId);
count = 0;
while (reader.Read() && count < pageSize)
{
    lastRowId = reader.RowId;
    count++;
}
```

## RowId Property

Access the current row's SQLite rowid:

```csharp
while (reader.Read())
{
    long rowId = reader.RowId;
    Console.WriteLine($"Row {rowId}: {reader.GetString(1)}");
}
```

## Column Metadata

```csharp
int fieldCount = reader.FieldCount;
string colName = reader.GetColumnName(0);
int ordinal = reader.GetOrdinal("email");  // Case-insensitive lookup
```

## SharcColumnType

```csharp
public enum SharcColumnType
{
    Null = 0,       // SQL NULL
    Integral = 1,   // Signed integer (1, 2, 3, 4, 6, or 8 bytes)
    Real = 2,       // IEEE 754 64-bit float
    Text = 3,       // UTF-8 string
    Blob = 4        // Binary large object
}
```

## Filtered Scans

Apply predicates that evaluate directly on raw page bytes:

```csharp
using var reader = db.CreateReader("users",
    new SharcFilter("age", SharcOperator.GreaterOrEqual, 18L),
    new SharcFilter("status", SharcOperator.Equal, "active"));

while (reader.Read())
{
    // Only rows where age >= 18 AND status = 'active'
}
```

### FilterStar (Advanced)

For complex filter expressions:

```csharp
var filter = FilterStar.And(
    FilterStar.Column("age").Gte(18L),
    FilterStar.Column("status").Eq("active")
);

using var reader = db.CreateReader("users", filter);
```

### SharcOperator

| Operator | SQL Equivalent |
|----------|---------------|
| `Equal` | `=` |
| `NotEqual` | `!=` |
| `LessThan` | `<` |
| `GreaterThan` | `>` |
| `LessOrEqual` | `<=` |
| `GreaterOrEqual` | `>=` |
