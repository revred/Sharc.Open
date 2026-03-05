// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.


/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Licensed under the MIT License — free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using Sharc.Core;
using Sharc.IntegrationTests.Helpers;
using Xunit;

#pragma warning disable CA1859 // Intentionally testing interface polymorphism

namespace Sharc.IntegrationTests;

public sealed class PreparedWriterTests : IDisposable
{
    private readonly string _dbPath;

    public PreparedWriterTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"prepared_writer_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); }
        catch { /* best-effort cleanup */ }
    }

    private SharcWriter CreateWritableWriter(int seedRows = 0)
    {
        var data = TestDatabaseFactory.CreateUsersDatabase(seedRows);
        File.WriteAllBytes(_dbPath, data);
        var db = SharcDatabase.Open(_dbPath, new SharcOpenOptions { Writable = true });
        return SharcWriter.From(db);
    }

    // ─── Lifecycle ────────────────────────────────────────────────

    [Fact]
    public void PrepareWriter_ReturnsNonNull()
    {
        using var writer = CreateWritableWriter();
        using var prepared = writer.PrepareWriter("users");

        Assert.NotNull(prepared);
    }

    [Fact]
    public void PrepareWriter_ImplementsIPreparedWriter()
    {
        using var writer = CreateWritableWriter();
        using var prepared = writer.PrepareWriter("users");

        Assert.IsAssignableFrom<IPreparedWriter>(prepared);
    }

    [Fact]
    public void Dispose_DoesNotThrow()
    {
        using var writer = CreateWritableWriter();
        var prepared = writer.PrepareWriter("users");
        prepared.Dispose();
    }

    [Fact]
    public void Dispose_Twice_DoesNotThrow()
    {
        using var writer = CreateWritableWriter();
        var prepared = writer.PrepareWriter("users");
        prepared.Dispose();
        prepared.Dispose(); // second dispose is no-op
    }

    // ─── Insert ──────────────────────────────────────────────────

    [Fact]
    public void Insert_ReturnsRowId()
    {
        using var writer = CreateWritableWriter();
        using var prepared = writer.PrepareWriter("users");

        long rowId = prepared.Insert(
            ColumnValue.FromInt64(1, 1),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("Alice")),
            ColumnValue.FromInt64(2, 30),
            ColumnValue.FromDouble(100.0),
            ColumnValue.Null());

        Assert.True(rowId > 0);
    }

    [Fact]
    public void Insert_DataReadableAfterInsert()
    {
        using var writer = CreateWritableWriter();
        using var prepared = writer.PrepareWriter("users");

        prepared.Insert(
            ColumnValue.FromInt64(1, 1),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("TestUser")),
            ColumnValue.FromInt64(2, 25),
            ColumnValue.FromDouble(50.0),
            ColumnValue.Null());

        // Verify via reader
        using var reader = writer.Database.CreateReader("users", "name");
        Assert.True(reader.Read());
        Assert.Equal("TestUser", reader.GetString(0));
    }

    [Fact]
    public void Insert_MultipleInserts_AllPersist()
    {
        using var writer = CreateWritableWriter();
        using var prepared = writer.PrepareWriter("users");

        for (int i = 1; i <= 5; i++)
        {
            prepared.Insert(
                ColumnValue.FromInt64(1, i),
                ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes($"User{i}")),
                ColumnValue.FromInt64(2, 20 + i),
                ColumnValue.FromDouble(100.0 + i),
                ColumnValue.Null());
        }

        using var reader = writer.Database.CreateReader("users");
        int count = 0;
        while (reader.Read()) count++;
        Assert.Equal(5, count);
    }

    [Fact]
    public void Insert_SequentialRowIds()
    {
        using var writer = CreateWritableWriter();
        using var prepared = writer.PrepareWriter("users");

        long id1 = prepared.Insert(
            ColumnValue.FromInt64(1, 1),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("A")),
            ColumnValue.FromInt64(2, 20),
            ColumnValue.FromDouble(1.0),
            ColumnValue.Null());

        long id2 = prepared.Insert(
            ColumnValue.FromInt64(1, 2),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("B")),
            ColumnValue.FromInt64(2, 21),
            ColumnValue.FromDouble(2.0),
            ColumnValue.Null());

        Assert.True(id2 > id1);
    }

    // ─── Delete ──────────────────────────────────────────────────

    [Fact]
    public void Delete_ExistingRow_ReturnsTrue()
    {
        using var writer = CreateWritableWriter(5);
        using var prepared = writer.PrepareWriter("users");

        Assert.True(prepared.Delete(3));
    }

    [Fact]
    public void Delete_NonExistentRow_ReturnsFalse()
    {
        using var writer = CreateWritableWriter(5);
        using var prepared = writer.PrepareWriter("users");

        Assert.False(prepared.Delete(999));
    }

    [Fact]
    public void Delete_RowNoLongerReadable()
    {
        using var writer = CreateWritableWriter(5);
        using var prepared = writer.PrepareWriter("users");

        prepared.Delete(3);

        using var reader = writer.Database.CreateReader("users");
        int count = 0;
        while (reader.Read()) count++;
        Assert.Equal(4, count);
    }

    // ─── Update ──────────────────────────────────────────────────

    [Fact]
    public void Update_ExistingRow_ReturnsTrue()
    {
        using var writer = CreateWritableWriter(5);
        using var prepared = writer.PrepareWriter("users");

        bool updated = prepared.Update(2,
            ColumnValue.FromInt64(1, 2),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("UpdatedUser")),
            ColumnValue.FromInt64(2, 99),
            ColumnValue.FromDouble(999.0),
            ColumnValue.Null());

        Assert.True(updated);
    }

    [Fact]
    public void Update_NonExistentRow_ReturnsFalse()
    {
        using var writer = CreateWritableWriter(5);
        using var prepared = writer.PrepareWriter("users");

        bool updated = prepared.Update(999,
            ColumnValue.FromInt64(1, 999),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("Sample")),
            ColumnValue.FromInt64(2, 0),
            ColumnValue.FromDouble(0.0),
            ColumnValue.Null());

        Assert.False(updated);
    }

    [Fact]
    public void Update_DataReflectsChange()
    {
        using var writer = CreateWritableWriter(5);
        using var prepared = writer.PrepareWriter("users");

        prepared.Update(1,
            ColumnValue.FromInt64(1, 1),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("Modified")),
            ColumnValue.FromInt64(2, 77),
            ColumnValue.FromDouble(777.0),
            ColumnValue.Null());

        using var reader = writer.Database.CreateReader("users", "name", "age");
        Assert.True(reader.Seek(1));
        Assert.Equal("Modified", reader.GetString(0));
        Assert.Equal(77, reader.GetInt32(1));
    }

    // ─── Reuse (zero-overhead repeated writes) ───────────────────

    [Fact]
    public void Reuse_MultipleInsertDeleteUpdate_Cycle()
    {
        using var writer = CreateWritableWriter();
        using var prepared = writer.PrepareWriter("users");

        // Insert
        long id = prepared.Insert(
            ColumnValue.FromInt64(1, 1),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("Cycle")),
            ColumnValue.FromInt64(2, 30),
            ColumnValue.FromDouble(100.0),
            ColumnValue.Null());

        // Update
        Assert.True(prepared.Update(id,
            ColumnValue.FromInt64(1, id),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("Updated")),
            ColumnValue.FromInt64(2, 31),
            ColumnValue.FromDouble(200.0),
            ColumnValue.Null()));

        // Delete
        Assert.True(prepared.Delete(id));

        // Verify gone
        using var reader = writer.Database.CreateReader("users");
        int count = 0;
        while (reader.Read()) count++;
        Assert.Equal(0, count);
    }

    // ─── Disposed ────────────────────────────────────────────────

    [Fact]
    public void Insert_AfterDispose_ThrowsObjectDisposedException()
    {
        using var writer = CreateWritableWriter();
        var prepared = writer.PrepareWriter("users");
        prepared.Dispose();

        Assert.Throws<ObjectDisposedException>(() => prepared.Insert(
            ColumnValue.FromInt64(1, 1),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("X")),
            ColumnValue.FromInt64(2, 1),
            ColumnValue.FromDouble(1.0),
            ColumnValue.Null()));
    }

    [Fact]
    public void Delete_AfterDispose_ThrowsObjectDisposedException()
    {
        using var writer = CreateWritableWriter();
        var prepared = writer.PrepareWriter("users");
        prepared.Dispose();

        Assert.Throws<ObjectDisposedException>(() => prepared.Delete(1));
    }

    [Fact]
    public void Update_AfterDispose_ThrowsObjectDisposedException()
    {
        using var writer = CreateWritableWriter();
        var prepared = writer.PrepareWriter("users");
        prepared.Dispose();

        Assert.Throws<ObjectDisposedException>(() => prepared.Update(1,
            ColumnValue.FromInt64(1, 1),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("X")),
            ColumnValue.FromInt64(2, 1),
            ColumnValue.FromDouble(1.0),
            ColumnValue.Null()));
    }

    // ─── Polymorphic via IPreparedWriter ─────────────────────────

    [Fact]
    public void Polymorphic_Insert_ViaInterface()
    {
        using var writer = CreateWritableWriter();
        using var prepared = writer.PrepareWriter("users");

        IPreparedWriter iface = prepared;
        long rowId = iface.Insert(
            ColumnValue.FromInt64(1, 1),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("PolyUser")),
            ColumnValue.FromInt64(2, 40),
            ColumnValue.FromDouble(400.0),
            ColumnValue.Null());

        Assert.True(rowId > 0);
    }

    [Fact]
    public void Polymorphic_Delete_ViaInterface()
    {
        using var writer = CreateWritableWriter(3);
        using var prepared = writer.PrepareWriter("users");

        IPreparedWriter iface = prepared;
        Assert.True(iface.Delete(1));
    }

    [Fact]
    public void Polymorphic_Update_ViaInterface()
    {
        using var writer = CreateWritableWriter(3);
        using var prepared = writer.PrepareWriter("users");

        IPreparedWriter iface = prepared;
        Assert.True(iface.Update(1,
            ColumnValue.FromInt64(1, 1),
            ColumnValue.Text(25, System.Text.Encoding.UTF8.GetBytes("PolyUpdate")),
            ColumnValue.FromInt64(2, 99),
            ColumnValue.FromDouble(99.0),
            ColumnValue.Null()));
    }
}