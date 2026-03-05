// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System.Diagnostics;
using Microsoft.Data.Sqlite;
using Sharc.Core.Query;
using Sharc.IntegrationTests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace Sharc.IntegrationTests;

public sealed class Issue83PerformanceComparisonTests
{
    private readonly ITestOutputHelper _output;

    public Issue83PerformanceComparisonTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    [Trait("Category", "Performance")]
    public void Issue83_TypeIdFilter_CompareWithSqlite()
    {
        const int distinctTypeCount = 64;
        const int rowsPerType = 1_000;
        const int rowCount = distinctTypeCount * rowsPerType;
        const int warmupIterations = 12;
        const int measuredIterations = 64;

        var dbBytes = TestDatabaseFactory.CreateDatabaseWith(conn =>
        {
            TestDatabaseFactory.Execute(conn, """
                CREATE TABLE Entity (
                    Id INTEGER PRIMARY KEY,
                    TypeID INTEGER NOT NULL,
                    Name TEXT NOT NULL,
                    Status INTEGER NOT NULL,
                    Score REAL NOT NULL,
                    Payload TEXT,
                    CreatedAt INTEGER NOT NULL,
                    UpdatedAt INTEGER NOT NULL,
                    Flags INTEGER NOT NULL
                );
                CREATE INDEX idx_entity_typeid ON Entity(TypeID);
                """);

            using var tx = conn.BeginTransaction();
            using var insert = conn.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = """
                INSERT INTO Entity (
                    Id, TypeID, Name, Status, Score, Payload, CreatedAt, UpdatedAt, Flags
                )
                VALUES ($id, $typeId, $name, $status, $score, $payload, $createdAt, $updatedAt, $flags);
                """;

            var pId = insert.Parameters.Add("$id", SqliteType.Integer);
            var pTypeId = insert.Parameters.Add("$typeId", SqliteType.Integer);
            var pName = insert.Parameters.Add("$name", SqliteType.Text);
            var pStatus = insert.Parameters.Add("$status", SqliteType.Integer);
            var pScore = insert.Parameters.Add("$score", SqliteType.Real);
            var pPayload = insert.Parameters.Add("$payload", SqliteType.Text);
            var pCreatedAt = insert.Parameters.Add("$createdAt", SqliteType.Integer);
            var pUpdatedAt = insert.Parameters.Add("$updatedAt", SqliteType.Integer);
            var pFlags = insert.Parameters.Add("$flags", SqliteType.Integer);

            long baseEpoch = 1_700_000_000L;
            for (int i = 0; i < rowCount; i++)
            {
                int typeId = (i % distinctTypeCount) + 1;
                pId.Value = i + 1;
                pTypeId.Value = typeId;
                pName.Value = $"entity-{i + 1}";
                pStatus.Value = i % 8;
                pScore.Value = (double)(i % 1000) / 10.0;
                pPayload.Value = $"payload-{typeId}-{i % 31}";
                pCreatedAt.Value = baseEpoch + i;
                pUpdatedAt.Value = baseEpoch + i + 42;
                pFlags.Value = i & 0xFF;
                insert.ExecuteNonQuery();
            }

            tx.Commit();
        });

        string dbPath = Path.Combine(Path.GetTempPath(), $"issue83_perf_{Guid.NewGuid():N}.db");
        File.WriteAllBytes(dbPath, dbBytes);

        try
        {
            var sqlite = MeasureSqliteTypeIdReadAll(
                dbPath, distinctTypeCount, warmupIterations, measuredIterations);
            var sharc = MeasureSharcTypeIdReadAll(
                dbPath, distinctTypeCount, warmupIterations, measuredIterations);

            _output.WriteLine($"SQLite mean/query: {sqlite.MeanMs:F3} ms");
            _output.WriteLine($"Sharc  mean/query: {sharc.MeanMs:F3} ms");
            _output.WriteLine($"Rows read (SQLite): {sqlite.RowsRead:N0}");
            _output.WriteLine($"Rows read (Sharc):  {sharc.RowsRead:N0}");
            _output.WriteLine($"Sharc/SQLite ratio: {sharc.MeanMs / sqlite.MeanMs:F2}x");

            Assert.True(sharc.RowsRead > 0);
            Assert.True(
                sharc.MeanMs <= sqlite.MeanMs * 1.75,
                $"Issue #83 regression: Sharc mean/query={sharc.MeanMs:F3} ms, " +
                $"SQLite mean/query={sqlite.MeanMs:F3} ms (ratio={(sharc.MeanMs / sqlite.MeanMs):F2}x).");
        }
        finally
        {
            try
            {
                SqliteConnection.ClearAllPools();
            }
            catch
            {
                // Best-effort cleanup.
            }

            try
            {
                if (File.Exists(dbPath))
                    File.Delete(dbPath);
            }
            catch
            {
                // Best-effort cleanup.
            }
        }
    }

    private static (double MeanMs, long RowsRead) MeasureSqliteTypeIdReadAll(
        string dbPath,
        int distinctTypeCount,
        int warmupIterations,
        int measuredIterations)
    {
        using var conn = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT * FROM Entity WHERE TypeID = $t;
            """;
        var pType = cmd.Parameters.Add("$t", SqliteType.Integer);

        for (int i = 0; i < warmupIterations; i++)
        {
            pType.Value = (i % distinctTypeCount) + 1;
            using var warmupReader = cmd.ExecuteReader();
            while (warmupReader.Read())
            {
                for (int c = 0; c < warmupReader.FieldCount; c++)
                    _ = warmupReader.GetValue(c);
            }
        }

        long rowsRead = 0;
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < measuredIterations; i++)
        {
            pType.Value = (i % distinctTypeCount) + 1;
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                for (int c = 0; c < reader.FieldCount; c++)
                    _ = reader.GetValue(c);
                rowsRead++;
            }
        }
        sw.Stop();

        return (sw.Elapsed.TotalMilliseconds / measuredIterations, rowsRead);
    }

    private static (double MeanMs, long RowsRead) MeasureSharcTypeIdReadAll(
        string dbPath,
        int distinctTypeCount,
        int warmupIterations,
        int measuredIterations)
    {
        using var db = SharcDatabase.Open(dbPath, new SharcOpenOptions { Writable = false });

        for (int i = 0; i < warmupIterations; i++)
        {
            int typeId = (i % distinctTypeCount) + 1;
            using var warmupReader = db.CreateReader(
                "Entity",
                new SharcFilter("TypeID", SharcOperator.Equal, (long)typeId));
            while (warmupReader.Read())
            {
                for (int c = 0; c < warmupReader.FieldCount; c++)
                    _ = warmupReader.GetValue(c);
            }
        }

        long rowsRead = 0;
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < measuredIterations; i++)
        {
            int typeId = (i % distinctTypeCount) + 1;
            using var reader = db.CreateReader(
                "Entity",
                new SharcFilter("TypeID", SharcOperator.Equal, (long)typeId));
            while (reader.Read())
            {
                for (int c = 0; c < reader.FieldCount; c++)
                    _ = reader.GetValue(c);
                rowsRead++;
            }
        }
        sw.Stop();

        return (sw.Elapsed.TotalMilliseconds / measuredIterations, rowsRead);
    }
}
