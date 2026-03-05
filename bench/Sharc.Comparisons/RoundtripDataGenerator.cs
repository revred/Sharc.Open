/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Licensed under the MIT License — free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using Microsoft.Data.Sqlite;

namespace Sharc.Comparisons;

/// <summary>
/// Generates a benchmark database with two identically-schemaed tables (users_a, users_b)
/// that share an overlapping ID range. This enables meaningful UNION, INTERSECT, and EXCEPT
/// benchmarks where some rows are duplicated across tables.
///
/// Schema per table: id, name, email, age, score, active, dept, created
/// Overlap: rows with IDs in [overlapStart, rowsPerTable] appear in both tables with identical data.
/// </summary>
public static class RoundtripDataGenerator
{
    private static readonly string[] Departments = ["eng", "sales", "ops", "hr", "marketing"];
    private static readonly string[] FirstNames = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Iris", "Jack"];
    private static readonly string[] LastNames = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"];

    /// <summary>
    /// Generates the benchmark database.
    /// Table users_a: IDs 1 through <paramref name="rowsPerTable"/>.
    /// Table users_b: IDs <paramref name="overlapStart"/> through <paramref name="overlapStart"/> + <paramref name="rowsPerTable"/> - 1.
    /// Overlapping IDs get identical data so INTERSECT/EXCEPT produce predictable results.
    /// </summary>
    public static void Generate(string dbPath, int rowsPerTable, int overlapStart)
    {
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var conn = new SqliteConnection($"Data Source={dbPath};Pooling=False");
        conn.Open();

        using var schemaCmd = conn.CreateCommand();
        schemaCmd.CommandText = @"
            PRAGMA journal_mode = DELETE;
            PRAGMA page_size = 4096;

            CREATE TABLE users_a (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                age INTEGER NOT NULL,
                score REAL NOT NULL,
                active INTEGER NOT NULL,
                dept TEXT NOT NULL,
                created TEXT NOT NULL
            );

            CREATE TABLE users_b (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                age INTEGER NOT NULL,
                score REAL NOT NULL,
                active INTEGER NOT NULL,
                dept TEXT NOT NULL,
                created TEXT NOT NULL
            );
        ";
        schemaCmd.ExecuteNonQuery();

        using var tx = conn.BeginTransaction();

        // Populate users_a: IDs 1..rowsPerTable
        PopulateTable(conn, tx, "users_a", 1, rowsPerTable);

        // Populate users_b: IDs overlapStart..(overlapStart + rowsPerTable - 1)
        PopulateTable(conn, tx, "users_b", overlapStart, rowsPerTable);

        tx.Commit();

        using var vacuumCmd = conn.CreateCommand();
        vacuumCmd.CommandText = "VACUUM;";
        vacuumCmd.ExecuteNonQuery();

        conn.Close();
        SqliteConnection.ClearPool(conn);
    }

    private static void PopulateTable(
        SqliteConnection conn,
        SqliteTransaction tx,
        string tableName,
        int startId,
        int count)
    {
        var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = $@"INSERT INTO {tableName} (id, name, email, age, score, active, dept, created)
                             VALUES ($id, $name, $email, $age, $score, $active, $dept, $created)";

        var pId = cmd.Parameters.Add("$id", SqliteType.Integer);
        var pName = cmd.Parameters.Add("$name", SqliteType.Text);
        var pEmail = cmd.Parameters.Add("$email", SqliteType.Text);
        var pAge = cmd.Parameters.Add("$age", SqliteType.Integer);
        var pScore = cmd.Parameters.Add("$score", SqliteType.Real);
        var pActive = cmd.Parameters.Add("$active", SqliteType.Integer);
        var pDept = cmd.Parameters.Add("$dept", SqliteType.Text);
        var pCreated = cmd.Parameters.Add("$created", SqliteType.Text);

        for (int i = 0; i < count; i++)
        {
            int id = startId + i;

            // Use a seed derived from the ID so overlapping rows produce identical data
            var rng = new Random(id * 31 + 42);

            var firstName = FirstNames[rng.Next(FirstNames.Length)];
            var lastName = LastNames[rng.Next(LastNames.Length)];

            pId.Value = id;
            pName.Value = $"{firstName} {lastName}";
            pEmail.Value = $"{firstName.ToLowerInvariant()}.{lastName.ToLowerInvariant()}{id}@example.com";
            pAge.Value = rng.Next(18, 80);
            pScore.Value = Math.Round(rng.NextDouble() * 100, 2);
            pActive.Value = rng.Next(0, 2);
            pDept.Value = Departments[rng.Next(Departments.Length)];
            pCreated.Value = new DateTime(2024, 1, 1).AddDays(rng.Next(0, 365)).ToString("yyyy-MM-dd");

            cmd.ExecuteNonQuery();
        }
    }
}
