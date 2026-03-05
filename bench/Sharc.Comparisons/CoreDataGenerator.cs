/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Licensed under the MIT License — free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using Microsoft.Data.Sqlite;

namespace Sharc.Comparisons;

/// <summary>
/// Deterministic data generator for core benchmarks: users table with 9 typed columns.
/// Mirrors the Arena's DataGenerator schema for accurate browser-vs-desktop comparison.
/// Uses fixed seed=42 for reproducibility.
/// </summary>
public static class CoreDataGenerator
{
    private static readonly string[] Departments = ["eng", "sales", "ops", "hr", "marketing"];
    private static readonly string[] FirstNames = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Iris", "Jack"];
    private static readonly string[] LastNames = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"];

    public static void GenerateSQLite(string dbPath, int userCount)
    {
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var conn = new SqliteConnection($"Data Source={dbPath};Pooling=False");
        conn.Open();

        using var schemaCmd = conn.CreateCommand();
        schemaCmd.CommandText = @"
            PRAGMA journal_mode = DELETE;
            PRAGMA page_size = 4096;
            
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                age INTEGER NOT NULL,
                score REAL NOT NULL,
                bio TEXT,
                active INTEGER NOT NULL,
                dept TEXT NOT NULL,
                created TEXT NOT NULL
            );
        ";
        schemaCmd.ExecuteNonQuery();

        using var tx = conn.BeginTransaction();

        var insertCmd = conn.CreateCommand();
        insertCmd.Transaction = tx;
        insertCmd.CommandText = @"INSERT INTO users (id, name, email, age, score, bio, active, dept, created) 
                                  VALUES ($id, $name, $email, $age, $score, $bio, $active, $dept, $created)";
        var pId = insertCmd.Parameters.Add("$id", SqliteType.Integer);
        var pName = insertCmd.Parameters.Add("$name", SqliteType.Text);
        var pEmail = insertCmd.Parameters.Add("$email", SqliteType.Text);
        var pAge = insertCmd.Parameters.Add("$age", SqliteType.Integer);
        var pScore = insertCmd.Parameters.Add("$score", SqliteType.Real);
        var pBio = insertCmd.Parameters.Add("$bio", SqliteType.Text);
        var pActive = insertCmd.Parameters.Add("$active", SqliteType.Integer);
        var pDept = insertCmd.Parameters.Add("$dept", SqliteType.Text);
        var pCreated = insertCmd.Parameters.Add("$created", SqliteType.Text);

        var rng = new Random(42);

        for (int i = 1; i <= userCount; i++)
        {
            var firstName = FirstNames[rng.Next(FirstNames.Length)];
            var lastName = LastNames[rng.Next(LastNames.Length)];

            pId.Value = i;
            pName.Value = $"{firstName} {lastName}";
            pEmail.Value = $"{firstName.ToLowerInvariant()}.{lastName.ToLowerInvariant()}{i}@example.com";
            pAge.Value = rng.Next(18, 80);
            pScore.Value = Math.Round(rng.NextDouble() * 100, 2);
            pBio.Value = rng.NextDouble() < 0.3 ? DBNull.Value : $"Bio for user {i}: {firstName} works in {Departments[rng.Next(Departments.Length)]}";
            pActive.Value = rng.Next(0, 2);
            pDept.Value = Departments[rng.Next(Departments.Length)];
            pCreated.Value = new DateTime(2024, 1, 1).AddDays(rng.Next(0, 365)).ToString("yyyy-MM-dd");

            insertCmd.ExecuteNonQuery();
        }

        tx.Commit();

        using var vacuumCmd = conn.CreateCommand();
        vacuumCmd.CommandText = "VACUUM;";
        vacuumCmd.ExecuteNonQuery();
        conn.Close();
        SqliteConnection.ClearAllPools();
    }
}
