/*-------------------------------------------------------------------------------------------------!
  "Where the mind is free to imagine and the craft is guided by clarity, code awakens."            |

  A collaborative work shaped by Artificial Intelligence and curated with intent by Ram Revanur.
  Licensed under the MIT License — free for personal and commercial use.                           |
--------------------------------------------------------------------------------------------------*/

using Microsoft.Data.Sqlite;
using Sharc.Vector;

namespace Sharc.Comparisons;

/// <summary>
/// Deterministic data generator for vector search benchmarks.
/// Creates a table with id, category, and a BLOB vector column.
/// Uses fixed seed=42 for reproducibility.
/// </summary>
public static class VectorDataGenerator
{
    private static readonly string[] Categories = ["science", "tech", "art", "health", "finance"];

    /// <summary>
    /// Generates a SQLite database with a vector table for benchmarking.
    /// Each row has an integer id, text category, and a BLOB float vector.
    /// </summary>
    /// <param name="dbPath">Output database path.</param>
    /// <param name="rowCount">Number of rows to insert.</param>
    /// <param name="dimensions">Vector dimensionality (e.g., 128, 384).</param>
    public static void GenerateSQLite(string dbPath, int rowCount, int dimensions)
    {
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var conn = new SqliteConnection($"Data Source={dbPath};Pooling=False");
        conn.Open();

        using var schemaCmd = conn.CreateCommand();
        schemaCmd.CommandText = @"
            PRAGMA journal_mode = DELETE;
            PRAGMA page_size = 4096;

            CREATE TABLE vectors (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                embedding BLOB NOT NULL
            );
        ";
        schemaCmd.ExecuteNonQuery();

        using var tx = conn.BeginTransaction();

        var insertCmd = conn.CreateCommand();
        insertCmd.Transaction = tx;
        insertCmd.CommandText = "INSERT INTO vectors (id, category, embedding) VALUES ($id, $cat, $emb)";
        var pId = insertCmd.Parameters.Add("$id", SqliteType.Integer);
        var pCat = insertCmd.Parameters.Add("$cat", SqliteType.Text);
        var pEmb = insertCmd.Parameters.Add("$emb", SqliteType.Blob);

        var rng = new Random(42);

        for (int i = 1; i <= rowCount; i++)
        {
            var vector = new float[dimensions];
            for (int d = 0; d < dimensions; d++)
                vector[d] = (float)(rng.NextDouble() * 2.0 - 1.0); // [-1, 1]

            pId.Value = i;
            pCat.Value = Categories[i % Categories.Length];
            pEmb.Value = BlobVectorCodec.Encode(vector);

            insertCmd.ExecuteNonQuery();
        }

        tx.Commit();

        using var vacuumCmd = conn.CreateCommand();
        vacuumCmd.CommandText = "VACUUM;";
        vacuumCmd.ExecuteNonQuery();
        conn.Close();
        SqliteConnection.ClearAllPools();
    }

    /// <summary>
    /// Generates a deterministic query vector (same seed, index-based).
    /// </summary>
    public static float[] GenerateQueryVector(int dimensions, int seed = 99)
    {
        var rng = new Random(seed);
        var vector = new float[dimensions];
        for (int d = 0; d < dimensions; d++)
            vector[d] = (float)(rng.NextDouble() * 2.0 - 1.0);
        return vector;
    }
}
