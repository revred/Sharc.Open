// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using Microsoft.Data.Sqlite;

namespace Sharc.Comparisons;

/// <summary>
/// Generates SQLite-backed graph databases for benchmark scenarios.
/// </summary>
public static class GraphGenerator
{
    public static void GenerateSQLite(string dbPath, int nodeCount, int edgeCount)
    {
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var conn = new SqliteConnection($"Data Source={dbPath};Pooling=False");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            PRAGMA journal_mode = DELETE;
            PRAGMA page_size = 4096;
            
            CREATE TABLE _concepts (
                id TEXT NOT NULL,
                key INTEGER PRIMARY KEY,
                type_id INTEGER NOT NULL,
                data TEXT,
                cvn INTEGER,
                lvn INTEGER,
                sync_status INTEGER
            );
            
            CREATE TABLE _relations (
                id TEXT NOT NULL,
                source_key INTEGER NOT NULL,
                kind INTEGER NOT NULL,
                target_key INTEGER NOT NULL,
                data TEXT
            );

            CREATE INDEX idx_concepts_id ON _concepts(id);
            CREATE UNIQUE INDEX idx_concepts_key ON _concepts(key);
            CREATE INDEX idx_relations_source_kind ON _relations(source_key, kind, target_key);
            CREATE INDEX idx_relations_target_kind ON _relations(target_key, kind, source_key);
        ";
        cmd.ExecuteNonQuery();

        using var tx = conn.BeginTransaction();
        
        // Insert Nodes
        var nodeCmd = conn.CreateCommand();
        nodeCmd.Transaction = tx;
        nodeCmd.CommandText = "INSERT INTO _concepts (id, key, type_id, data) VALUES ($id, $key, $type, $data)";
        var pId = nodeCmd.Parameters.Add("$id", SqliteType.Text);
        var pKey = nodeCmd.Parameters.Add("$key", SqliteType.Integer);
        var pType = nodeCmd.Parameters.Add("$type", SqliteType.Integer);
        var pData = nodeCmd.Parameters.Add("$data", SqliteType.Text);

        var random = new Random(42);
        
        for (int i = 0; i < nodeCount; i++)
        {
            pId.Value = Guid.NewGuid().ToString();
            pKey.Value = i + 1;
            pType.Value = random.Next(1, 5); // 1=File, 2=Class, 3=Method, 4=Variable
            pData.Value = $"{{\"name\": \"Node_{i}\", \"size\": {random.Next(100, 10000)}}}";
            nodeCmd.ExecuteNonQuery();
        }

        // Insert Edges
        var edgeCmd = conn.CreateCommand();
        edgeCmd.Transaction = tx;
        edgeCmd.CommandText = "INSERT INTO _relations (id, source_key, kind, target_key, data) VALUES ($id, $sKey, $k, $tKey, $d)";
        var epId = edgeCmd.Parameters.Add("$id", SqliteType.Text);
        var epOrigin = edgeCmd.Parameters.Add("$sKey", SqliteType.Integer);
        var epKind = edgeCmd.Parameters.Add("$k", SqliteType.Integer);
        var epTarget = edgeCmd.Parameters.Add("$tKey", SqliteType.Integer);
        var epData = edgeCmd.Parameters.Add("$d", SqliteType.Text);

        for (int i = 0; i < edgeCount; i++)
        {
            epId.Value = Guid.NewGuid().ToString();
            epOrigin.Value = random.Next(1, nodeCount + 1);
            epKind.Value = random.Next(10, 20); // 10=Defines, 15=Calls
            epTarget.Value = random.Next(1, nodeCount + 1);
            epData.Value = "{}";
            edgeCmd.ExecuteNonQuery();
        }

        tx.Commit();
        
        // Optimize B-Tree layout
        using var vacuumCmd = conn.CreateCommand();
        vacuumCmd.CommandText = "VACUUM;";
        vacuumCmd.ExecuteNonQuery();
    }
}
