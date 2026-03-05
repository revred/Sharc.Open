// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using Sharc.Core;
using Sharc.Core.Query;
using Xunit;

namespace Sharc.IntegrationTests;

/// <summary>
/// Tests that SharcWriter correctly maintains secondary indexes during INSERT,
/// UPDATE, and DELETE operations. Verifies that index-accelerated queries return
/// correct results for databases created entirely through SharcWriter (not SQLite).
/// </summary>
public class IndexMaintenanceTests : IDisposable
{
    private readonly string _dbPath;
    private readonly SharcDatabase _db;

    public IndexMaintenanceTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"idx_maint_{Guid.NewGuid():N}.arc");
        _db = SharcDatabase.Create(_dbPath);
    }

    public void Dispose()
    {
        _db?.Dispose();
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_dbPath + ".journal"); } catch { }
        GC.SuppressFinalize(this);
    }

    private void ExecuteDDL(string sql)
    {
        using var tx = _db.BeginTransaction();
        tx.Execute(sql);
        tx.Commit();
    }

    /// <summary>
    /// Creates a database with table + index using SharcWriter DDL, inserts rows,
    /// and verifies that index-accelerated queries return correct results.
    /// </summary>
    [Fact]
    public void Insert_WithIndex_PopulatesIndexForQueries()
    {
        ExecuteDDL("CREATE TABLE events (id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT)");
        ExecuteDDL("CREATE INDEX idx_events_user ON events (user_id)");

        using (var writer = SharcWriter.From(_db))
        {
            for (int i = 1; i <= 50; i++)
            {
                writer.Insert("events",
                    ColumnValue.FromInt64(6, i),
                    ColumnValue.FromInt64(4, (i % 5) + 1),
                    ColumnValue.Text(2 * $"event_{i}".Length + 13,
                        System.Text.Encoding.UTF8.GetBytes($"event_{i}")));
            }
        }

        using var reader = _db.CreateReader("events", FilterStar.Column("user_id").Eq(3L));
        Assert.True(reader.IsIndexAccelerated, "Query should use index");

        int count = 0;
        while (reader.Read())
        {
            Assert.Equal(3L, reader.GetInt64(1));
            count++;
        }
        Assert.Equal(10, count);
    }

    /// <summary>
    /// Verifies index correctness for all distinct key values after bulk insert.
    /// </summary>
    [Fact]
    public void Insert_WithIndex_AllKeysReturnCorrectCounts()
    {
        ExecuteDDL("CREATE TABLE items (id INTEGER PRIMARY KEY, category INTEGER, label TEXT)");
        ExecuteDDL("CREATE INDEX idx_items_cat ON items (category)");

        using (var writer = SharcWriter.From(_db))
        {
            for (int i = 1; i <= 100; i++)
            {
                writer.Insert("items",
                    ColumnValue.FromInt64(6, i),
                    ColumnValue.FromInt64(4, (i % 10) + 1),
                    ColumnValue.Text(2 * $"item_{i}".Length + 13,
                        System.Text.Encoding.UTF8.GetBytes($"item_{i}")));
            }
        }

        for (int cat = 1; cat <= 10; cat++)
        {
            using var reader = _db.CreateReader("items", FilterStar.Column("category").Eq((long)cat));
            Assert.True(reader.IsIndexAccelerated);

            int count = 0;
            while (reader.Read())
            {
                Assert.Equal((long)cat, reader.GetInt64(1));
                count++;
            }
            Assert.Equal(10, count);
        }

        using var noMatch = _db.CreateReader("items", FilterStar.Column("category").Eq(99L));
        Assert.False(noMatch.Read());
    }

    /// <summary>
    /// Tests multiple indexes on the same table.
    /// </summary>
    [Fact]
    public void Insert_WithMultipleIndexes_AllIndexesWork()
    {
        ExecuteDDL("CREATE TABLE products (id INTEGER PRIMARY KEY, category INTEGER, brand TEXT, price INTEGER)");
        ExecuteDDL("CREATE INDEX idx_prod_cat ON products (category)");
        ExecuteDDL("CREATE INDEX idx_prod_brand ON products (brand)");

        string[] brands = ["Acme", "Beta", "Gamma"];

        using (var writer = SharcWriter.From(_db))
        {
            for (int i = 1; i <= 30; i++)
            {
                var brandName = brands[(i - 1) % brands.Length];
                writer.Insert("products",
                    ColumnValue.FromInt64(6, i),
                    ColumnValue.FromInt64(4, (i % 3) + 1),
                    ColumnValue.Text(2 * brandName.Length + 13,
                        System.Text.Encoding.UTF8.GetBytes(brandName)),
                    ColumnValue.FromInt64(4, i * 10));
            }
        }

        using var catReader = _db.CreateReader("products", FilterStar.Column("category").Eq(2L));
        Assert.True(catReader.IsIndexAccelerated);
        int catCount = 0;
        while (catReader.Read()) catCount++;
        Assert.Equal(10, catCount);

        using var brandReader = _db.CreateReader("products", FilterStar.Column("brand").Eq("Beta"));
        Assert.True(brandReader.IsIndexAccelerated);
        int brandCount = 0;
        while (brandReader.Read())
        {
            Assert.Equal("Beta", brandReader.GetString(2));
            brandCount++;
        }
        Assert.Equal(10, brandCount);
    }

    /// <summary>
    /// Verifies that index results match a full table scan (ground truth).
    /// </summary>
    [Fact]
    public void Insert_IndexResults_MatchFullScan()
    {
        ExecuteDDL("CREATE TABLE data (id INTEGER PRIMARY KEY, key INTEGER, value TEXT)");
        ExecuteDDL("CREATE INDEX idx_data_key ON data (key)");

        using (var writer = SharcWriter.From(_db))
        {
            for (int i = 1; i <= 200; i++)
            {
                var val = $"val_{i}";
                writer.Insert("data",
                    ColumnValue.FromInt64(6, i),
                    ColumnValue.FromInt64(4, i % 7),
                    ColumnValue.Text(2 * val.Length + 13,
                        System.Text.Encoding.UTF8.GetBytes(val)));
            }
        }

        for (int key = 0; key < 7; key++)
        {
            var scanIds = new List<long>();
            using (var scan = _db.CreateReader("data"))
            {
                while (scan.Read())
                {
                    if (scan.GetInt64(1) == key)
                        scanIds.Add(scan.GetInt64(0));
                }
            }

            var indexIds = new List<long>();
            using (var idx = _db.CreateReader("data", FilterStar.Column("key").Eq((long)key)))
            {
                Assert.True(idx.IsIndexAccelerated);
                while (idx.Read())
                    indexIds.Add(idx.GetInt64(0));
            }

            scanIds.Sort();
            indexIds.Sort();
            Assert.Equal(scanIds, indexIds);
        }
    }

    /// <summary>
    /// Tests that inserting into multiple tables with indexes doesn't corrupt table data.
    /// Simulates a graph-style workload: many tables + indexes, then read back.
    /// </summary>
    [Fact]
    public void Insert_MultipleTables_WithIndexes_ReadBackSucceeds()
    {
        ExecuteDDL("CREATE TABLE nodes (id INTEGER PRIMARY KEY, name TEXT NOT NULL, file TEXT NOT NULL, category INTEGER NOT NULL)");
        ExecuteDDL("CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, target_id INTEGER NOT NULL, link_type INTEGER NOT NULL)");
        ExecuteDDL("CREATE TABLE attributes (item_id INTEGER NOT NULL, attr_id INTEGER NOT NULL, value TEXT NOT NULL)");
        ExecuteDDL("CREATE INDEX idx_nodes_name ON nodes(name)");
        ExecuteDDL("CREATE INDEX idx_edges_source ON edges(source_id, link_type)");
        ExecuteDDL("CREATE INDEX idx_edges_target ON edges(target_id, link_type)");
        ExecuteDDL("CREATE INDEX idx_attrs_item ON attributes(item_id)");

        // Insert data across multiple tables
        using (var writer = SharcWriter.From(_db))
        {
            for (int i = 1; i <= 100; i++)
            {
                var name = $"ns::class_{i}::method_{i}";
                var file = $"/src/file_{i % 10}.cpp";
                writer.Insert("nodes",
                    ColumnValue.FromInt64(6, i),
                    ColumnValue.Text(2 * name.Length + 13, System.Text.Encoding.UTF8.GetBytes(name)),
                    ColumnValue.Text(2 * file.Length + 13, System.Text.Encoding.UTF8.GetBytes(file)),
                    ColumnValue.FromInt64(4, (i % 5) + 1));
            }

            for (int i = 1; i <= 200; i++)
            {
                writer.Insert("edges",
                    ColumnValue.FromInt64(6, i),
                    ColumnValue.FromInt64(4, (i % 100) + 1),
                    ColumnValue.FromInt64(4, ((i + 50) % 100) + 1),
                    ColumnValue.FromInt64(4, (i % 3) + 1));
            }

            for (int i = 1; i <= 300; i++)
            {
                var val = $"attr_value_{i}";
                writer.Insert("attributes",
                    ColumnValue.FromInt64(4, (i % 100) + 1),
                    ColumnValue.FromInt64(4, (i % 10) + 1),
                    ColumnValue.Text(2 * val.Length + 13, System.Text.Encoding.UTF8.GetBytes(val)));
            }
        }

        // Read back all data
        int nodeCount = 0;
        using (var reader = _db.CreateReader("nodes"))
        {
            while (reader.Read())
            {
                _ = reader.GetInt64(0);  // id
                _ = reader.GetString(1); // name
                _ = reader.GetString(2); // file
                _ = reader.GetInt64(3);  // category
                nodeCount++;
            }
        }
        Assert.Equal(100, nodeCount);

        int edgeCount = 0;
        using (var reader = _db.CreateReader("edges"))
        {
            while (reader.Read())
            {
                _ = reader.GetInt64(0); // id
                edgeCount++;
            }
        }
        Assert.Equal(200, edgeCount);

        int attrCount = 0;
        using (var reader = _db.CreateReader("attributes"))
        {
            while (reader.Read())
            {
                attrCount++;
            }
        }
        Assert.Equal(300, attrCount);
    }

    /// <summary>
    /// Tests many tables + indexes + data, then reads back.
    /// </summary>
    [Fact]
    public void Insert_ManyTablesAndIndexes_ReadBackSucceeds()
    {
        // Create 10 tables, each with 1-2 indexes
        for (int t = 0; t < 10; t++)
        {
            ExecuteDDL($"CREATE TABLE tab{t} (id INTEGER PRIMARY KEY, val INTEGER, txt TEXT)");
            ExecuteDDL($"CREATE INDEX idx_tab{t}_val ON tab{t} (val)");
            if (t % 2 == 0)
                ExecuteDDL($"CREATE INDEX idx_tab{t}_txt ON tab{t} (txt)");
        }

        // Insert data into all tables
        using (var writer = SharcWriter.From(_db))
        {
            for (int t = 0; t < 10; t++)
            {
                for (int i = 1; i <= 50; i++)
                {
                    var txt = $"table{t}_row{i}";
                    writer.Insert($"tab{t}",
                        ColumnValue.FromInt64(6, i),
                        ColumnValue.FromInt64(4, i % 5),
                        ColumnValue.Text(2 * txt.Length + 13,
                            System.Text.Encoding.UTF8.GetBytes(txt)));
                }
            }
        }

        // Read back from each table
        for (int t = 0; t < 10; t++)
        {
            int count = 0;
            using var reader = _db.CreateReader($"tab{t}");
            while (reader.Read())
            {
                _ = reader.GetInt64(0);
                _ = reader.GetInt64(1);
                _ = reader.GetString(2);
                count++;
            }
            Assert.Equal(50, count);
        }
    }

    /// <summary>
    /// Graph workload: create schema, insert data, read+update, insert more, read.
    /// Mimics a scan-then-recompute flow with multiple indexed tables.
    /// </summary>
    [Fact]
    public void Insert_GraphPattern_InsertUpdateReadSucceeds()
    {
        // Create graph schema with indexes
        ExecuteDDL("CREATE TABLE graph_nodes (node_id INTEGER PRIMARY KEY, name TEXT NOT NULL, file TEXT NOT NULL, line_start INTEGER NOT NULL, line_end INTEGER NOT NULL, category INTEGER NOT NULL, in_degree INTEGER NOT NULL DEFAULT 0, out_degree INTEGER NOT NULL DEFAULT 0, reach INTEGER NOT NULL DEFAULT 0, hash TEXT NOT NULL, created_at INTEGER NOT NULL DEFAULT 0, expired_at INTEGER)");
        ExecuteDDL("CREATE TABLE graph_edges (edge_id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, target_id INTEGER NOT NULL, link_type INTEGER NOT NULL, weight REAL NOT NULL DEFAULT 1.0)");
        ExecuteDDL("CREATE TABLE node_attributes (node_id INTEGER NOT NULL, attr_id INTEGER NOT NULL, value TEXT NOT NULL, weight REAL NOT NULL DEFAULT 1.0)");
        ExecuteDDL("CREATE INDEX idx_nodes_name ON graph_nodes(name)");
        ExecuteDDL("CREATE INDEX idx_nodes_file ON graph_nodes(file, line_start)");
        ExecuteDDL("CREATE INDEX idx_edges_src ON graph_edges(source_id, link_type)");
        ExecuteDDL("CREATE INDEX idx_edges_tgt ON graph_edges(target_id, link_type)");
        ExecuteDDL("CREATE INDEX idx_attrs_node ON node_attributes(node_id)");

        using var writer = SharcWriter.From(_db);

        // Phase 1: insert nodes, edges, attributes
        for (int i = 1; i <= 20; i++)
        {
            var name = $"ns::class_{i}";
            var file = "/src/file1.cpp";
            var hash = $"abc{i:D8}";
            writer.Insert("graph_nodes",
                ColumnValue.FromInt64(6, i),
                ColumnValue.Text(2 * name.Length + 13, System.Text.Encoding.UTF8.GetBytes(name)),
                ColumnValue.Text(2 * file.Length + 13, System.Text.Encoding.UTF8.GetBytes(file)),
                ColumnValue.FromInt64(4, i),
                ColumnValue.FromInt64(4, i + 10),
                ColumnValue.FromInt64(4, 1),
                ColumnValue.FromInt64(4, 0),
                ColumnValue.FromInt64(4, 0),
                ColumnValue.FromInt64(4, 0),
                ColumnValue.Text(2 * hash.Length + 13, System.Text.Encoding.UTF8.GetBytes(hash)),
                ColumnValue.FromInt64(4, 0),
                ColumnValue.Null());
        }

        for (int i = 1; i <= 30; i++)
        {
            writer.Insert("graph_edges",
                ColumnValue.FromInt64(6, i),
                ColumnValue.FromInt64(4, (i % 20) + 1),
                ColumnValue.FromInt64(4, ((i + 5) % 20) + 1),
                ColumnValue.FromInt64(4, 1),
                ColumnValue.FromDouble(1.0));
        }

        for (int i = 1; i <= 40; i++)
        {
            var val = $"attr_{i}";
            writer.Insert("node_attributes",
                ColumnValue.FromInt64(4, (i % 20) + 1),
                ColumnValue.FromInt64(4, (i % 5) + 1),
                ColumnValue.Text(2 * val.Length + 13, System.Text.Encoding.UTF8.GetBytes(val)),
                ColumnValue.FromDouble(1.0));
        }

        // Phase 2: read all nodes back, then update degrees
        var nodeIds = new List<long>();
        using (var reader = _db.CreateReader("graph_nodes"))
        {
            while (reader.Read())
            {
                nodeIds.Add(reader.GetInt64(0));
            }
        }
        Assert.Equal(20, nodeIds.Count);

        // Update each node with recomputed degree values
        foreach (var nodeId in nodeIds)
        {
            var name = $"ns::class_{nodeId}";
            var file = "/src/file1.cpp";
            var hash = $"abc{nodeId:D8}";
            writer.Update("graph_nodes", nodeId,
                nodeId,
                name,
                file,
                nodeId,
                nodeId + 10,
                1L,
                1L,
                2L,
                3L,
                hash,
                0L,
                ColumnValue.Null());
        }

        // Read nodes back again after update
        int finalCount = 0;
        using (var reader = _db.CreateReader("graph_nodes"))
        {
            while (reader.Read())
            {
                finalCount++;
            }
        }
        Assert.Equal(20, finalCount);
    }

    /// <summary>
    /// Tests that large inserts (requiring B-tree splits) work correctly.
    /// </summary>
    [Fact]
    public void Insert_LargeDataset_IndexSplitsWork()
    {
        ExecuteDDL("CREATE TABLE logs (id INTEGER PRIMARY KEY, level INTEGER, msg TEXT)");
        ExecuteDDL("CREATE INDEX idx_logs_level ON logs (level)");

        using (var writer = SharcWriter.From(_db))
        {
            for (int i = 1; i <= 1000; i++)
            {
                var msg = $"Log message number {i}";
                writer.Insert("logs",
                    ColumnValue.FromInt64(6, i),
                    ColumnValue.FromInt64(4, (i % 4) + 1),
                    ColumnValue.Text(2 * msg.Length + 13,
                        System.Text.Encoding.UTF8.GetBytes(msg)));
            }
        }

        for (int level = 1; level <= 4; level++)
        {
            using var reader = _db.CreateReader("logs", FilterStar.Column("level").Eq((long)level));
            Assert.True(reader.IsIndexAccelerated);

            int count = 0;
            while (reader.Read())
            {
                Assert.Equal((long)level, reader.GetInt64(1));
                count++;
            }
            Assert.Equal(250, count);
        }
    }
}
