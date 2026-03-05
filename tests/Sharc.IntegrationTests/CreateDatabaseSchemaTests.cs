// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using Sharc.Core;
using Sharc.Core.Format;
using Xunit;

namespace Sharc.IntegrationTests;

/// <summary>
/// Regression tests for SharcDatabase.Create(path) + schema operations.
/// These reproduce a bug where creating many tables in a single transaction
/// on a file-based database causes CorruptPageException (page type 0x53).
/// The in-memory path (CreateInMemory) works fine — the bug is file-specific.
/// </summary>
public sealed class CreateDatabaseSchemaTests : IDisposable
{
    private readonly string _dbPath;

    public CreateDatabaseSchemaTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"create_schema_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".journal")) File.Delete(_dbPath + ".journal");
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Baseline: CreateInMemory + CREATE TABLE works. Proves the issue is file-specific.
    /// </summary>
    [Fact]
    public void CreateInMemory_ThenCreateTable_ShouldWork()
    {
        using var db = SharcDatabase.CreateInMemory();
        using var tx = db.BeginTransaction();
        tx.Execute("CREATE TABLE test(id INTEGER PRIMARY KEY, name TEXT)");
        tx.Commit();

        var table = db.Schema.GetTable("test");
        Assert.NotNull(table);
        Assert.Equal(2, table.Columns.Count);
    }

    /// <summary>
    /// Create(path) then read schema — should be able to read built-in tables.
    /// </summary>
    [Fact]
    public void CreateFile_ThenReadSchema_ShouldWork()
    {
        using var db = SharcDatabase.Create(_dbPath);
        var schema = db.Schema;
        var tableNames = schema.Tables.Select(t => t.Name).ToList();

        Assert.Contains("_sharc_ledger", tableNames);
        Assert.Contains("_sharc_agents", tableNames);
        Assert.Contains("_sharc_scores", tableNames);
        Assert.Contains("_sharc_audit", tableNames);
    }

    /// <summary>
    /// Create(path) then CREATE TABLE — single table in one transaction.
    /// </summary>
    [Fact]
    public void CreateFile_ThenCreateTable_ShouldWork()
    {
        using var db = SharcDatabase.Create(_dbPath);
        using var tx = db.BeginTransaction();
        tx.Execute("CREATE TABLE test(id INTEGER PRIMARY KEY, name TEXT)");
        tx.Commit();

        var table = db.Schema.GetTable("test");
        Assert.NotNull(table);
        Assert.Equal(2, table.Columns.Count);
    }

    /// <summary>
    /// Create(path) then insert and read back rows unfiltered.
    /// </summary>
    [Fact]
    public void CreateFile_ThenInsertAndRead_ShouldWork()
    {
        using var db = SharcDatabase.Create(_dbPath);

        using (var tx = db.BeginTransaction())
        {
            tx.Execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
            tx.Commit();
        }

        using var writer = SharcWriter.From(db);
        writer.Insert("items",
            ColumnValue.FromInt64(1, 1),
            ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes("Alpha")),
            ColumnValue.FromInt64(2, 100));
        writer.Insert("items",
            ColumnValue.FromInt64(1, 2),
            ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes("Beta")),
            ColumnValue.FromInt64(2, 200));

        using var reader = db.CreateReader("items", "name", "value");
        Assert.True(reader.Read());
        Assert.Equal("Alpha", reader.GetString(0));
        Assert.Equal(100, reader.GetInt32(1));
        Assert.True(reader.Read());
        Assert.Equal("Beta", reader.GetString(0));
        Assert.Equal(200, reader.GetInt32(1));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// Create(path) then insert and read with FilterStar — verifies schema column
    /// metadata is correct so the filter can resolve column ordinals.
    /// </summary>
    [Fact]
    public void CreateFile_ThenInsertAndFilterRead_ShouldWork()
    {
        using var db = SharcDatabase.Create(_dbPath);

        using (var tx = db.BeginTransaction())
        {
            tx.Execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
            tx.Commit();
        }

        using var writer = SharcWriter.From(db);
        writer.Insert("items",
            ColumnValue.FromInt64(1, 1),
            ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes("Alpha")),
            ColumnValue.FromInt64(2, 100));
        writer.Insert("items",
            ColumnValue.FromInt64(1, 2),
            ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes("Beta")),
            ColumnValue.FromInt64(2, 200));
        writer.Insert("items",
            ColumnValue.FromInt64(1, 3),
            ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes("Gamma")),
            ColumnValue.FromInt64(2, 300));

        using var reader = db.CreateReader("items",
            FilterStar.Column("id").Eq(2L));
        Assert.True(reader.Read());
        Assert.Equal("Beta", reader.GetString(1));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// Verifies BuildNewDatabaseBytes creates 5 pages and the header pageCount matches.
    /// The audit table root is on page 5, so the header must say 5.
    /// </summary>
    [Fact]
    public void BuildNewDatabaseBytes_PageCount_ShouldBe5()
    {
        using var db = SharcDatabase.Create(_dbPath);
        var info = db.Info;
        Assert.Equal(5, info.PageCount);
    }

    /// <summary>
    /// Reproduces the Reference Graph scenario: creating 14 tables in a single transaction
    /// on a file-created database. This mirrors the reference schema DDL.
    /// </summary>
    [Fact]
    public void CreateFile_ManyTablesInOneTx_ShouldWork()
    {
        using var db = SharcDatabase.Create(_dbPath);

        using (var tx = db.BeginTransaction())
        {
            tx.Execute("CREATE TABLE IF NOT EXISTS t01(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t02(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t03(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t04(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t05(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t06(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t07(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t08(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t09(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t10(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t11(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t12(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t13(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Execute("CREATE TABLE IF NOT EXISTS t14(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
            tx.Commit();
        }

        // Verify all 14 tables exist
        var schema = db.Schema;
        for (int i = 1; i <= 14; i++)
        {
            var table = schema.GetTable($"t{i:D2}");
            Assert.NotNull(table);
            Assert.Equal(3, table.Columns.Count);
        }
    }

    /// <summary>
    /// CREATE INDEX DDL support is required for Reference Graph schema.
    /// The reference schema has 9 CREATE INDEX statements that currently throw NotSupportedException.
    /// </summary>
    [Fact]
    public void CreateFile_CreateIndex_ShouldWork()
    {
        using var db = SharcDatabase.Create(_dbPath);

        using (var tx = db.BeginTransaction())
        {
            tx.Execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, category INTEGER)");
            tx.Execute("CREATE INDEX IF NOT EXISTS idx_items_name ON items(name)");
            tx.Execute("CREATE INDEX IF NOT EXISTS idx_items_cat ON items(category)");
            tx.Commit();
        }

        // Schema should list the table (indexes are stored in sqlite_master too)
        var table = db.Schema.GetTable("items");
        Assert.NotNull(table);
        Assert.Equal(3, table.Columns.Count);
    }

    /// <summary>
    /// Pinpoints the exact threshold at which CREATE TABLE triggers CorruptPageException
    /// on a file-created database. The bug occurs when the schema B-tree on page 1 splits.
    /// The database starts with 4 system tables, so we're adding on top of those.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    public void CreateFile_NVerboseTablesInOneTx_ShouldWork(int tableCount)
    {
        // Long DDL strings (similar to reference schema) to trigger page split
        var ddls = new[]
        {
            "CREATE TABLE IF NOT EXISTS t1(id INTEGER PRIMARY KEY, fqn TEXT NOT NULL, file TEXT NOT NULL, line_start INTEGER NOT NULL, line_end INTEGER NOT NULL, kind_id INTEGER NOT NULL, in_degree INTEGER NOT NULL DEFAULT 0, out_degree INTEGER NOT NULL DEFAULT 0, blast_radius INTEGER NOT NULL DEFAULT 0, hash TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS t2(edge_id INTEGER PRIMARY KEY, source_node INTEGER NOT NULL, target_node INTEGER NOT NULL, edge_type_id INTEGER NOT NULL, weight REAL NOT NULL DEFAULT 1.0, conditional TEXT, call_freq INTEGER, first_seen TEXT NOT NULL, last_seen TEXT, conf REAL NOT NULL DEFAULT 1.0, tier_src TEXT NOT NULL DEFAULT 'scanner')",
            "CREATE TABLE IF NOT EXISTS t3(node_id INTEGER NOT NULL, trait_id INTEGER NOT NULL, value TEXT NOT NULL, weight REAL NOT NULL DEFAULT 1.0, confidence REAL NOT NULL DEFAULT 1.0, tier_src TEXT NOT NULL DEFAULT 'scanner', valid_from_sha TEXT NOT NULL DEFAULT '', stale INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (node_id, trait_id))",
            "CREATE TABLE IF NOT EXISTS t4(event_id INTEGER PRIMARY KEY, node_id INTEGER NOT NULL, event_kind TEXT NOT NULL, trait_name TEXT, old_value TEXT, new_value TEXT, commit_sha TEXT NOT NULL DEFAULT '', version_tag TEXT, timestamp TEXT NOT NULL, inference TEXT, inference_conf REAL, inference_cost INTEGER)",
            "CREATE TABLE IF NOT EXISTS t5(snap_id INTEGER PRIMARY KEY, node_id INTEGER NOT NULL, version_tag TEXT NOT NULL, timestamp TEXT NOT NULL, lines INTEGER NOT NULL, cyclomatic INTEGER NOT NULL DEFAULT 0, max_nesting INTEGER NOT NULL DEFAULT 0, in_degree INTEGER NOT NULL DEFAULT 0, out_degree INTEGER NOT NULL DEFAULT 0, blast_radius INTEGER NOT NULL DEFAULT 0, tok_traits INTEGER NOT NULL DEFAULT 0, tok_edges INTEGER NOT NULL DEFAULT 0, tok_source INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS t6(child_node INTEGER NOT NULL, parent_node INTEGER NOT NULL, lineage_kind TEXT NOT NULL, confidence REAL NOT NULL DEFAULT 1.0, inferred_by TEXT NOT NULL DEFAULT '', version_tag TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '', PRIMARY KEY (child_node, parent_node))",
            "CREATE TABLE IF NOT EXISTS t7(cache_id INTEGER PRIMARY KEY, node_id INTEGER NOT NULL, inference_kind TEXT NOT NULL, result_summary TEXT NOT NULL, tokens_spent INTEGER NOT NULL DEFAULT 0, model_used TEXT NOT NULL DEFAULT '', node_hash_at_inference TEXT NOT NULL, current_node_hash TEXT NOT NULL, stale INTEGER NOT NULL DEFAULT 0, tokens_saved_to_date INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS t8(intent_id INTEGER NOT NULL, trait_id INTEGER NOT NULL, boost_weight REAL NOT NULL DEFAULT 0.0, direction TEXT NOT NULL DEFAULT 'neutral', PRIMARY KEY (intent_id, trait_id))",
        };

        using var db = SharcDatabase.Create(_dbPath);
        using var tx = db.BeginTransaction();
        for (int i = 0; i < tableCount; i++)
            tx.Execute(ddls[i]);
        tx.Commit();

        for (int i = 1; i <= tableCount; i++)
            Assert.NotNull(db.Schema.GetTable($"t{i}"));
    }

    private static readonly string[] ReferenceDdls =
    [
        "CREATE TABLE IF NOT EXISTS ref_node_kinds(kind_id INTEGER PRIMARY KEY, kind_name TEXT NOT NULL, tier TEXT NOT NULL DEFAULT 'scanner')",
        "CREATE TABLE IF NOT EXISTS ref_trait_defs(trait_id INTEGER PRIMARY KEY, trait_name TEXT NOT NULL, value_type TEXT NOT NULL, description TEXT)",
        "CREATE TABLE IF NOT EXISTS ref_edge_type_defs(edge_type_id INTEGER PRIMARY KEY, edge_name TEXT NOT NULL, direction TEXT NOT NULL DEFAULT 'directed', tier TEXT NOT NULL DEFAULT 'scanner')",
        "CREATE TABLE IF NOT EXISTS ref_intent_defs(intent_id INTEGER PRIMARY KEY, intent_name TEXT NOT NULL, description TEXT)",
        "CREATE TABLE IF NOT EXISTS ref_nodes(node_id INTEGER PRIMARY KEY, fqn TEXT NOT NULL, file TEXT NOT NULL, line_start INTEGER NOT NULL, line_end INTEGER NOT NULL, kind_id INTEGER NOT NULL, in_degree INTEGER NOT NULL DEFAULT 0, out_degree INTEGER NOT NULL DEFAULT 0, blast_radius INTEGER NOT NULL DEFAULT 0, hash TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS ref_edges(edge_id INTEGER PRIMARY KEY, source_node INTEGER NOT NULL, target_node INTEGER NOT NULL, edge_type_id INTEGER NOT NULL, weight REAL NOT NULL DEFAULT 1.0, conditional TEXT, call_freq INTEGER, first_seen TEXT NOT NULL, last_seen TEXT, conf REAL NOT NULL DEFAULT 1.0, tier_src TEXT NOT NULL DEFAULT 'scanner')",
        "CREATE TABLE IF NOT EXISTS ref_node_traits(node_id INTEGER NOT NULL, trait_id INTEGER NOT NULL, value TEXT NOT NULL, weight REAL NOT NULL DEFAULT 1.0, confidence REAL NOT NULL DEFAULT 1.0, tier_src TEXT NOT NULL DEFAULT 'scanner', valid_from_sha TEXT NOT NULL DEFAULT '', stale INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (node_id, trait_id))",
        "CREATE TABLE IF NOT EXISTS ref_timeline(event_id INTEGER PRIMARY KEY, node_id INTEGER NOT NULL, event_kind TEXT NOT NULL, trait_name TEXT, old_value TEXT, new_value TEXT, commit_sha TEXT NOT NULL DEFAULT '', version_tag TEXT, timestamp TEXT NOT NULL, inference TEXT, inference_conf REAL, inference_cost INTEGER)",
        "CREATE TABLE IF NOT EXISTS ref_snapshots(snap_id INTEGER PRIMARY KEY, node_id INTEGER NOT NULL, version_tag TEXT NOT NULL, timestamp TEXT NOT NULL, lines INTEGER NOT NULL, cyclomatic INTEGER NOT NULL DEFAULT 0, max_nesting INTEGER NOT NULL DEFAULT 0, in_degree INTEGER NOT NULL DEFAULT 0, out_degree INTEGER NOT NULL DEFAULT 0, blast_radius INTEGER NOT NULL DEFAULT 0, tok_traits INTEGER NOT NULL DEFAULT 0, tok_edges INTEGER NOT NULL DEFAULT 0, tok_source INTEGER NOT NULL DEFAULT 0)",
        "CREATE TABLE IF NOT EXISTS ref_lineage(child_node INTEGER NOT NULL, parent_node INTEGER NOT NULL, lineage_kind TEXT NOT NULL, confidence REAL NOT NULL DEFAULT 1.0, inferred_by TEXT NOT NULL DEFAULT '', version_tag TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '', PRIMARY KEY (child_node, parent_node))",
        "CREATE TABLE IF NOT EXISTS ref_inference_cache(cache_id INTEGER PRIMARY KEY, node_id INTEGER NOT NULL, inference_kind TEXT NOT NULL, result_summary TEXT NOT NULL, tokens_spent INTEGER NOT NULL DEFAULT 0, model_used TEXT NOT NULL DEFAULT '', node_hash_at_inference TEXT NOT NULL, current_node_hash TEXT NOT NULL, stale INTEGER NOT NULL DEFAULT 0, tokens_saved_to_date INTEGER NOT NULL DEFAULT 0)",
        "CREATE TABLE IF NOT EXISTS ref_intent_weights(intent_id INTEGER NOT NULL, trait_id INTEGER NOT NULL, boost_weight REAL NOT NULL DEFAULT 0.0, direction TEXT NOT NULL DEFAULT 'neutral', PRIMARY KEY (intent_id, trait_id))",
        "CREATE TABLE IF NOT EXISTS ref_alloc_pairs(allocator_node INTEGER NOT NULL, deallocator_node INTEGER, status TEXT NOT NULL DEFAULT 'unmatched', confidence REAL NOT NULL DEFAULT 1.0, custom_allocator_macro TEXT, ownership_note TEXT NOT NULL DEFAULT '', PRIMARY KEY (allocator_node))",
        "CREATE TABLE IF NOT EXISTS ref_warnings(warning_id INTEGER PRIMARY KEY, node_id INTEGER NOT NULL, severity TEXT NOT NULL, category TEXT NOT NULL, text TEXT NOT NULL, confidence REAL NOT NULL DEFAULT 1.0, source TEXT NOT NULL DEFAULT 'scanner')",
        "CREATE INDEX IF NOT EXISTS idx_ref_nodes_fqn ON ref_nodes(fqn)",
        "CREATE INDEX IF NOT EXISTS idx_ref_nodes_file ON ref_nodes(file, line_start)",
        "CREATE INDEX IF NOT EXISTS idx_ref_edges_source ON ref_edges(source_node, edge_type_id)",
        "CREATE INDEX IF NOT EXISTS idx_ref_edges_target ON ref_edges(target_node, edge_type_id)",
        "CREATE INDEX IF NOT EXISTS idx_ref_traits_node ON ref_node_traits(node_id)",
        "CREATE INDEX IF NOT EXISTS idx_ref_timeline_node ON ref_timeline(node_id, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_ref_snapshots_node ON ref_snapshots(node_id, version_tag)",
        "CREATE INDEX IF NOT EXISTS idx_ref_warnings_node ON ref_warnings(node_id)",
        "CREATE INDEX IF NOT EXISTS idx_ref_cache_node ON ref_inference_cache(node_id, stale)",
    ];

    /// <summary>
    /// Bisect to find which DDL statement triggers the CorruptPageException.
    /// </summary>
    [Theory]
    [InlineData(5)]
    [InlineData(8)]
    [InlineData(10)]
    [InlineData(12)]
    [InlineData(14)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(18)]
    [InlineData(19)]
    [InlineData(20)]
    [InlineData(23)]
    public void CreateFile_ReferenceDdl_NStatements(int count)
    {
        using var db = SharcDatabase.Create(_dbPath);
        using var tx = db.BeginTransaction();
        for (int i = 0; i < count && i < ReferenceDdls.Length; i++)
            tx.Execute(ReferenceDdls[i]);
        tx.Commit();
    }

    /// <summary>
    /// End-to-end: create file, create tables, insert data, close/reopen, read with FilterStar.
    /// Tests persistence across close/reopen cycle with schema created via Sharc engine.
    /// </summary>
    [Fact]
    public void CreateFile_TablesInsertAndReopen_FilterWorks()
    {
        // Create and populate
        using (var db = SharcDatabase.Create(_dbPath))
        {
            using (var tx = db.BeginTransaction())
            {
                tx.Execute("CREATE TABLE nodes(id INTEGER PRIMARY KEY, fqn TEXT, kind INTEGER)");
                tx.Execute("CREATE TABLE edges(id INTEGER PRIMARY KEY, src INTEGER, tgt INTEGER, etype INTEGER)");
                tx.Execute("CREATE TABLE traits(node_id INTEGER, trait_id INTEGER, value TEXT, PRIMARY KEY(node_id, trait_id))");
                tx.Commit();
            }

            using var writer = SharcWriter.From(db);
            for (int i = 1; i <= 20; i++)
            {
                writer.Insert("nodes",
                    ColumnValue.FromInt64(1, i),
                    ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes($"node_{i}")),
                    ColumnValue.FromInt64(2, i % 5));
            }
            for (int i = 1; i <= 30; i++)
            {
                writer.Insert("edges",
                    ColumnValue.FromInt64(1, i),
                    ColumnValue.FromInt64(2, (i % 20) + 1),
                    ColumnValue.FromInt64(3, ((i + 5) % 20) + 1),
                    ColumnValue.FromInt64(4, i % 3));
            }
        }

        // Reopen and verify with FilterStar
        using (var db = SharcDatabase.Open(_dbPath))
        {
            // Unfiltered read
            using var allReader = db.CreateReader("nodes");
            int count = 0;
            while (allReader.Read()) count++;
            Assert.Equal(20, count);

            // Filtered read
            using var filteredReader = db.CreateReader("nodes",
                FilterStar.Column("id").Eq(5L));
            Assert.True(filteredReader.Read());
            Assert.Equal("node_5", filteredReader.GetString(1));
            Assert.False(filteredReader.Read());
        }
    }

    /// <summary>
    /// Real-world regression: full Reference Graph schema (14 tables + 9 indexes) in a single
    /// transaction, then insert data into multiple tables, close/reopen, and verify both
    /// unfiltered and filtered reads work. This mirrors the actual scanner pipeline.
    /// The root cause was a B-tree root split on page 1 that copied the 100-byte database
    /// header into the new left page, corrupting it for cursors that read at offset 0.
    /// </summary>
    [Fact]
    public void CreateFile_FullReferenceSchema_InsertAndReopen_FilterWorks()
    {
        // Phase 1: Create database with full reference schema
        using (var db = SharcDatabase.Create(_dbPath))
        {
            using (var tx = db.BeginTransaction())
            {
                foreach (var ddl in ReferenceDdls)
                    tx.Execute(ddl);
                tx.Commit();
            }

            // Verify all 14 tables exist
            var schema = db.Schema;
            Assert.True(schema.Tables.Count >= 14 + 4, // 14 reference + 4 system tables
                $"Expected at least 18 tables, got {schema.Tables.Count}");

            // Phase 2: Insert data into multiple reference tables
            using var writer = SharcWriter.From(db);

            // Seed node kinds
            for (int i = 1; i <= 6; i++)
            {
                writer.Insert("ref_node_kinds",
                    ColumnValue.FromInt64(1, i),
                    ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes($"kind_{i}")),
                    ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes("scanner")));
            }

            // Seed nodes
            for (int i = 1; i <= 50; i++)
            {
                writer.Insert("ref_nodes",
                    ColumnValue.FromInt64(1, i),
                    ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes($"ns::Class{i}::method")),
                    ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes($"src/file{i % 10}.c")),
                    ColumnValue.FromInt64(2, i * 10),           // line_start
                    ColumnValue.FromInt64(3, i * 10 + 20),      // line_end
                    ColumnValue.FromInt64(4, (i % 6) + 1),      // kind_id
                    ColumnValue.FromInt64(5, i % 8),             // in_degree
                    ColumnValue.FromInt64(6, i % 5),             // out_degree
                    ColumnValue.FromInt64(7, i % 3),             // blast_radius
                    ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes($"hash_{i:x8}")));
            }

            // Seed edges
            for (int i = 1; i <= 80; i++)
            {
                writer.Insert("ref_edges",
                    ColumnValue.FromInt64(1, i),
                    ColumnValue.FromInt64(2, (i % 50) + 1),     // source_node
                    ColumnValue.FromInt64(3, ((i + 7) % 50) + 1), // target_node
                    ColumnValue.FromInt64(4, (i % 3) + 1),       // edge_type_id
                    ColumnValue.FromDouble(1.0),                // weight
                    ColumnValue.Null(),                         // conditional
                    ColumnValue.FromInt64(6, i % 10),            // call_freq
                    ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes("2025-01-01")),
                    ColumnValue.Null(),                         // last_seen
                    ColumnValue.FromDouble(0.95),               // conf
                    ColumnValue.Text(2 * 10 + 13, System.Text.Encoding.UTF8.GetBytes("scanner")));
            }
        }

        // Phase 3: Reopen and verify reads
        using (var db = SharcDatabase.Open(_dbPath))
        {
            // Unfiltered: count all nodes
            using var allNodes = db.CreateReader("ref_nodes");
            int nodeCount = 0;
            while (allNodes.Read()) nodeCount++;
            Assert.Equal(50, nodeCount);

            // Unfiltered: count all edges
            using var allEdges = db.CreateReader("ref_edges");
            int edgeCount = 0;
            while (allEdges.Read()) edgeCount++;
            Assert.Equal(80, edgeCount);

            // Filtered: find node by id
            using var filteredNode = db.CreateReader("ref_nodes",
                FilterStar.Column("node_id").Eq(25L));
            Assert.True(filteredNode.Read());
            Assert.Equal("ns::Class25::method", filteredNode.GetString(1));
            Assert.False(filteredNode.Read());

            // Filtered: count node kinds
            using var kindReader = db.CreateReader("ref_node_kinds");
            int kindCount = 0;
            while (kindReader.Read()) kindCount++;
            Assert.Equal(6, kindCount);
        }
    }

    /// <summary>
    /// Regression test for root split on page 1 when schema has many tables with indexes.
    /// After root split, schema reads must still work mid-transaction (the cursor creates
    /// a new BTreeCursor on page 1 after each DDL to check IF NOT EXISTS).
    /// Previously failed with CorruptPageException 0x53 because the new left page inherited
    /// the 100-byte database header from page 1 but was read at offset 0.
    /// </summary>
    [Fact]
    public void CreateFile_RootSplitMidTransaction_SchemaReadsWork()
    {
        using var db = SharcDatabase.Create(_dbPath);
        using var tx = db.BeginTransaction();

        // Create enough tables to force a root split on page 1's B-tree.
        // The split happens around 10-12 tables with verbose DDL.
        // After the split, the next CREATE TABLE IF NOT EXISTS reads db.Schema
        // which must successfully parse the now-interior page 1.
        for (int i = 1; i <= 20; i++)
        {
            tx.Execute($"CREATE TABLE IF NOT EXISTS stress_t{i:D2}(" +
                "id INTEGER PRIMARY KEY, " +
                "name TEXT NOT NULL, " +
                "description TEXT, " +
                "category INTEGER NOT NULL DEFAULT 0, " +
                "score REAL NOT NULL DEFAULT 0.0, " +
                "created TEXT NOT NULL, " +
                "updated TEXT, " +
                "status INTEGER NOT NULL DEFAULT 1)");
        }

        // Create indexes on each table (doubles the sqlite_master records)
        for (int i = 1; i <= 20; i++)
        {
            tx.Execute($"CREATE INDEX IF NOT EXISTS idx_stress_t{i:D2}_name ON stress_t{i:D2}(name)");
            tx.Execute($"CREATE INDEX IF NOT EXISTS idx_stress_t{i:D2}_cat ON stress_t{i:D2}(category)");
        }

        tx.Commit();

        // Verify all tables and indexes were created
        var schema = db.Schema;
        for (int i = 1; i <= 20; i++)
        {
            var table = schema.GetTable($"stress_t{i:D2}");
            Assert.NotNull(table);
            Assert.Equal(8, table.Columns.Count);
        }
        // 20 tables * 2 indexes each = 40 indexes
        Assert.True(schema.Indexes.Count >= 40,
            $"Expected at least 40 indexes, got {schema.Indexes.Count}");
    }
}
