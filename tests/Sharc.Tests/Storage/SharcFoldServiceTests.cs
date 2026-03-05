// Copyright (c) Ram Revanur. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Text;
using Sharc.Core;
using Sharc.Storage;
using Sharc.Trust;
using Sharc.Tests.Trust;
using Sharc.Core.Primitives;
using Xunit;

namespace Sharc.Tests.Storage;

public class SharcFoldServiceTests
{
    [Fact]
    public void FoldFast_ReordersAndSwapsTable()
    {
         using var db = SharcDatabase.CreateInMemory();
         
         // Setup: Agent
         var registry = new AgentRegistry(db);
         using var alice = new SharcSigner("alice");
         var agentRecord = TrustTestFixtures.CreateValidAgent(alice, end: DateTimeOffset.UtcNow.AddDays(1).ToUnixTimeSeconds());
         registry.RegisterAgent(agentRecord);
         
         // Setup: Table and Data
         using (var scope = SharcWriter.Scope(db))
         {
             scope.Execute("CREATE TABLE test_data (id INTEGER, category INTEGER, score INTEGER)");
             scope.Insert("test_data", ColumnValue.FromInt64(4, 1), ColumnValue.FromInt64(4, 2), ColumnValue.FromInt64(4, 10)); // B, 10
             scope.Insert("test_data", ColumnValue.FromInt64(4, 2), ColumnValue.FromInt64(4, 1), ColumnValue.FromInt64(4, 20)); // A, 20
             scope.Insert("test_data", ColumnValue.FromInt64(4, 3), ColumnValue.FromInt64(4, 2), ColumnValue.FromInt64(4, 5));  // B, 5
             scope.Complete();
         }
         
         // Act
         db.FoldFast("alice", "test_data", new[] { "category", "score" });
         
         // Assert: check order
         using var reader = db.CreateReader("test_data");
         Assert.True(reader.Read());
         Assert.Equal(2, reader.GetInt64(0)); // A, 20.0 (id=2)
         Assert.True(reader.Read());
         Assert.Equal(3, reader.GetInt64(0)); // B, 5.0 (id=3)
         Assert.True(reader.Read());
         Assert.Equal(1, reader.GetInt64(0)); // B, 10.5 (id=1)
         Assert.False(reader.Read());
    }

    [Fact]
    public void FoldFast_RequiresEntitlement()
    {
         using var db = SharcDatabase.CreateInMemory();
         using (var scope = SharcWriter.Scope(db))
         {
             scope.Execute("CREATE TABLE test_data (id INTEGER PRIMARY KEY)");
             scope.Complete();
         }

         Assert.Throws<UnauthorizedAccessException>(() => db.FoldFast("unknown", "test_data", new[] { "id" }));
    }

    [Fact]
    public void FoldMax_CompressesColumnsIntoBlob_And_Unfold_RestoresThem()
    {
         using var db = SharcDatabase.CreateInMemory();
         
         // Setup: Agent
         var registry = new AgentRegistry(db);
         using var alice = new SharcSigner("alice");
         var agentRecord = TrustTestFixtures.CreateValidAgent(alice, end: DateTimeOffset.UtcNow.AddDays(1).ToUnixTimeSeconds());
         registry.RegisterAgent(agentRecord);
         
         // Setup: Table and Data
         using (var scope = SharcWriter.Scope(db))
         {
             scope.Execute("CREATE TABLE compress_test (id INTEGER, category INTEGER, score REAL, label TEXT)");
             scope.Insert("compress_test", ColumnValue.FromInt64(4, 1), ColumnValue.FromInt64(4, 100), ColumnValue.FromDouble(1.5), ColumnValue.Text(0, Encoding.UTF8.GetBytes("Alpha")));
             scope.Insert("compress_test", ColumnValue.FromInt64(4, 2), ColumnValue.FromInt64(4, 200), ColumnValue.FromDouble(2.5), ColumnValue.Text(0, Encoding.UTF8.GetBytes("Beta")));
             scope.Insert("compress_test", ColumnValue.FromInt64(4, 3), ColumnValue.FromInt64(4, 300), ColumnValue.FromDouble(3.5), ColumnValue.Text(0, Encoding.UTF8.GetBytes("Gamma")));
             scope.Complete();
         }
         
         // Step 1: FoldFast (Sort)
         db.FoldFast("alice", "compress_test", new[] { "id" });

         // Step 2: FoldMax (Compress)
         db.FoldMax("alice", "compress_test");

         // Assert: The table should now be folded with a single BLOB column
         using (var reader = db.CreateReader("compress_test"))
         {
             Assert.True(reader.Read());
             var type = reader.GetColumnType(0);
             Assert.Equal(SharcColumnType.Blob, type); // Folded state
             Assert.False(reader.Read()); // Should only be 1 row!
         }

         // Step 3: Unfold (Decompress)
         db.Unfold("alice", "compress_test");

         // Assert: The table should be back to its original schema and row count
         using (var reader2 = db.CreateReader("compress_test"))
         {
             Assert.True(reader2.Read());
             Assert.Equal(1, reader2.GetInt64(0));
             Assert.Equal(100, reader2.GetInt64(1));
             Assert.Equal(1.5, reader2.GetDouble(2));
             Assert.Equal("Alpha", reader2.GetString(3));
             
             Assert.True(reader2.Read());
             Assert.True(reader2.Read());
             Assert.False(reader2.Read()); // Total 3 rows again
         }
    }
}
