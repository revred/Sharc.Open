using Microsoft.Data.Sqlite;

namespace Sharc.Comparisons;

public static class JoinDataGenerator
{
    /// <summary>
    /// Generates a skewed join dataset for stress-testing specific join shapes.
    /// </summary>
    /// <param name="dbPath">Path for the SQLite database file.</param>
    /// <param name="userCount">Total number of user rows (build side).</param>
    /// <param name="matchedFraction">Fraction of users that have at least one order (0.0–1.0).</param>
    /// <param name="hotKeyCount">Number of probe rows that all target the same "hot" user_id.</param>
    /// <param name="nullKeyCount">Number of order rows with NULL user_id (never match).</param>
    public static void GenerateSkewed(string dbPath, int userCount, double matchedFraction,
        int hotKeyCount, int nullKeyCount)
    {
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var conn = new SqliteConnection($"Data Source={dbPath};Pooling=False");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            PRAGMA journal_mode = DELETE;
            PRAGMA page_size = 4096;
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, dept TEXT NOT NULL);
            CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL NOT NULL, status TEXT NOT NULL);
        ";
        cmd.ExecuteNonQuery();

        using var tx = conn.BeginTransaction();
        var rng = new Random(42);
        string[] depts = ["Engineering", "Sales", "Marketing", "HR", "Legal"];
        string[] statuses = ["Pending", "Shipped", "Delivered", "Cancelled"];

        // Insert all users
        var userCmd = conn.CreateCommand();
        userCmd.Transaction = tx;
        userCmd.CommandText = "INSERT INTO users (id, name, dept) VALUES ($id, $name, $dept)";
        var pUId = userCmd.Parameters.Add("$id", SqliteType.Integer);
        var pUName = userCmd.Parameters.Add("$name", SqliteType.Text);
        var pUDept = userCmd.Parameters.Add("$dept", SqliteType.Text);
        for (int i = 1; i <= userCount; i++)
        {
            pUId.Value = i;
            pUName.Value = $"User {i}";
            pUDept.Value = depts[rng.Next(depts.Length)];
            userCmd.ExecuteNonQuery();
        }

        // Insert orders: matched fraction get 1 order each, rest get none
        var orderCmd = conn.CreateCommand();
        orderCmd.Transaction = tx;
        orderCmd.CommandText = "INSERT INTO orders (id, user_id, amount, status) VALUES ($id, $uid, $amt, $stat)";
        var pOId = orderCmd.Parameters.Add("$id", SqliteType.Integer);
        var pOUid = orderCmd.Parameters.Add("$uid", SqliteType.Integer);
        var pOAmt = orderCmd.Parameters.Add("$amt", SqliteType.Real);
        var pOStat = orderCmd.Parameters.Add("$stat", SqliteType.Text);
        int orderId = 1;
        int matchedUsers = Math.Max(1, (int)(userCount * matchedFraction));

        for (int i = 1; i <= matchedUsers; i++)
        {
            pOId.Value = orderId++;
            pOUid.Value = i;
            pOAmt.Value = Math.Round(rng.NextDouble() * 1000, 2);
            pOStat.Value = statuses[rng.Next(statuses.Length)];
            orderCmd.ExecuteNonQuery();
        }

        // Hot key: many probe rows all targeting user 1
        for (int i = 0; i < hotKeyCount; i++)
        {
            pOId.Value = orderId++;
            pOUid.Value = 1;
            pOAmt.Value = Math.Round(rng.NextDouble() * 1000, 2);
            pOStat.Value = statuses[rng.Next(statuses.Length)];
            orderCmd.ExecuteNonQuery();
        }

        // Null keys: user_id = NULL → never match any build row
        for (int i = 0; i < nullKeyCount; i++)
        {
            pOId.Value = orderId++;
            pOUid.Value = DBNull.Value;
            pOAmt.Value = Math.Round(rng.NextDouble() * 1000, 2);
            pOStat.Value = statuses[rng.Next(statuses.Length)];
            orderCmd.ExecuteNonQuery();
        }

        tx.Commit();
        conn.Close();
        SqliteConnection.ClearAllPools();
    }

    public static void Generate(string dbPath, int userCount, int ordersPerUser, bool createIndexes = false)
    {
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var conn = new SqliteConnection($"Data Source={dbPath};Pooling=False");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            PRAGMA journal_mode = DELETE;
            PRAGMA page_size = 4096;

            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept TEXT NOT NULL
            );

            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL
            );
        ";
        cmd.ExecuteNonQuery();

        if (createIndexes)
        {
            using var idxCmd = conn.CreateCommand();
            idxCmd.CommandText = @"
                CREATE INDEX idx_orders_user_id ON orders(user_id);
                CREATE INDEX idx_users_dept ON users(dept);
            ";
            idxCmd.ExecuteNonQuery();
        }

        using var tx = conn.BeginTransaction();
        var rng = new Random(42);

        var userCmd = conn.CreateCommand();
        userCmd.Transaction = tx;
        userCmd.CommandText = "INSERT INTO users (id, name, dept) VALUES ($id, $name, $dept)";
        var pUId = userCmd.Parameters.Add("$id", SqliteType.Integer);
        var pUName = userCmd.Parameters.Add("$name", SqliteType.Text);
        var pUDept = userCmd.Parameters.Add("$dept", SqliteType.Text);

        string[] depts = ["Engineering", "Sales", "Marketing", "HR", "Legal"];

        for (int i = 1; i <= userCount; i++)
        {
            pUId.Value = i;
            pUName.Value = $"User {i}";
            pUDept.Value = depts[rng.Next(depts.Length)];
            userCmd.ExecuteNonQuery();
        }

        var orderCmd = conn.CreateCommand();
        orderCmd.Transaction = tx;
        orderCmd.CommandText = "INSERT INTO orders (id, user_id, amount, status) VALUES ($id, $uid, $amt, $stat)";
        var pOId = orderCmd.Parameters.Add("$id", SqliteType.Integer);
        var pOUid = orderCmd.Parameters.Add("$uid", SqliteType.Integer);
        var pOAmt = orderCmd.Parameters.Add("$amt", SqliteType.Real);
        var pOStat = orderCmd.Parameters.Add("$stat", SqliteType.Text);

        string[] statuses = ["Pending", "Shipped", "Delivered", "Cancelled"];
        int orderId = 1;

        for (int i = 1; i <= userCount; i++)
        {
            for (int j = 0; j < ordersPerUser; j++)
            {
                pOId.Value = orderId++;
                pOUid.Value = i;
                pOAmt.Value = Math.Round(rng.NextDouble() * 1000, 2);
                pOStat.Value = statuses[rng.Next(statuses.Length)];
                orderCmd.ExecuteNonQuery();
            }
        }

        tx.Commit();
        conn.Close();
        SqliteConnection.ClearAllPools();
    }
}
