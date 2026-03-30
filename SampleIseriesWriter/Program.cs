using System;
using System.Threading.Tasks;
using SharpSeries.Data;
using SharpSeries.Logging;

namespace SampleIseriesWriter
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Db2Logger.Level = Db2LogLevel.Error;
            Db2Logger.LogAction = (level, message) =>
            {
                Console.WriteLine($"[{level,5}] {message}");
            };

            var envPath = System.IO.Path.Combine(System.IO.Directory.GetCurrentDirectory(), ".env");
            if (System.IO.File.Exists(envPath))
            {
                foreach (var line in System.IO.File.ReadAllLines(envPath))
                {
                    if (string.IsNullOrWhiteSpace(line) || line.StartsWith("#")) continue;
                    var parts = line.Split('=', 2, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length == 2)
                    {
                        Environment.SetEnvironmentVariable(parts[0].Trim(), parts[1].Trim());
                    }
                }
            }

            string system = Environment.GetEnvironmentVariable("DB2_SYSTEM") ?? "YOUR_SYSTEM_IP";
            string user = Environment.GetEnvironmentVariable("DB2_USER") ?? "YOUR_USER";
            string password = Environment.GetEnvironmentVariable("DB2_PASSWORD") ?? "YOUR_PASSWORD";

            string connectionString = $"Server={system};" +
                                      $"User ID={user};" +
                                      $"Password={password};" +
                                      $"Naming=SQL";

            Console.WriteLine("Connecting to IBM iSeries in write mode...");

            try
            {
                using (Db2Connection connection = new Db2Connection(connectionString))
                {
                    await connection.OpenAsync();
                    Console.WriteLine("Connected.\n");

                    string schema = "QTMP";
                    string table = "TEST123";
                    string qualifiedTable = $"{schema}.{table}";

                    // 1. Create table
                    Console.WriteLine($"Creating table {qualifiedTable}...");
                    try
                    {
                        using (var cmd = new Db2Command(
                            $"CREATE TABLE {qualifiedTable} (DESCRIPTION VARCHAR(50) NOT NULL)", connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        Console.WriteLine("Table created.\n");
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("-601"))
                    {
                        Console.WriteLine("Table already exists, dropping and recreating...\n");
                        using (var dropCmd = new Db2Command(
                            $"DROP TABLE {qualifiedTable}", connection))
                        {
                            dropCmd.ExecuteNonQuery();
                        }
                        using (var createCmd = new Db2Command(
                            $"CREATE TABLE {qualifiedTable} (DESCRIPTION VARCHAR(50) NOT NULL)", connection))
                        {
                            createCmd.ExecuteNonQuery();
                        }
                        Console.WriteLine("Table recreated.\n");
                    }

                    // 2. Insert a record
                    Console.WriteLine("Inserting a record...");
                    using (var cmd = new Db2Command(
                        $"INSERT INTO {qualifiedTable} (DESCRIPTION) VALUES ('Hello from SharpSeries!')", connection))
                    {
                        int rows = cmd.ExecuteNonQuery();
                        Console.WriteLine($"Inserted {rows} row(s).\n");
                    }

                    // 3. Select and print results
                    Console.WriteLine("Querying table...");
                    using (var cmd = new Db2Command($"SELECT * FROM {qualifiedTable}", connection))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        int fieldCount = reader.FieldCount;
                        for (int i = 0; i < fieldCount; i++)
                        {
                            Console.Write($"{reader.GetName(i),-20} | ");
                        }
                        Console.WriteLine();
                        Console.WriteLine(new string('-', fieldCount * 23));

                        int rowCount = 0;
                        while (await reader.ReadAsync())
                        {
                            for (int i = 0; i < fieldCount; i++)
                            {
                                string? val = reader.IsDBNull(i) ? "NULL" : reader[i].ToString();
                                Console.Write($"{val,-20} | ");
                            }
                            Console.WriteLine();
                            rowCount++;
                        }
                        Console.WriteLine($"\nReturned {rowCount} row(s).");
                    }

                    // 4. Drop table
                    Console.WriteLine($"\nDropping table {qualifiedTable}...");
                    try
                    {
                        using (var cmd = new Db2Command($"DROP TABLE {qualifiedTable}", connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        Console.WriteLine("Table dropped.");
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("-615"))
                    {
                        Console.WriteLine("Table locked, retrying drop...");
                        await Task.Delay(1000);
                        using (var cmd = new Db2Command($"DROP TABLE {qualifiedTable}", connection))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        Console.WriteLine("Table dropped on retry.");
                    }
                }
                Console.WriteLine("\nDisconnected from IBM iSeries.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nError:");
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine("Exiting program.");
        }
    }
}
