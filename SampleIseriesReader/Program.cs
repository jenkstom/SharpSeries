using System;
using System.Threading.Tasks;
using SharpSeries.Data;
using SharpSeries.Logging;

namespace SampleIseriesReader
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Configure SharpSeries Logging
            //Db2Logger.Level = Db2LogLevel.Trace; // Change to Debug, Info, etc.
            Db2Logger.Level = Db2LogLevel.Error; // Change to Debug, Info, etc.
            Db2Logger.LogAction = (level, message) =>
            {
                Console.WriteLine($"[{level,5}] {message}");
            };

            // Set up variables - read from .env file or default
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
            string query = Environment.GetEnvironmentVariable("DB2_QUERY") ?? "SELECT * FROM QSYS2.SYSTABLES FETCH FIRST 10 ROWS ONLY";

            // Important considerations for SharpSeries:
            string connectionString = $"Server={system};" +
                                      $"User ID={user};" +
                                      $"Password={password};" +
                                      $"Naming=SQL;Read Only=true";

            Console.WriteLine("Connecting to IBM iSeries in read-only mode...");

            try
            {
                using (Db2Connection connection = new Db2Connection(connectionString))
                {
                    // 1. Connect
                    await connection.OpenAsync();
                    Console.WriteLine("Successfully connected.\n");

                    using (Db2Command command = new Db2Command(query, connection))
                    {
                        Console.WriteLine($"Running query: {query}\n");

                        // 2. Run Query
                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            int fieldCount = reader.FieldCount;

                            // Print column headers
                            for (int i = 0; i < fieldCount; i++)
                            {
                                Console.Write($"{reader.GetName(i),-15} | ");
                            }
                            Console.WriteLine();
                            Console.WriteLine(new string('-', fieldCount * 18));

                            // 3. Print Results
                            int rowCount = 0;
                            while (await reader.ReadAsync())
                            {
                                for (int i = 0; i < fieldCount; i++)
                                {
                                    string? val = reader.IsDBNull(i) ? "NULL" : reader[i].ToString();
                                    Console.Write($"{val,-15} | ");
                                }
                                Console.WriteLine();
                                rowCount++;
                            }

                            Console.WriteLine($"\nReturned {rowCount} rows.");
                        } 
                    } 
                    // 4. Disconnect (Db2Connection implicitly closes/disconnects on Dispose)
                }
                Console.WriteLine("\nDisconnected from IBM iSeries.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nError connecting or executing query:");
                Console.WriteLine(ex.Message);
            }
            
            // 5. Exit
            Console.WriteLine("Exiting program.");
        }
    }
}
