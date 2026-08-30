using SharpSeries.DataQueues;
using SharpSeries.Logging;

namespace SampleDataQueueWriter
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Configure SharpSeries Logging
            //Db2Logger.Level = Db2LogLevel.Trace; // Change to Debug, Info, etc. to inspect the wire protocol.
            Db2Logger.Level = Db2LogLevel.Error;
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

            string system = Environment.GetEnvironmentVariable("DTAQ_SYSTEM") ?? Environment.GetEnvironmentVariable("DB2_SYSTEM") ?? "YOUR_SYSTEM_IP";
            string user = Environment.GetEnvironmentVariable("DTAQ_USER") ?? Environment.GetEnvironmentVariable("DB2_USER") ?? "YOUR_USER";
            string password = Environment.GetEnvironmentVariable("DTAQ_PASSWORD") ?? Environment.GetEnvironmentVariable("DB2_PASSWORD") ?? "YOUR_PASSWORD";
            string queueName = Environment.GetEnvironmentVariable("DTAQ_QUEUE") ?? "SHARPDQ";
            string keyedQueueName = Environment.GetEnvironmentVariable("DTAQ_KEYED_QUEUE") ?? "SHARPKDQ";
            string library = Environment.GetEnvironmentVariable("DTAQ_LIBRARY") ?? "QGPL";

            string connectionString = $"Server={system};User ID={user};Password={password};CCSID=37";

            Console.WriteLine("Connecting to IBM i data queue server...");

            try
            {
                using (var connection = new DataQueueConnection(connectionString))
                {
                    // 1. Connect (sign-on + data queue exchange-attributes handshake)
                    await connection.OpenAsync();
                    Console.WriteLine("Successfully connected.\n");

                    // 2. Write entries to a FIFO data queue
                    //    Prerequisite, run once on the IBM i:
                    //    CRTDTAQ DTAQ(QGPL/SHARPDQ) MAXLEN(64000)
                    var queue = new DataQueue(connection, queueName, library);
                    for (int i = 1; i <= 5; i++)
                    {
                        await queue.WriteAsync($"Entry number {i} at {DateTime.Now:HH:mm:ss}");
                        Console.WriteLine($"Wrote entry {i} to {library}/{queueName}");
                    }

                    // 3. Write entries under keys to a keyed data queue
                    //    Prerequisite, run once on the IBM i:
                    //    CRTDTAQ DTAQ(QGPL/SHARPKDQ) TYPE(*KEYED) MAXLEN(64000) KEYLEN(8)
                    var keyedQueue = new KeyedDataQueue(connection, keyedQueueName, library);
                    foreach (string customer in new[] { "CUST0042", "CUST0017", "CUST0099" })
                    {
                        await keyedQueue.WriteAsync(key: customer, data: $"Order payload for {customer}");
                        Console.WriteLine($"Wrote keyed entry '{customer}' to {library}/{keyedQueueName}");
                    }

                    // 4. Show the queue attributes we just wrote to
                    DataQueueAttributes attrs = await queue.GetAttributesAsync();
                    Console.WriteLine($"\n{library}/{queueName} attributes: {attrs}");
                    DataQueueAttributes keyedAttrs = await keyedQueue.GetAttributesAsync();
                    Console.WriteLine($"{library}/{keyedQueueName} attributes: {keyedAttrs}");
                }
                Console.WriteLine("\nDisconnected from IBM i.");
            }
            catch (DataQueueException ex)
            {
                Console.WriteLine($"\nData queue error (return code 0x{ex.ReturnCode:X4}, message ID {ex.MessageId ?? "n/a"}):");
                Console.WriteLine(ex.Message);
                Console.WriteLine("\nHint: the queues must exist first, e.g.:");
                Console.WriteLine($"  CRTDTAQ DTAQ({library}/{queueName}) MAXLEN(64000)");
                Console.WriteLine($"  CRTDTAQ DTAQ({library}/{keyedQueueName}) TYPE(*KEYED) MAXLEN(64000) KEYLEN(8)");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nError connecting or writing:");
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine("Exiting program.");
        }
    }
}
