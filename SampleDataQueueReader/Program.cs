using SharpSeries.DataQueues;
using SharpSeries.Logging;

namespace SampleDataQueueReader
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

                    // 2. Drain the FIFO queue without waiting (ReadAsync(0) returns null when empty)
                    var queue = new DataQueue(connection, queueName, library);
                    Console.WriteLine($"Reading {library}/{queueName}:");
                    int count = 0;
                    DataQueueEntry? entry;
                    while ((entry = await queue.ReadAsync(waitSeconds: 0)) != null)
                    {
                        Console.WriteLine($"  [{count + 1}] {entry.GetString(connection.Ccsid)}");
                        if (entry.SenderInfo != null)
                        {
                            Console.WriteLine($"      sent by job {entry.SenderInfo}");
                        }
                        count++;
                    }
                    Console.WriteLine($"  Drained {count} entries.\n");

                    // 3. Peek the oldest entry of the keyed queue without consuming it,
                    //    then read entries back by key (GreaterThanOrEqual walks keys in order).
                    var keyedQueue = new KeyedDataQueue(connection, keyedQueueName, library);
                    KeyedDataQueueEntry? peeked = await keyedQueue.PeekAsync("CUST0001", KeySearchType.GreaterThanOrEqual);
                    if (peeked != null)
                    {
                        Console.WriteLine($"Peeked oldest keyed entry '{peeked.GetKeyString(connection.Ccsid)}': {peeked.GetString(connection.Ccsid)}");
                    }

                    Console.WriteLine($"\nReading keyed entries from {library}/{keyedQueueName}:");
                    KeyedDataQueueEntry? keyedEntry;
                    while ((keyedEntry = await keyedQueue.ReadAsync("CUST0001", KeySearchType.GreaterThanOrEqual)) != null)
                    {
                        Console.WriteLine($"  key '{keyedEntry.GetKeyString(connection.Ccsid)}' -> {keyedEntry.GetString(connection.Ccsid)}");
                    }
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
                Console.WriteLine($"\nError connecting or reading:");
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine("Exiting program.");
        }
    }
}
