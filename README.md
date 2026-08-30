# SharpSeries

[![Build & Test](https://github.com/jenkstom/SharpSeries/actions/workflows/ci.yml/badge.svg)](https://github.com/jenkstom/SharpSeries/actions/workflows/ci.yml)
[![Publish to NuGet](https://github.com/jenkstom/SharpSeries/actions/workflows/publish.yml/badge.svg)](https://github.com/jenkstom/SharpSeries/actions/workflows/publish.yml)
[![NuGet](https://img.shields.io/nuget/v/SharpSeries?label=NuGet)](https://www.nuget.org/packages/SharpSeries)
[![License: IPL-1.0](https://img.shields.io/badge/License-IPL--1.0-blue.svg)](LICENSE)

**A pure C# ADO.NET data provider and data queue client for IBM i (AS/400, System i).**

SharpSeries implements the DRDA wire protocol directly in managed C# — no proprietary IBM client libraries, no native dependencies, no client access licenses required. It works on any platform .NET runs on. It also speaks the native Data Queue Host Server protocol, so you can read, write, and peek `*DTAQ` entries (FIFO, LIFO, and keyed queues) from the same package.

## Features

- **Pure managed C#** — no IBM client software or native dependencies
- **Full ADO.NET implementation** — `DbConnection`, `DbCommand`, `DbDataReader`, `DbTransaction`, `DbParameter`
- **DRDA wire protocol** — native implementation of the Distributed Relational Database Architecture protocol
- **IBM i data queues** — write, read (destructive), and peek `*DTAQ` entries, including keyed queues with EQ/NE/GT/GE/LT/LE key search and sender information
- **Connection pooling** — built-in connection pool for high-throughput workloads
- **Transaction support** — explicit commit/rollback with automatic auto-commit for standalone statements
- **EBCDIC/CCSID support** — configurable character set conversion for international IBM i systems
- **SQL & System naming** — supports both `SCHEMA.TABLE` and `LIBRARY/FILE` naming conventions
- **Async & sync APIs** — full `async/await` support alongside synchronous methods

## Quick Start

### Install the NuGet package

```bash
dotnet add package SharpSeries
```

### Connect and query

```csharp
using SharpSeries.Data;

string connString = "Server=10.0.0.5;User ID=myuser;Password=mypass;";

using var connection = new Db2Connection(connString);
await connection.OpenAsync();

using var command = connection.CreateCommand();
command.CommandText = "SELECT * FROM QSYS2.SYSTABLES FETCH FIRST 10 ROWS ONLY";

using var reader = await command.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine(reader["TABLE_NAME"]);
}
```

### ExecuteNonQuery — INSERT, UPDATE, DELETE

```csharp
using var cmd = connection.CreateCommand();
cmd.CommandText = "UPDATE MYLIB.CUSTOMERS SET STATUS = 'ACTIVE' WHERE ID = 123";
int rows = cmd.ExecuteNonQuery();
```

### Transactions

```csharp
using var tx = connection.BeginTransaction();
try
{
    using var cmd1 = connection.CreateCommand();
    cmd1.Transaction = tx;
    cmd1.CommandText = "INSERT INTO MYLIB.ORDERS (ITEM) VALUES ('Widget')";
    cmd1.ExecuteNonQuery();

    using var cmd2 = connection.CreateCommand();
    cmd2.Transaction = tx;
    cmd2.CommandText = "UPDATE MYLIB.INVENTORY SET QTY = QTY - 1 WHERE ITEM = 'Widget'";
    cmd2.ExecuteNonQuery();

    tx.Commit();
}
catch
{
    tx.Rollback();
    throw;
}
```

## Data Queues

Read, write, and peek IBM i data queues (`*DTAQ` objects) over the native data queue host server (QZHQSSRV). The same connection string keys as `Db2Connection` are used:

```csharp
using SharpSeries.DataQueues;

using var connection = new DataQueueConnection("Server=10.0.0.5;User ID=myuser;Password=mypass;");
await connection.OpenAsync();

var queue = new DataQueue(connection, "ORDERQ", library: "APPLIB");

// Write an entry (string is encoded with the connection CCSID, or pass raw bytes)
await queue.WriteAsync("Hello from SharpSeries");

// Read the oldest entry: 0 = no wait, N = wait up to N seconds, -1 = wait forever
DataQueueEntry? entry = await queue.ReadAsync(waitSeconds: 30);
Console.WriteLine(entry?.GetString(37));

// Peek without consuming, and inspect queue attributes
DataQueueEntry? peeked = await queue.PeekAsync();
DataQueueAttributes attrs = await queue.GetAttributesAsync();
```

Keyed queues address entries by key with a comparison operator:

```csharp
var keyed = new KeyedDataQueue(connection, "ORDERQ", "APPLIB");
await keyed.WriteAsync(key: "CUST0042", data: orderPayload);
KeyedDataQueueEntry? e = await keyed.ReadAsync("CUST0042", KeySearchType.Equal, waitSeconds: 10);
```

Queues must already exist on the system (create them with `CRTDTAQ`). A read with a wait time holds the connection's server session until an entry arrives or the wait expires, so give concurrent long-waiting consumers their own `DataQueueConnection`.

## Connection Strings

| Parameter   | Description                                      | Default          |
|-------------|--------------------------------------------------|------------------|
| **Server**  | Hostname or IP of the IBM i system               | *(required)*     |
| **User ID** | IBM i user profile                               | *(required)*     |
| **Password**| IBM i password                                   | *(required)*     |
| **Database**| RDB name (system name)                           | *(empty)*        |
| **Naming**  | `SQL` (schema.table) or `System` (library/file)  | `SQL`            |
| **CCSID**   | EBCDIC character set identifier                  | `37` (US English)|
| **Read Only** | `true` to prevent write operations              | `false`          |

```text
Server=192.168.1.100;User ID=MYUSER;Password=MYPASS;Naming=SQL;
```

## Architecture

```
SharpSeries/
├── Data/              # ADO.NET provider implementation
│   ├── Db2Connection
│   ├── Db2Command
│   ├── Db2DataReader
│   ├── Db2Parameter
│   ├── Db2Transaction
│   └── Db2ProviderFactory
├── DataQueues/        # IBM i data queue API (FIFO/LIFO/keyed)
│   ├── DataQueueConnection
│   ├── DataQueue / KeyedDataQueue
│   └── DataQueueEntry / DataQueueAttributes
├── HostServer/        # Host server wire protocols
│   ├── HostServerSessionBase (shared sign-on/mapper)
│   ├── HostServerConnectionManager (SQL)
│   ├── DataQueueConnectionManager (data queues)
│   ├── QueryExecutor
│   └── DataQueueExecutor
├── Network/           # Network stream handling
├── Security/          # DES password encryption
├── Encoding/          # CCSID/EBCDIC conversion
├── Types/             # Db2 type converters (DateTime, Decimal, String)
├── Pool/              # Connection pooling
└── Logging/           # Built-in diagnostics logger
```

## Logging

SharpSeries includes a built-in logger for diagnostics:

```csharp
using SharpSeries.Logging;

Db2Logger.Level = Db2LogLevel.Trace;
Db2Logger.LogAction = (level, message) => Console.WriteLine($"[{level}] {message}");
```

## Requirements

- **.NET 10.0+**
- **IBM i** system with host servers accessible (port 449 for the server mapper; typically port 8471 for the database host server and 8474 for the data queue host server)

## Documentation

See the [User Guide](USERGUIDE.md) for detailed documentation on connection strings, transactions, naming conventions, CCSID configuration, and more.

## Samples

Four sample applications are included:

- **SampleIseriesReader** — connects and runs a configurable SELECT query
- **SampleIseriesWriter** — demonstrates CREATE TABLE → INSERT → SELECT → DROP TABLE
- **SampleDataQueueWriter** — writes entries to a FIFO and a keyed data queue
- **SampleDataQueueReader** — reads and peeks entries, showing sender information and attributes

Both pairs use a `.env` file for credentials:

```text
DB2_SYSTEM=as400.example.com
DB2_USER=MYUSER
DB2_PASSWORD=MYPASSWORD
```

The data queue samples additionally read `DTAQ_QUEUE`, `DTAQ_KEYED_QUEUE`, and `DTAQ_LIBRARY`.

## Building from Source

```bash
git clone https://github.com/jenkstom/SharpSeries.git
cd SharpSeries
dotnet build
dotnet test
```

## License

This project is licensed under the [IBM Public License Version 1.0](LICENSE).

Portions of this software are derivative works of [JTOpen](https://github.com/IBM/JTOpen), the IBM Toolbox for Java.
