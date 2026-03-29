# SharpSeries

[![Build & Test](https://github.com/tomdertechie/SharpSeries/actions/workflows/ci.yml/badge.svg)](https://github.com/tomdertechie/SharpSeries/actions/workflows/ci.yml)
[![Publish to NuGet](https://github.com/tomdertechie/SharpSeries/actions/workflows/publish.yml/badge.svg)](https://github.com/tomdertechie/SharpSeries/actions/workflows/publish.yml)
[![NuGet](https://img.shields.io/nuget/v/SharpSeries?label=NuGet)](https://www.nuget.org/packages/SharpSeries)
[![License: IPL-1.0](https://img.shields.io/badge/License-IPL--1.0-blue.svg)](LICENSE)

**A pure C# ADO.NET data provider for IBM i Db2 (AS/400, System i).**

SharpSeries implements the DRDA wire protocol directly in managed C# — no proprietary IBM client libraries, no native dependencies, no client access licenses required. It works on any platform .NET runs on.

## Features

- **Pure managed C#** — no IBM client software or native dependencies
- **Full ADO.NET implementation** — `DbConnection`, `DbCommand`, `DbDataReader`, `DbTransaction`, `DbParameter`
- **DRDA wire protocol** — native implementation of the Distributed Relational Database Architecture protocol
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

## Connection Strings

| Parameter   | Description                                      | Default          |
|-------------|--------------------------------------------------|------------------|
| **Server**  | Hostname or IP of the IBM i system               | *(required)*     |
| **User ID** | IBM i user profile                               | *(required)*     |
| **Password**| IBM i password                                   | *(required)*     |
| **Database**| RDB name (system name)                           | *(empty)*        |
| **Naming**  | `SQL` (schema.table) or `System` (library/file)  | `SQL`            |
| **CCSID**   | EBCDIC character set identifier                  | `37` (US English)|

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
├── HostServer/        # DRDA host server communication
│   ├── HostServerConnectionManager
│   ├── QueryExecutor
│   └── QueryResult
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
- **IBM i** system with DRDA host server accessible (port 449 for server mapper, typically port 8471 for the database host server)

## Documentation

See the [User Guide](USERGUIDE.md) for detailed documentation on connection strings, transactions, naming conventions, CCSID configuration, and more.

## Samples

Two sample applications are included:

- **SampleIseriesReader** — connects and runs a configurable SELECT query
- **SampleIseriesWriter** — demonstrates CREATE TABLE → INSERT → SELECT → DROP TABLE

Both use a `.env` file for credentials:

```text
DB2_SYSTEM=as400.example.com
DB2_USER=MYUSER
DB2_PASSWORD=MYPASSWORD
```

## Building from Source

```bash
git clone https://github.com/tomdertechie/SharpSeries.git
cd SharpSeries
dotnet build
dotnet test
```

## License

This project is licensed under the [IBM Public License Version 1.0](LICENSE).

Portions of this software are derivative works of [JTOpen](https://github.com/IBM/JTOpen), the IBM Toolbox for Java.
