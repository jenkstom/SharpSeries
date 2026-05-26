# User Guide: SharpSeries — C# ADO.NET Provider for IBM i Db2

Welcome to the **SharpSeries** driver user guide. This library is a high-performance ADO.NET data provider built specifically for IBM DB2 on iSeries (AS/400, System i). It implements the DRDA wire protocol directly in pure, managed C# without requiring any proprietary IBM binaries or client access licenses.

## Table of Contents
1. [Installation](#installation)
2. [Connection Strings](#connection-strings)
3. [Connecting to the Database](#connecting-to-the-database)
4. [Executing Commands](#executing-commands)
5. [Reading Data](#reading-data)
6. [Transactions](#transactions)
7. [Auto-Commit Behavior](#auto-commit-behavior)
8. [iSeries Specifics](#iseries-specifics)
9. [Samples](#samples)
10. [License](#license)

---

## Installation

Currently, the driver is available as a local project reference. To use it in your .NET application, add a reference to the `SharpSeries.csproj` file:

```bash
dotnet add reference /path/to/SharpSeries/SharpSeries.csproj
```

Ensure your project is targeting a compatible framework (the library targets `net10.0`).

---

## Connection Strings

The `Db2Connection` uses standard ADO.NET connection strings. The following parameters are supported:

| Parameter | Description | Default |
|-----------|-------------|---------|
| **Server** | The hostname or IP address of the IBM i system | (required) |
| **Port** | Not used for connection. The driver always queries the IBM i Server Mapper (port 449) to discover the database host server port (typically 8471). | `446` (unused) |
| **Database** | The RDB name (e.g., the system name) | (empty) |
| **User ID** | Your IBM i user profile | (required) |
| **Password** | Your IBM i password | (required) |
| **Naming** | Naming convention: `SQL` (schema.table) or `System` (library/file) | `SQL` |
| **CCSID** | EBCDIC Character Set Identifier for string conversions | `37` (US English) |
| **Read Only** | Set to `true` to open the connection in read-only mode. Prevents `ExecuteNonQuery` calls (INSERT, UPDATE, DELETE, DDL). | `false` |

**Example:**
```text
Server=192.168.1.100;User ID=MYUSER;Password=MYPASS;Naming=SQL;
```

**Read-only example:**
```text
Server=192.168.1.100;User ID=MYUSER;Password=MYPASS;Read Only=true;
```

---

## Connecting to the Database

Connecting is as simple as using any other ADO.NET provider. The driver automatically handles connection pooling in the background to maximize performance.

```csharp
using SharpSeries.Data;

string connString = "Server=10.0.0.5;User ID=admin;Password=secret;";

using (var connection = new Db2Connection(connString))
{
    await connection.OpenAsync();
    Console.WriteLine("Connected!");
}
// Connection is automatically returned to the pool on Dispose
```

---

## Executing Commands

### ExecuteNonQuery — INSERT, UPDATE, DELETE, DDL

```csharp
using var command = connection.CreateCommand();
command.CommandText = "UPDATE MYLIB.CUSTOMERS SET STATUS = 'ACTIVE' WHERE ID = 123";

int rowsAffected = command.ExecuteNonQuery();
Console.WriteLine($"{rowsAffected} rows updated.");
```

### ExecuteScalar — Return a Single Value

```csharp
using var command = connection.CreateCommand();
command.CommandText = "SELECT COUNT(*) FROM MYLIB.CUSTOMERS";

object result = command.ExecuteScalar();
Console.WriteLine($"Total customers: {result}");
```

---

## Reading Data

To read result sets, use a `Db2DataReader`. The reader fetches all rows into memory when opened and iterates them locally.

```csharp
using var command = connection.CreateCommand();
command.CommandText = "SELECT ID, NAME, BALANCE FROM MYLIB.CUSTOMERS";

using (var reader = command.ExecuteReader())
{
    while (reader.Read())
    {
        Console.WriteLine($"ID={reader["ID"]}, Name={reader["NAME"]}, Balance={reader["BALANCE"]}");
    }
}
// The cursor is automatically closed when the reader is disposed
```

---

## Transactions

The driver natively supports DRDA transactions via `RDBCMM` (commit) and `RDBRLLBCK` (rollback).

```csharp
using var transaction = connection.BeginTransaction();

try
{
    using var command1 = connection.CreateCommand();
    command1.Transaction = transaction;
    command1.CommandText = "INSERT INTO MYLIB.LOG (ENTRY) VALUES ('Event 1')";
    command1.ExecuteNonQuery();

    using var command2 = connection.CreateCommand();
    command2.Transaction = transaction;
    command2.CommandText = "INSERT INTO MYLIB.LOG (ENTRY) VALUES ('Event 2')";
    command2.ExecuteNonQuery();

    transaction.Commit();
    Console.WriteLine("Transaction committed.");
}
catch (Exception ex)
{
    transaction.Rollback();
    Console.WriteLine($"Transaction rolled back: {ex.Message}");
}
```

---

## Auto-Commit Behavior

When `ExecuteNonQuery` is called **without** an explicit transaction, the driver automatically commits after each successful statement. If a statement fails, the driver rolls back before throwing. This means:

- **INSERT / UPDATE / DELETE** — auto-committed immediately after execution.
- **DDL (CREATE TABLE, DROP TABLE, etc.)** — auto-committed immediately after execution.
- **Failed statements** — rolled back automatically; no uncommitted state is left on the connection.

When an explicit transaction is started via `BeginTransaction()`, auto-commit is **not** applied. You must call `Commit()` or `Rollback()` explicitly.

---

## iSeries Specifics

### Naming Conventions

IBM i supports two naming conventions for qualifying objects:

- **SQL Naming** (`Naming=SQL`): Uses a period separator — `SELECT * FROM SCHEMA.TABLE`
- **System Naming** (`Naming=System`): Uses a slash separator — `SELECT * FROM LIBRARY/FILE`

### EBCDIC Encoding (CCSID)

Because the IBM i stores text data natively in EBCDIC, all string conversions over the DRDA wire must account for specific character sets (CCSID).

- The driver leverages `System.Text.Encoding.CodePages` for highly optimized conversions.
- The default `CCSID=37` (Standard US English) is used if not specified.
- If your system uses a different language character set (e.g., German, Japanese DBCS), ensure you set the correct `CCSID=xxx` in your connection string so that text like `VARCHAR` and `CHAR` decodes accurately.

---

## Samples

Two sample applications are included:

### SampleIseriesReader

A read-only sample that connects to IBM i, executes a configurable SELECT query, and prints results in a formatted table. Set the query via the `DB2_QUERY` environment variable (or `.env` file).

### SampleIseriesWriter

A read/write sample that demonstrates the full DML lifecycle:
1. **CREATE TABLE** — creates a test table
2. **INSERT** — inserts a row
3. **SELECT** — reads the row back
4. **DROP TABLE** — cleans up

Both samples use a `.env` file in the project directory for credentials:

```text
DB2_SYSTEM=as400.example.com
DB2_USER=MYUSER
DB2_PASSWORD=MYPASSWORD
```

To run a sample:

```bash
cd SampleIseriesWriter
dotnet run
```

---

## License

This project is licensed under the **IBM Public License Version 1.0 (IPL v1.0)**.

Portions of this software are derivative works of [JTOpen](https://github.com/IBM/JTOpen), the IBM Toolbox for Java. The original JTOpen source code is:

- Copyright (C) 2011-2012 International Business Machines Corporation and others.
- Licensed under the IBM Public License Version 1.0.

### What this means for you

| Right | Allowed? |
|------|----------|
| Use in your own applications (including commercial) | Yes |
| Modify the source code | Yes |
| Distribute modified or unmodified copies | Yes |
| Use in closed-source / proprietary applications | Yes |
| Sublicense under different terms | No — derivative works of the IPL-covered files must remain under the IPL when distributed in source form |

### IPL Compliance Summary

If you distribute this library (modified or unmodified), you must:

1. **Include a copy of the IPL license** — the `LICENSE` file in this repository, or a copy of the IBM Public License v1.0.
2. **Preserve copyright notices** — do not remove the IBM copyright attribution from source files that carry it.
3. **Preserve change notices** — files derived from JTOpen are marked as modified; do not remove these notices.
4. **If distributing in binary form only** (e.g., as a NuGet package without source), your license agreement must:
   - Disclaim all warranties and conditions on behalf of all contributors (the IPL provides no warranty — Section 5).
   - Exclude all liability for damages on behalf of all contributors (Section 6).
   - Inform recipients how to obtain the source code.

### Patent Grant

The IPL includes a reciprocal patent grant (Section 2(b)). Each contributor grants recipients a royalty-free patent license for their contributions. This grant terminates if you initiate patent litigation against any contributor alleging the software infringes your patents (Section 7). See the full license text for details.

### Source Code Availability

The complete source code for this project is available at the project repository. If you distribute binary-only copies, you must inform your licensees how to obtain the source.

### No Warranty

**This software is provided "as is" without warranty of any kind**, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, and non-infringement. In no event shall the authors or contributors be liable for any damages arising from the use of this software.
