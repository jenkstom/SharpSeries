using System.Data.Common;

namespace SharpSeries.Data;

/// <summary>
/// Provides a simple way to create and manage the contents of connection strings 
/// used by the <see cref="Db2Connection"/> class.
/// Inherits from ADO.NET's base builder to support dictionary-style parsing and configuration of parameters.
/// </summary>
public class Db2ConnectionStringBuilder : DbConnectionStringBuilder
{
    /// <summary>
    /// Initializes a new, empty connection string builder.
    /// </summary>
    public Db2ConnectionStringBuilder() { }
    
    /// <summary>
    /// Initializes a new connection string builder parsed from an existing connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to parse.</param>
    public Db2ConnectionStringBuilder(string connectionString) 
    { 
        ConnectionString = connectionString; 
    }

    /// <summary>
    /// Gets or sets the IP address or host name of the IBM i system.
    /// </summary>
    public string Server 
    { 
        // Retrieves the value from the dictionary, falling back to an empty string.
        get => TryGetValue("Server", out var v) ? v?.ToString() ?? "" : ""; 
        set => this["Server"] = value; 
    }

    /// <summary>
    /// Gets or sets the database port.
    /// Note: The Host Server mapper dynamically assigns ports, so this property acts 
    /// as a fallback override for DRDA port 446, though port 8471 is typical for QZDASOINIT.
    /// </summary>
    public int Port 
    { 
        get => TryGetValue("Port", out var v) && int.TryParse(v?.ToString(), out var p) ? p : 446; 
        set => this["Port"] = value; 
    }

    /// <summary>
    /// Gets or sets the name of the database or library to switch to.
    /// Note: Not strictly mapped to DRDA library list structures at runtime yet.
    /// </summary>
    public string Database 
    { 
        get => TryGetValue("Database", out var v) ? v?.ToString() ?? "" : ""; 
        set => this["Database"] = value; 
    }

    /// <summary>
    /// Gets or sets the User ID utilized for the Host Server authentication handshake.
    /// Must be an active user profile on the IBM i. Length must not exceed 10 characters.
    /// </summary>
    public string UserID 
    { 
        get => TryGetValue("User ID", out var v) ? v?.ToString() ?? "" : ""; 
        set => this["User ID"] = value; 
    }

    /// <summary>
    /// Gets or sets the password or pass-phrase for the User ID.
    /// </summary>
    public string Password 
    { 
        get => TryGetValue("Password", out var v) ? v?.ToString() ?? "" : ""; 
        set => this["Password"] = value; 
    }

    /// <summary>
    /// Represents the native naming convention utilized by Db2.
    /// Supported values are "SQL" (Schema.Table) or "System" (Library/File).
    /// </summary>
    public string Naming 
    { 
        get => TryGetValue("Naming", out var v) ? v?.ToString() ?? "SQL" : "SQL"; 
        set => this["Naming"] = value; 
    }

    /// <summary>
    /// The Default Coded Character Set Identifier (CCSID) to fall back to when interpreting network EBCDIC bytes.
    /// IBM i typically defaults to 37 for USA/Canada.
    /// </summary>
    public int Ccsid 
    { 
        get => TryGetValue("CCSID", out var v) && int.TryParse(v?.ToString(), out var c) ? c : 37; 
        set => this["CCSID"] = value; 
    }

    /// <summary>
    /// Gets or sets a boolean dictating whether the connection inherently rejects modification commands (INSERT/UPDATE/DELETE).
    /// Prevents Db2Command.ExecuteNonQuery from firing when set.
    /// </summary>
    public bool ReadOnly
    {
        get => TryGetValue("Read Only", out var v) && bool.TryParse(v?.ToString(), out var b) && b;
        set => this["Read Only"] = value;
    }
}
