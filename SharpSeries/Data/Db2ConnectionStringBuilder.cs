using System.Data.Common;

namespace SharpSeries.Data;

public class Db2ConnectionStringBuilder : DbConnectionStringBuilder
{
    public Db2ConnectionStringBuilder() { }
    
    public Db2ConnectionStringBuilder(string connectionString) 
    { 
        ConnectionString = connectionString; 
    }

    public string Server 
    { 
        get => TryGetValue("Server", out var v) ? v?.ToString() ?? "" : ""; 
        set => this["Server"] = value; 
    }

    public int Port 
    { 
        get => TryGetValue("Port", out var v) && int.TryParse(v?.ToString(), out var p) ? p : 446; 
        set => this["Port"] = value; 
    }

    public string Database 
    { 
        get => TryGetValue("Database", out var v) ? v?.ToString() ?? "" : ""; 
        set => this["Database"] = value; 
    }

    public string UserID 
    { 
        get => TryGetValue("User ID", out var v) ? v?.ToString() ?? "" : ""; 
        set => this["User ID"] = value; 
    }

    public string Password 
    { 
        get => TryGetValue("Password", out var v) ? v?.ToString() ?? "" : ""; 
        set => this["Password"] = value; 
    }

    public string Naming 
    { 
        get => TryGetValue("Naming", out var v) ? v?.ToString() ?? "SQL" : "SQL"; 
        set => this["Naming"] = value; 
    }

    public int Ccsid 
    { 
        get => TryGetValue("CCSID", out var v) && int.TryParse(v?.ToString(), out var c) ? c : 37; 
        set => this["CCSID"] = value; 
    }

    public bool ReadOnly
    {
        get => TryGetValue("Read Only", out var v) && bool.TryParse(v?.ToString(), out var b) && b;
        set => this["Read Only"] = value;
    }
}