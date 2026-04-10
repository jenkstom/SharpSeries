using System.Data.Common;

namespace SharpSeries.Data;

/// <summary>
/// A factory for creating instances of SharpSeries Db2 provider's data source classes.
/// This allows ADO.NET and ORMs (like Entity Framework Core or Dapper) to abstractly
/// spawn Db2Connections, DbCommands, and Parameters without needing to reference the types directly.
/// </summary>
public sealed class Db2ProviderFactory : DbProviderFactory
{
    /// <summary>
    /// The static singleton instance of the <see cref="Db2ProviderFactory"/>.
    /// Provided as a standard convenience field across all ADO.NET factories.
    /// </summary>
    public static readonly Db2ProviderFactory Instance = new();

    // Prevent external instantiation
    private Db2ProviderFactory()
    {
    }

    /// <summary>
    /// Returns a new instance of the provider's class that implements the <see cref="DbConnection"/> class.
    /// </summary>
    public override DbConnection CreateConnection() => new Db2Connection();

    /// <summary>
    /// Returns a new instance of the provider's class that implements the <see cref="DbCommand"/> class.
    /// </summary>
    public override DbCommand CreateCommand() => new Db2Command();

    /// <summary>
    /// Returns a new instance of the provider's class that implements the <see cref="DbParameter"/> class.
    /// </summary>
    public override DbParameter CreateParameter() => new Db2Parameter();

    /// <summary>
    /// Returns a new instance of the provider's class that implements the <see cref="DbConnectionStringBuilder"/> class.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new Db2ConnectionStringBuilder();
}
