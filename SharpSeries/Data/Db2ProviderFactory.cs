using System.Data.Common;

namespace SharpSeries.Data;

public class Db2ProviderFactory : DbProviderFactory
{
    public static readonly Db2ProviderFactory Instance = new();

    private Db2ProviderFactory() { }

    public override DbConnection CreateConnection() => new Db2Connection();
    public override DbCommand CreateCommand() => new Db2Command();
    public override DbParameter CreateParameter() => new Db2Parameter();
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new Db2ConnectionStringBuilder();
}
