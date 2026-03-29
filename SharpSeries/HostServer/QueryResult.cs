using System.Collections.Generic;
namespace SharpSeries.HostServer
{
    public class QueryResult
    {
        public int RowSize;
        public List<ColumnDef> Columns = new();
        public List<byte[]> Rows = new();
    }

    public class ColumnDef
    {
        public string Name = "";
        public int Type;
        public int Length;
        public int Ccsid;
        public int Scale;
        public int Precision;
    }
}
