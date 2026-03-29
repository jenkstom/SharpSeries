using System.Buffers;
using SharpSeries.Encoding;

namespace SharpSeries.Types;

public static class DateTimeConverter
{
    /// <summary>
    /// Parses an IBM i Date type mapped from DRDA string formats like 'YYYY-MM-DD'.
    /// Usually Dates come over the wire dynamically as strings formatted from CCSID 
    /// space encoded payload, unlike traditional numeric.
    /// </summary>
    public static DateTime ReadDate(ReadOnlySpan<byte> source, int ccsid)
    {
        if (source.IsEmpty) return DateTime.MinValue;

        // Assume standard DRDA mapped character EBCDIC formatted date ('YYYY-MM-DD')
        var dateString = CcsidConverter.GetString(ccsid, source);

        if (DateTime.TryParse(dateString, out var date))
            return date;

        return DateTime.MinValue; // Failed to parse
    }

    /// <summary>
    /// Parses an IBM i Timestamp field (typically 'YYYY-MM-DD-HH.MM.SS.NNNNNN') native DB2 formats
    /// to a C# DateTime struct.
    /// </summary>
    public static DateTime ReadTimestamp(ReadOnlySpan<byte> source, int ccsid)
    {
        if (source.IsEmpty) return DateTime.MinValue;

        var timestampString = CcsidConverter.GetString(ccsid, source);
        string[] formats = { 
            "yyyy-MM-dd-HH.mm.ss.ffffff", 
            "yyyy-MM-dd-HH.mm.ss.fff",
            "yyyy-MM-dd HH:mm:ss.ffffff"
        };
        
        if (DateTime.TryParseExact(timestampString, formats, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var dt))
        {
            return dt;
        }

        // Fallback
        return DateTime.MinValue;
    }
}
