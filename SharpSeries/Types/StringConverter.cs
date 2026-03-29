using System.Buffers;
using SharpSeries.Encoding;

namespace SharpSeries.Types;

public static class StringConverter
{
    /// <summary>
    /// Parses an IBM i CHAR (fixed space-padded length strings).
    /// </summary>
    public static string ReadChar(ReadOnlySpan<byte> source, int ccsid)
    {
        if (source.IsEmpty) return string.Empty;

        // Efficiently strip EBCDIC trailing spaces (0x40) before converting
        int length = source.Length;
        while (length > 0 && source[length - 1] == 0x40)
        {
            length--;
        }

        return CcsidConverter.GetString(ccsid, source.Slice(0, length));
    }

    /// <summary>
    /// Parses an IBM i VARCHAR format (length prefix + EBCDIC payload).
    /// The length prefix is standard DRDA length (typically 2-bytes big-endian).
    /// </summary>
    public static string ReadVarChar(ReadOnlySpan<byte> source, int ccsid)
    {
        if (source.Length < 2) return string.Empty;

        // First 2 bytes define string length
        ushort count = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(source.Slice(0, 2));
        
        // Return string starting past length prefix
        return CcsidConverter.GetString(ccsid, source.Slice(2, count));
    }

    /// <summary>
    /// Decodes an IBM i GRAPHIC type, which is DBCS (Double-Byte Character Set) natively.
    /// It translates directly to a C# Unicode string format string depending on CCSID.
    /// </summary>
    public static string ReadGraphicChar(ReadOnlySpan<byte> source, int ccsid)
    {
        if (source.IsEmpty) return string.Empty;

        // IBM i usually denotes GRAPHIC CCSIDs heavily. Typical DBCS mapping logic 
        // will automatically translate double-bytes if mapped to System.Text.Encoding properly
        return CcsidConverter.GetString(ccsid, source);
    }
}
