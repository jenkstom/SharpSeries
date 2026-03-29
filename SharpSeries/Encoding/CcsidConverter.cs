using System.Collections.Concurrent;
using System.Text;

namespace SharpSeries.Encoding;

public static class CcsidConverter
{
    private static readonly ConcurrentDictionary<int, System.Text.Encoding> _encodings = new();

    static CcsidConverter()
    {
        System.Text.Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    /// <summary>
    /// Gets the Encoding for the specified IBM CCSID.
    /// By default, standard US EBCDIC is CCSID 37.
    /// </summary>
    public static System.Text.Encoding GetEncoding(int ccsid)
    {
        return _encodings.GetOrAdd(ccsid, id =>
        {
            try
            {
                return System.Text.Encoding.GetEncoding(id);
            }
            catch (NotSupportedException)
            {
                throw new NotSupportedException($"The CCSID {id} is not supported on this system.");
            }
        });
    }

    /// <summary>
    /// Converts a ROS EBCDIC string from the byte span to standard C# string using the specified CCSID.
    /// </summary>
    public static string GetString(int ccsid, ReadOnlySpan<byte> bytes)
    {
        var encoding = GetEncoding(ccsid);
        return encoding.GetString(bytes);
    }

    /// <summary>
    /// Converts a C# string into EBCDIC bytes for the specified CCSID.
    /// </summary>
    public static int GetBytes(int ccsid, ReadOnlySpan<char> chars, Span<byte> bytes)
    {
        var encoding = GetEncoding(ccsid);
        return encoding.GetBytes(chars, bytes);
    }

    /// <summary>
    /// Gets a byte representation of the string.
    /// </summary>
    public static byte[] GetBytes(int ccsid, string value)
    {
        var encoding = GetEncoding(ccsid);
        return encoding.GetBytes(value);
    }
}
