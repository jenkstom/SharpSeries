using SharpSeries.Encoding;

namespace SharpSeries.DataQueues;

/// <summary>
/// The exception thrown when the IBM i data queue server rejects a request.
/// Carries the server return code and, when the server supplied one, the CPF
/// message identifier (e.g. CPF9801).
/// Return code semantics are ported from JTOpen BaseDataQueueImplRemote.buildException().
/// </summary>
public class DataQueueException : Exception
{
    /// <summary>
    /// The raw server return code (e.g. 0xF001 for a command check, 0xF005 for an
    /// unsupported entry length).
    /// </summary>
    public int ReturnCode { get; }

    /// <summary>
    /// The CPF message identifier from the server's message text, when available.
    /// </summary>
    public string? MessageId { get; }

    internal DataQueueException(string message, int returnCode, string? messageId)
        : base(message)
    {
        ReturnCode = returnCode;
        MessageId = messageId;
    }

    /// <summary>
    /// Builds the appropriate exception for a data queue server return code,
    /// mirroring the JTOpen error mapping.
    /// </summary>
    internal static DataQueueException Create(int returnCode, byte[]? messageBytes, string operation)
    {
        string? messageText = messageBytes is { Length: > 0 }
            ? CcsidConverter.GetString(37, messageBytes)
            : null;
        string messageId = messageText != null && messageText.Length >= 7
            ? messageText.Substring(0, 7)
            : "";

        string description = returnCode switch
        {
            0xF001 => messageId switch
            {
                "CPF9810" => "The library does not exist.",
                "CPF9801" or "CPF2105" => "The data queue does not exist.",
                "CPF9802" or "CPF2189" => "User is not authorized to the data queue.",
                "CPF9820" or "CPF2182" => "User is not authorized to the library.",
                "CPF9502" => "Cannot use a keyed data queue API against a non-keyed data queue.",
                "CPF9506" => "Cannot use a non-keyed data queue API against a keyed data queue; use KeyedDataQueue.",
                _ => messageText ?? "The data queue request failed a server-side check."
            },
            0xF002 => "Data queue protocol error.",
            0xF003 => "Data queue request contained a syntax error.",
            0xF004 => "The data queue has been destroyed.",
            0xF005 => "Unsupported data queue entry length.",
            0xF007 => "Data queue data stream level not valid.",
            0xF008 => "Data queue version/release/modification not valid.",
            0xF009 => "Request rejected by user exit program.",
            0xF00A => "User exit program not authorized.",
            0xF00B => "User exit program not found.",
            0xF00D => "User exit program error.",
            0xF00E => "User exit program number not valid.",
            _ => messageText ?? "Error completing data queue request."
        };

        return new DataQueueException(
            $"{description} (operation: {operation}, server return code: 0x{returnCode:X4})",
            returnCode,
            messageId.Length == 0 ? null : messageId);
    }
}
