using SharpSeries.Encoding;

namespace SharpSeries.DataQueues;

/// <summary>
/// Identifies the job that sent a data queue entry. Only populated for queues created
/// with SENDERINF(*YES); the server returns a 36-byte EBCDIC block consisting of the
/// sending job name (10), user name (10), job number (6), and current user profile (10).
/// </summary>
public sealed class DataQueueSenderInfo
{
    /// <summary>The name of the job that sent the entry.</summary>
    public string JobName { get; }

    /// <summary>The user name under which the sending job runs.</summary>
    public string UserName { get; }

    /// <summary>The six-digit job number of the sending job.</summary>
    public string JobNumber { get; }

    /// <summary>The current user profile of the sending job.</summary>
    public string CurrentUserProfile { get; }

    internal DataQueueSenderInfo(string jobName, string userName, string jobNumber, string currentUserProfile)
    {
        JobName = jobName;
        UserName = userName;
        JobNumber = jobNumber;
        CurrentUserProfile = currentUserProfile;
    }

    /// <summary>
    /// Parses the 36-byte sender information block. Returns null when the block is
    /// absent or starts with an EBCDIC space, which is how the server signals that
    /// the queue does not save sender information.
    /// </summary>
    internal static DataQueueSenderInfo? Parse(byte[]? bytes)
    {
        const byte EbcdicSpace = 0x40;
        if (bytes == null || bytes.Length < 36 || bytes[0] == EbcdicSpace)
            return null;

        return new DataQueueSenderInfo(
            CcsidConverter.GetString(37, bytes.AsSpan(0, 10)).TrimEnd(),
            CcsidConverter.GetString(37, bytes.AsSpan(10, 10)).TrimEnd(),
            CcsidConverter.GetString(37, bytes.AsSpan(20, 6)).TrimEnd(),
            CcsidConverter.GetString(37, bytes.AsSpan(26, 10)).TrimEnd());
    }

    public override string ToString()
        => $"{JobNumber}/{UserName}/{JobName} (current profile: {CurrentUserProfile})";
}
