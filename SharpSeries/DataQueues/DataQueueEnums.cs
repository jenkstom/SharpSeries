namespace SharpSeries.DataQueues;

/// <summary>
/// The type of an IBM i data queue, as reported by the server.
/// </summary>
public enum DataQueueType
{
    /// <summary>First-in, first-out queue.</summary>
    Fifo = 0,

    /// <summary>Last-in, first-out queue.</summary>
    Lifo = 1,

    /// <summary>Keyed queue; entries are addressed by key.</summary>
    Keyed = 2,
}

/// <summary>
/// The key comparison operator used when reading from a keyed data queue.
/// Keys are compared as unsigned byte strings.
/// </summary>
public enum KeySearchType
{
    /// <summary>Match a key equal to the search key.</summary>
    Equal,

    /// <summary>Match a key not equal to the search key.</summary>
    NotEqual,

    /// <summary>Match a key less than the search key.</summary>
    LessThan,

    /// <summary>Match a key less than or equal to the search key.</summary>
    LessThanOrEqual,

    /// <summary>Match a key greater than the search key.</summary>
    GreaterThan,

    /// <summary>Match a key greater than or equal to the search key.</summary>
    GreaterThanOrEqual,
}
