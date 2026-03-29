using System;

namespace SharpSeries.Logging;

public enum Db2LogLevel
{
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    None
}

public static class Db2Logger
{
    public static Db2LogLevel Level { get; set; } = Db2LogLevel.None;
    public static Action<Db2LogLevel, string>? LogAction { get; set; }

    public static void Log(Db2LogLevel level, string message)
    {
        if (level >= Level && LogAction != null)
        {
            LogAction(level, message);
        }
    }

    public static void Trace(string message) => Log(Db2LogLevel.Trace, message);
    public static void Debug(string message) => Log(Db2LogLevel.Debug, message);
    public static void Info(string message) => Log(Db2LogLevel.Info, message);
    public static void Warn(string message) => Log(Db2LogLevel.Warn, message);
    public static void Error(string message) => Log(Db2LogLevel.Error, message);
}