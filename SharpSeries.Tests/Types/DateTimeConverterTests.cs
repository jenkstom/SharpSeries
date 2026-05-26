using SharpSeries.Types;

namespace SharpSeries.Tests.Types;

public class DateTimeConverterTests
{
    [Fact]
    public void ParseIsoDateFromEbcdic()
    {
        // "2023-10-15" (EBCDIC)
        var ebcdicBytes = SharpSeries.Encoding.CcsidConverter.GetBytes(37, "2023-10-15");
        
        var date = DateTimeConverter.ReadDate(ebcdicBytes, 37);
        
        Assert.Equal(new DateTime(2023, 10, 15), date);
    }

    [Fact]
    public void ParseNativeTimestamp()
    {
        // "2023-10-15-12.30.45.123456" 
        var ebcdicBytes = SharpSeries.Encoding.CcsidConverter.GetBytes(37, "2023-10-15-12.30.45.123456");
        
        var dt = DateTimeConverter.ReadTimestamp(ebcdicBytes, 37);
        
        Assert.Equal(2023, dt.Year);
        Assert.Equal(10, dt.Month);
        Assert.Equal(15, dt.Day);
        Assert.Equal(12, dt.Hour);
        Assert.Equal(30, dt.Minute);
        Assert.Equal(45, dt.Second);
        // Note: Milliseconds are truncated by DateTime.TryParse depending on format strings, but testing base parse
    }
}
