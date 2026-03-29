using SharpSeries.Encoding;

namespace SharpSeries.Tests.Encoding;

public class CcsidConverterTests
{
    [Fact]
    public void CanConvertEbcdicToCSharpString()
    {
        // 0xC8 0xC5 0xD3 0xD3 0xD6 is "HELLO" in EBCDIC (CCSID 37)
        byte[] ebcdicBytes = new byte[] { 0xC8, 0xC5, 0xD3, 0xD3, 0xD6, 0x40, 0x40 }; // HELLO (with spaces)
        
        var result = CcsidConverter.GetString(37, ebcdicBytes);
        
        Assert.Equal("HELLO  ", result);
    }
    
    [Fact]
    public void CanConvertCSharpStringToEbcdic()
    {
        var result = CcsidConverter.GetBytes(37, "123");
        
        // '1' = 0xF1, '2' = 0xF2, '3' = 0xF3
        Assert.Equal(new byte[] { 0xF1, 0xF2, 0xF3 }, result);
    }
}
