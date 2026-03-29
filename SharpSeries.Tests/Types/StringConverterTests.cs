using SharpSeries.Types;

namespace SharpSeries.Tests.Types;

public class StringConverterTests
{
    [Fact]
    public void ReadCharStripsTrailingSpaces()
    {
        // "ABC " in EBCDIC (0xC1 0xC2 0xC3 0x40)
        byte[] buffer = new byte[] { 0xC1, 0xC2, 0xC3, 0x40 };
        
        string result = StringConverter.ReadChar(buffer, 37);
        
        Assert.Equal("ABC", result);
    }
    
    [Fact]
    public void ReadVarCharUsesLengthPrefix()
    {
        // 'A' 'B' length=2 -> (0x00 0x02 0xC1 0xC2)
        byte[] buffer = new byte[] { 0x00, 0x02, 0xC1, 0xC2 };
        
        string result = StringConverter.ReadVarChar(buffer, 37);
        
        Assert.Equal("AB", result);
    }
}
