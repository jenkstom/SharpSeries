using SharpSeries.Types;

namespace SharpSeries.Tests.Types;

public class DecimalConverterTests
{
    [Fact]
    public void ParsePackedDecimalPositive()
    {
        // 1234.5 in packed decimal (length 3 bytes), sign positive (F)
        // 0x12 0x34 0x5F -> 12345 (scale 1) -> 1234.5
        byte[] buffer = new byte[] { 0x12, 0x34, 0x5F };
        
        decimal result = DecimalConverter.ReadPackedDecimal(buffer, 1);
        
        Assert.Equal(1234.5m, result);
    }

    [Fact]
    public void ParsePackedDecimalNegative()
    {
        // -987 in packed decimal (length 2 bytes), sign negative (D)
        // 0x98 0x7D -> 987 (scale 0) -> -987
        byte[] buffer = new byte[] { 0x98, 0x7D };
        
        decimal result = DecimalConverter.ReadPackedDecimal(buffer, 0);
        
        Assert.Equal(-987m, result);
    }

    [Fact]
    public void ParseZonedDecimalPositive()
    {
        // "123" in zoned EBCDIC (sign F on last byte)
        // 0xF1 0xF2 0xF3
        byte[] buffer = new byte[] { 0xF1, 0xF2, 0xF3 };
        
        decimal result = DecimalConverter.ReadZonedDecimal(buffer, 0);
        
        Assert.Equal(123m, result);
    }

    [Fact]
    public void ParseZonedDecimalNegative()
    {
        // "-123" in zoned EBCDIC (sign D on last byte)
        // 0xF1 0xF2 0xD3 -> -123
        byte[] buffer = new byte[] { 0xF1, 0xF2, 0xD3 };
        
        decimal result = DecimalConverter.ReadZonedDecimal(buffer, 0);
        
        Assert.Equal(-123m, result);
    }
}
