using System.Buffers;

namespace SharpSeries.Types;

public static class DecimalConverter
{
    /// <summary>
    /// Parses an IBM i Packed Decimal (COMP-3) from a ReadOnlySpan of bytes into a C# decimal.
    /// In packed decimal, each byte contains two decimal digits (nibbles), except the last 
    /// byte which contains one digit and the sign nibble.
    /// Positive signs: 0xA, 0xC, 0xE, 0xF (usually 0xF)
    /// Negative signs: 0xB, 0xD (usually 0xD)
    /// </summary>
    public static decimal ReadPackedDecimal(ReadOnlySpan<byte> source, int scale)
    {
        if (source.IsEmpty) return 0m;

        long value = 0;
        bool isNegative = false;

        for (int i = 0; i < source.Length; i++)
        {
            byte b = source[i];

            if (i == source.Length - 1)
            {
                // Last byte: Top nibble is a digit, bottom nibble is the sign
                int digit = b >> 4;
                value = (value * 10) + digit;

                int signNibble = b & 0x0F;
                isNegative = (signNibble == 0x0B || signNibble == 0x0D);
            }
            else
            {
                // Two digits per byte
                int topDigit = b >> 4;
                int bottomDigit = b & 0x0F;

                value = (value * 10) + topDigit;
                value = (value * 10) + bottomDigit;
            }
        }

        decimal result = new decimal(value);
        if (isNegative) result = -result;
        
        // Apply scale
        if (scale > 0)
        {
             // This is a simplistic scale applicator. In a production scenario 
             // we'd construct the decimal using its internal constructor to avoid loss
             // of precision on very large numbers.
             result /= (decimal)Math.Pow(10, scale);
        }

        return result;
    }

    /// <summary>
    /// Parses an IBM i Zoned Decimal.
    /// Each byte is a digit (in EBCDIC usually 0xF0-0xF9). The sign is stored in the top 
    /// nibble of the last byte (0xD for negative, 0xF for positive).
    /// </summary>
    public static decimal ReadZonedDecimal(ReadOnlySpan<byte> source, int scale)
    {
        if (source.IsEmpty) return 0m;

        long value = 0;
        bool isNegative = false;

        for (int i = 0; i < source.Length; i++)
        {
            byte b = source[i];
            
            if (i == source.Length - 1)
            {
                // Last byte contains the sign in the zone nibble
                int signZone = b >> 4;
                isNegative = (signZone == 0x0B || signZone == 0x0D);
                int digit = b & 0x0F;
                value = (value * 10) + digit;
            }
            else
            {
                // Strip the EBCDIC zone (usually 0xF) and take the digit
                int digit = b & 0x0F;
                value = (value * 10) + digit;
            }
        }

        decimal result = new decimal(value);
        if (isNegative) result = -result;

        if (scale > 0)
        {
            result /= (decimal)Math.Pow(10, scale);
        }

        return result;
    }
}
