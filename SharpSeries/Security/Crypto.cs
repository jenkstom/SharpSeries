using System.Security.Cryptography;

namespace SharpSeries.Security;

public static class Crypto
{
    /// <summary>
    /// DRDA Security Mechanism 9 (EUSRIDPWD) typically requires encrypting the password 
    /// using a DES algorithm, derived from a random server seed (SECTKN).
    /// </summary>
    public static byte[] EncryptPasswordMechanism9(string password, byte[] serverToken)
    {
        // IBM DRDA defines a specific padding and transposition standard for passwords 
        // to conform to DES 8-byte blocks. This is a skeletal representation of the algorithm.
        
        // Pad password to 8 bytes with spaces matching EBCDIC ' '
        var paddedPwd = new byte[8];
        Array.Fill(paddedPwd, (byte)0x40); // 0x40 is EBCDIC space
        
        var pwdBytes = Encoding.CcsidConverter.GetBytes(37, password);
        Array.Copy(pwdBytes, paddedPwd, Math.Min(pwdBytes.Length, 8));

        // Use DES in ECB mode, no padding for the token permutation
        using var des = DES.Create();
        des.Mode = CipherMode.ECB;
        des.Padding = PaddingMode.None;
        des.Key = paddedPwd; 

        // The token is encrypted with the password as the key
        using var encryptor = des.CreateEncryptor();
        var clientToken = new byte[8];
        encryptor.TransformBlock(serverToken, 0, 8, clientToken, 0);
        
        return clientToken;
    }
}
