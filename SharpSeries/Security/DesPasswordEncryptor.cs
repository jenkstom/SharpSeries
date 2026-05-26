// Portions of this file are derivative works of JTOpenLite EncryptPassword.java.
// Copyright (C) 2011-2012 International Business Machines Corporation and others.
// All rights reserved. Licensed under the IBM Public License Version 1.0.
//
// This file was originally derived from the JTOpen project (https://github.com/IBM/JTOpen)
// and has been substantially modified for use in this project.

namespace SharpSeries.Security;

using System;
using System.Security.Cryptography;

internal static class DesPasswordEncryptor
{
    public static byte[] EncryptPasswordDES(byte[] userID, byte[] password, byte[] clientSeed, byte[] serverSeed)
    {
        byte[] sequenceNumber = { 0, 0, 0, 0, 0, 0, 0, 1 };

        byte[] token = DerivePasswordToken(userID, password);
        return ComputePasswordSubstitute(userID, token, sequenceNumber, clientSeed, serverSeed);
    }

    private static void SetDesParity(Span<byte> key)
    {
        for (int i = 0; i < 8; i++)
        {
            byte b = (byte)(key[i] & 0xFE);
            int bits = 0;
            for (int j = 0; j < 7; j++)
                bits ^= (b >> j) & 1;
            key[i] = (byte)(b | (bits & 1));
        }
    }

    private static byte[] DesEncrypt(byte[] key, byte[] data)
    {
        Span<byte> parityKey = stackalloc byte[8];
        key.AsSpan(0, 8).CopyTo(parityKey);
        SetDesParity(parityKey);

        using var des = DES.Create();
        des.Mode = CipherMode.ECB;
        des.Padding = PaddingMode.None;
        des.Key = parityKey.ToArray();
        using var encryptor = des.CreateEncryptor();
        var output = new byte[8];
        encryptor.TransformBlock(data, 0, 8, output, 0);
        return output;
    }

    private static int EbcdicLength(ReadOnlySpan<byte> buf, int maxLen)
    {
        int i = 0;
        while (i < maxLen && buf[i] != 0x40 && buf[i] != 0x00)
            i++;
        return i;
    }

    private static byte[] DerivePasswordToken(byte[] userID, byte[] password)
    {
        Span<byte> foldedUser = stackalloc byte[10];
        userID.AsSpan(0, 10).CopyTo(foldedUser);

        int uidLen = EbcdicLength(foldedUser, 10);
        if (uidLen > 8)
        {
            FoldUserId(foldedUser);
        }

        Span<byte> workA = stackalloc byte[10];
        Span<byte> workB = stackalloc byte[10];
        workA.Fill(0x40);
        workB.Fill(0x40);

        int pwdLen = EbcdicLength(password, 10);

        if (pwdLen > 8)
        {
            password.AsSpan(0, 8).CopyTo(workA);
            password.AsSpan(8, pwdLen - 8).CopyTo(workB);

            ApplyBitMaskAndShift(workA);
            byte[] half1 = DesEncrypt(workA.Slice(0, 8).ToArray(), foldedUser.ToArray());

            ApplyBitMaskAndShift(workB);
            byte[] half2 = DesEncrypt(workB.Slice(0, 8).ToArray(), foldedUser.ToArray());

            var token = new byte[8];
            for (int i = 0; i < 8; i++)
                token[i] = (byte)(half1[i] ^ half2[i]);
            return token;
        }
        else
        {
            password.AsSpan(0, pwdLen).CopyTo(workA);
            ApplyBitMaskAndShift(workA);
            return DesEncrypt(workA.Slice(0, 8).ToArray(), foldedUser.ToArray());
        }
    }

    private static void FoldUserId(Span<byte> uid)
    {
        uid[0] ^= (byte)(uid[8] & 0xC0);
        uid[1] ^= (byte)((uid[8] & 0x30) << 2);
        uid[2] ^= (byte)((uid[8] & 0x0C) << 4);
        uid[3] ^= (byte)((uid[8] & 0x03) << 6);
        uid[4] ^= (byte)(uid[9] & 0xC0);
        uid[5] ^= (byte)((uid[9] & 0x30) << 2);
        uid[6] ^= (byte)((uid[9] & 0x0C) << 4);
        uid[7] ^= (byte)((uid[9] & 0x03) << 6);
    }

    private static void ApplyBitMaskAndShift(Span<byte> buf)
    {
        for (int i = 0; i < 8; i++)
            buf[i] ^= 0x55;

        for (int i = 0; i < 7; i++)
            buf[i] = (byte)((buf[i] << 1) | ((buf[i + 1] & 0x80) >> 7));
        buf[7] <<= 1;
    }

    private static void XorInto(Span<byte> dest, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        for (int i = 0; i < 8; i++)
            dest[i] = (byte)(a[i] ^ b[i]);
    }

    private static void AddBytes(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, Span<byte> result)
    {
        int carry = 0;
        for (int i = 7; i >= 0; i--)
        {
            int sum = (a[i] & 0xFF) + (b[i] & 0xFF) + carry;
            carry = sum >> 8;
            result[i] = (byte)sum;
        }
    }

    private static byte[] ComputePasswordSubstitute(byte[] userID, byte[] token, byte[] seq, byte[] clientSeed, byte[] serverSeed)
    {
        Span<byte> drSeq = stackalloc byte[8];
        Span<byte> block = stackalloc byte[8];
        Span<byte> enc = stackalloc byte[8];

        AddBytes(seq, serverSeed, drSeq);
        enc = DesEncrypt(token, drSeq.ToArray());

        XorInto(block, enc, clientSeed);
        enc = DesEncrypt(token, block.ToArray());

        XorInto(block, userID, drSeq);
        XorInto(block, block, enc);
        enc = DesEncrypt(token, block.ToArray());

        block.Fill(0x40);
        block[0] = userID[8];
        block[1] = userID[9];

        XorInto(block, drSeq, block);
        XorInto(block, block, enc);
        enc = DesEncrypt(token, block.ToArray());

        XorInto(block, seq, enc);
        return DesEncrypt(token, block.ToArray());
    }
}
