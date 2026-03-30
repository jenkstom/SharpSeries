// This file is a derivative work of JTOpenLite (DatabaseConnection.java).
// Original source: https://github.com/IBM/JTOpen
// Copyright (C) 2011-2012 International Business Machines Corporation and others.
// Licensed under the IBM Public License v1.0.
// This file has been modified from the original.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SharpSeries.Encoding;

namespace SharpSeries.HostServer
{
    public static class QueryExecutor
    {
        public static void WritePrepareRequest(Memory<byte> buffer, int rpbId, string sql, string statementName, out int length)
        {
            var sqlBytes = System.Text.Encoding.BigEndianUnicode.GetBytes(sql);
            int sqlLL = 12 + sqlBytes.Length;
            
            var stmtNameBytes = CcsidConverter.GetBytes(37, statementName.PadRight(10, ' ').Substring(0, 10));
            int stmtNameLL = 10 + stmtNameBytes.Length;

            int parms = 4;
            length = 40 + sqlLL + stmtNameLL + 7 + 7;
            
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(0, 4), (uint)length);
            buffer.Span[4] = 0; buffer.Span[5] = 0;
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(6, 2), 0xE004);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(8, 4), 0); // CS Instance
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(12, 4), 1); // Corr
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(16, 2), 20); // Template
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), 0x1803); // Prepare & Describe
            
            // ORS Bitmap - 0x88020000 = IMMED, DATA_FORMAT, EXT_COL
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(20, 4), 0x88020000); 
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(24, 4), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(28, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(30, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(32, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(34, 2), (ushort)rpbId);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(36, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(38, 2), (ushort)parms);
            
            int offset = 40;
            
            // 0x3806 Prepare Statement Name
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)stmtNameLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3806);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 37);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+8, 2), (ushort)stmtNameBytes.Length);
            stmtNameBytes.CopyTo(buffer.Span.Slice(offset+10, stmtNameBytes.Length));
            offset += stmtNameLL;

            // 0x3831 Extended SQL Statement Text
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)sqlLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3831);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 13488);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset+8, 4), (uint)sqlBytes.Length);
            sqlBytes.CopyTo(buffer.Span.Slice(offset+12, sqlBytes.Length));
            offset += sqlLL;

            // 0x380A Describe Option
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 7);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x380A);
            buffer.Span[offset+6] = 0xD5; offset += 7;

            // 0x3829 Extended Column Descriptor
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 7);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3829);
            buffer.Span[offset+6] = 0xF1; offset += 7;
        }

        public static void WriteCreateRpb(Memory<byte> buffer, int rpbId, string statementName, string cursorName, out int length)
        {
            var stmtNameBytes = CcsidConverter.GetBytes(37, statementName.PadRight(10, ' ').Substring(0, 10));
            int stmtNameLL = 10 + stmtNameBytes.Length;
            var cursorNameBytes = CcsidConverter.GetBytes(37, cursorName.PadRight(10, ' ').Substring(0, 10));
            int cursorNameLL = 10 + cursorNameBytes.Length;
            var pkgNameBytes = CcsidConverter.GetBytes(37, "QZDAPKG   ");
            int pkgNameLL = 10 + pkgNameBytes.Length;
            var pkgLibBytes = CcsidConverter.GetBytes(37, "QGPL      ");
            int pkgLibLL = 10 + pkgLibBytes.Length;

            int parms = 4;
            length = 40 + stmtNameLL + cursorNameLL + pkgNameLL + pkgLibLL;
            
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(0, 4), (uint)length);
            buffer.Span[4] = 0; buffer.Span[5] = 0;
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(6, 2), 0xE004);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(8, 4), 0); // CS Instance
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(12, 4), 1); // Corr
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(16, 2), 20); // Template
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), 0x1D00); // Create RPB
            
            // ORS Bitmap - 0 (fire-and-forget, no reply expected, matching JTOpen syncRPB)
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(20, 4), 0x00000000); 
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(24, 4), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(28, 2), 0); // Return ORS handle
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(30, 2), 0); // Fill ORS handle
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(32, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(34, 2), (ushort)rpbId); // RPB ID
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(36, 2), 0); // descriptor
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(38, 2), (ushort)parms);
            
            int offset = 40;
            
            // 0x3806 Prepare Statement Name
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)stmtNameLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3806);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 37);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+8, 2), (ushort)stmtNameBytes.Length);
            stmtNameBytes.CopyTo(buffer.Span.Slice(offset+10, stmtNameBytes.Length));
            offset += stmtNameLL;

            // 0x380B Cursor Name
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)cursorNameLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x380B);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 37);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+8, 2), (ushort)cursorNameBytes.Length);
            cursorNameBytes.CopyTo(buffer.Span.Slice(offset+10, cursorNameBytes.Length));
            offset += cursorNameLL;

            // 0x3804 Package Name
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)pkgNameLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3804);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 37);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+8, 2), (ushort)pkgNameBytes.Length);
            pkgNameBytes.CopyTo(buffer.Span.Slice(offset+10, pkgNameBytes.Length));
            offset += pkgNameLL;

            // 0x3801 Package Library
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)pkgLibLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3801);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 37);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+8, 2), (ushort)pkgLibBytes.Length);
            pkgLibBytes.CopyTo(buffer.Span.Slice(offset+10, pkgLibBytes.Length));
            offset += pkgLibLL;
        }

        public static void WriteExecuteOrOpenAndDescribe(Memory<byte> buffer, int rpbId, string sql, string cursorName, out int length)
        {
            var sqlBytes = System.Text.Encoding.BigEndianUnicode.GetBytes(sql);
            int sqlLL = 12 + sqlBytes.Length;

            var cursorNameBytes = CcsidConverter.GetBytes(37, cursorName.PadRight(10, ' ').Substring(0, 10));
            int cursorNameLL = 10 + cursorNameBytes.Length;

            var pkgNameBytes = CcsidConverter.GetBytes(37, "QZDAPKG   ");
            int pkgNameLL = 10 + pkgNameBytes.Length;

            var pkgLibBytes = CcsidConverter.GetBytes(37, "QGPL      ");
            int pkgLibLL = 10 + pkgLibBytes.Length;

            int parms = 7;
            length = 40 + sqlLL + cursorNameLL + pkgNameLL + pkgLibLL + 7 + 7 + 10;
            
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(0, 4), (uint)length);
            buffer.Span[4] = 0; buffer.Span[5] = 0;
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(6, 2), 0xE004);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(8, 4), 0); // CS Instance
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(12, 4), 1); // Corr
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(16, 2), 20); // Template
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), 0x1812); // Execute or Open & Describe
            
            // ORS Bitmap - 0x8C000000 = IMMED, DATA_FORMAT, RESULT_DATA
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(20, 4), 0x8C000000); 
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(24, 4), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(28, 2), 1); // Return ORS handle
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(30, 2), 1); // Fill ORS handle
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(32, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(34, 2), (ushort)rpbId); // RPB ID
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(36, 2), 0); // descriptor
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(38, 2), (ushort)parms);
            
            int offset = 40;

            // 0x3831 Extended SQL Statement Text
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)sqlLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3831);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 13488);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset+8, 4), (uint)sqlBytes.Length);
            sqlBytes.CopyTo(buffer.Span.Slice(offset+12, sqlBytes.Length));
            offset += sqlLL;

            // 0x380B Cursor Name
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)cursorNameLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x380B); // 0x380B Cursor Name
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 37);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+8, 2), (ushort)cursorNameBytes.Length);
            cursorNameBytes.CopyTo(buffer.Span.Slice(offset+10, cursorNameBytes.Length));
            offset += cursorNameLL;

            // 0x3804 Package Name
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)pkgNameLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3804); // 0x3804 Package Name
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 37);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+8, 2), (ushort)pkgNameBytes.Length);
            pkgNameBytes.CopyTo(buffer.Span.Slice(offset+10, pkgNameBytes.Length));
            offset += pkgNameLL;

            // 0x3801 Package Library
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)pkgLibLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3801); // 0x3801 Package Library
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 37);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+8, 2), (ushort)pkgLibBytes.Length);
            pkgLibBytes.CopyTo(buffer.Span.Slice(offset+10, pkgLibBytes.Length));
            offset += pkgLibLL;

            // 0x3809 Open Attributes (0x80 = READ_ONLY)
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 7);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3809);
            buffer.Span[offset+6] = 0x80; offset += 7;

            // 0x380A Describe Option
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 7);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x380A);
            buffer.Span[offset+6] = 0xD5; offset += 7;

            // 0x380C Blocking Factor (Fetch rows at once)
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 10);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x380C);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset+6, 4), 32); offset += 10;
        }

        public static void WritePrepareAndExecute(Memory<byte> buffer, int rpbId, string sql, string statementName, out int length)
        {
            var sqlBytes = System.Text.Encoding.BigEndianUnicode.GetBytes(sql);
            int sqlLL = 12 + sqlBytes.Length;

            var stmtNameBytes = CcsidConverter.GetBytes(37, statementName.PadRight(10, ' ').Substring(0, 10));
            int stmtNameLL = 10 + stmtNameBytes.Length;

            int parms = 2;
            length = 40 + sqlLL + stmtNameLL;

            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(0, 4), (uint)length);
            buffer.Span[4] = 0; buffer.Span[5] = 0;
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(6, 2), 0xE004);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(8, 4), 0);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(12, 4), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(16, 2), 20);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), 0x180D); // Prepare and Execute

            // ORS Bitmap - SEND_REPLY_IMMED (0x80) | SQLCA (0x02) = 0x82000000
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(20, 4), 0x82000000);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(24, 4), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(28, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(30, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(32, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(34, 2), (ushort)rpbId);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(36, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(38, 2), (ushort)parms);

            int offset = 40;

            // 0x3806 Prepare Statement Name
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)stmtNameLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3806);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 37);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+8, 2), (ushort)stmtNameBytes.Length);
            stmtNameBytes.CopyTo(buffer.Span.Slice(offset+10, stmtNameBytes.Length));
            offset += stmtNameLL;

            // 0x3831 Extended SQL Statement Text
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)sqlLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3831);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 13488);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset+8, 4), (uint)sqlBytes.Length);
            sqlBytes.CopyTo(buffer.Span.Slice(offset+12, sqlBytes.Length));
            offset += sqlLL;
        }

        public static int ParseUpdateCount(byte[] reply)
        {
            if (reply.Length < 40) return -1;

            int length = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(0, 4));
            int offset = 40;

            while (offset + 6 <= reply.Length && offset < length)
            {
                int ll = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(offset, 4));
                if (ll < 6 || offset + ll > reply.Length) break;

                int cp = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(offset+4, 2));

                if (cp == 0x3807 && ll >= 6 + 108)
                {
                    int dataStart = offset + 6;
                    return BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 104, 4));
                }

                offset += ll;
            }

            return -1;
        }

        public static void WriteOpenDescribeFetch(Memory<byte> buffer, int rpbId, string cursorName, out int length)
        {
            var cursorNameBytes = CcsidConverter.GetBytes(37, cursorName.PadRight(10, ' ').Substring(0, 10));
            int cursorNameLL = 10 + cursorNameBytes.Length;

            int parms = 3;
            length = 40 + cursorNameLL + 7 + 10;
            
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(0, 4), (uint)length);
            buffer.Span[4] = 0; buffer.Span[5] = 0;
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(6, 2), 0xE004);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(8, 4), 0);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(12, 4), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(16, 2), 20);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), 0x180E); // Open Describe Fetch
            
            // ORS Bitmap - SEND_REPLY_IMMED | RESULT_DATA | SQLCA
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(20, 4), 0x86000000); 
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(24, 4), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(28, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(30, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(32, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(34, 2), (ushort)rpbId);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(36, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(38, 2), (ushort)parms);
            
            int offset = 40;

            // 0x380B Cursor Name
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)cursorNameLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x380B);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 37);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+8, 2), (ushort)cursorNameBytes.Length);
            cursorNameBytes.CopyTo(buffer.Span.Slice(offset+10, cursorNameBytes.Length));
            offset += cursorNameLL;

            // 0x3809 Open Attributes (0x80 = READ_ONLY)
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 7);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3809);
            buffer.Span[offset+6] = 0x80; offset += 7;

            // 0x380C Blocking Factor
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 10);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x380C);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset+6, 4), 256); offset += 10;
        }

        public static void ParseFormatAndResults(byte[] reply, QueryResult result)
        {
            if (reply.Length < 40) return;
            
            int length = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(0, 4));
            int offset = 40;
            
            while (offset + 6 <= reply.Length && offset < length)
            {
                int ll = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(offset, 4));
                if (ll < 6 || offset + ll > reply.Length) break;
                
                int cp = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(offset+4, 2));
                int dataStart = offset + 6;

                if (cp == 0x3805 && ll > 8)
                {
                    // Original Data Format (JTOpen: DBOriginalDataFormat)
                    // Fixed header: 8 bytes, then 54-byte field descriptors
                    // numFields at overlay+4 (uint16), recordSize at overlay+6 (uint16)
                    int numFields = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 4, 2));
                    int recordSize = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 6, 2));
                    result.RowSize = recordSize;

                    if (result.Columns.Count == 0)
                    {
                        for (int i = 0; i < numFields; i++)
                        {
                            // JTOpen: type at offset_+10, length at +12, scale +14, prec +16, ccsid +18
                            // nameLen at +28, nameCcsid +30, name at +32
                            // Each descriptor is 54 bytes starting at offset_+8
                            int foff = dataStart + 10 + i * 54;
                            if (foff + 24 > reply.Length) break;
                            var col = new ColumnDef();
                            col.Type = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff, 2));
                            col.Length = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff + 2, 2));
                            col.Scale = BinaryPrimitives.ReadInt16BigEndian(reply.AsSpan(foff + 4, 2));
                            col.Precision = BinaryPrimitives.ReadInt16BigEndian(reply.AsSpan(foff + 6, 2));
                            col.Ccsid = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff + 8, 2));
                            int nameLen = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff + 18, 2));
                            int nameCcsid = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff + 20, 2));
                            if (nameLen > 0 && foff + 22 + nameLen <= reply.Length)
                            {
                                try { col.Name = CcsidConverter.GetString(nameCcsid, reply.AsSpan(foff + 22, nameLen)).TrimEnd(); }
                                catch { col.Name = "Col" + i; }
                            }
                            else col.Name = "Col" + i;
                            result.Columns.Add(col);
                        }
                    }
                }
                else if (cp == 0x380C && ll > 8)
                {
                    // Extended Data Format (JTOpen: DBExtendedDataFormat)
                    // Similar to Original but with extended field descriptors
                    int numFields = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 4, 2));
                    int recordSize = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 6, 2));
                    result.RowSize = recordSize;

                    if (result.Columns.Count == 0)
                    {
                        int fieldBase = dataStart + 8;
                        for (int i = 0; i < numFields; i++)
                        {
                            int foff = fieldBase + i * 54;
                            if (foff + 10 > reply.Length) break;
                            var col = new ColumnDef();
                            col.Type = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff, 2));
                            col.Length = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff + 2, 2));
                            col.Scale = BinaryPrimitives.ReadInt16BigEndian(reply.AsSpan(foff + 4, 2));
                            col.Precision = BinaryPrimitives.ReadInt16BigEndian(reply.AsSpan(foff + 6, 2));
                            col.Ccsid = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff + 8, 2));
                            col.Name = "Col" + i;
                            result.Columns.Add(col);
                        }
                    }
                }
                else if (cp == 0x3812 && ll > 6)
                {
                    // Super Extended Data Format (JTOpen: DBSuperExtendedDataFormat)
                    // Fixed header: 16 bytes, then 48-byte field descriptors
                    // numFields at overlay+4 (uint32), recordSize at overlay+12 (uint32)
                    int numFields = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 4, 4));
                    int recordSize = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 12, 4));
                    result.RowSize = recordSize;

                    if (result.Columns.Count == 0)
                    {
                        int fieldBase = dataStart + 16;
                        for (int i = 0; i < numFields; i++)
                        {
                            int foff = fieldBase + i * 48;
                            if (foff + 14 > reply.Length) break;
                            var col = new ColumnDef();
                            col.Type = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff + 2, 2));
                            col.Length = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(foff + 4, 4));
                            col.Scale = BinaryPrimitives.ReadInt16BigEndian(reply.AsSpan(foff + 8, 2));
                            col.Precision = BinaryPrimitives.ReadInt16BigEndian(reply.AsSpan(foff + 10, 2));
                            col.Ccsid = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff + 12, 2));
                            col.Name = "Col" + i;
                            result.Columns.Add(col);
                        }
                    }
                }
                else if (cp == 0x3806 && ll > 14)
                {
                    // Original Result Data (JTOpen: DBOriginalData)
                    // Header: consistency(4) + rowCount(4) + columnCount(2) + indicatorSize(2) + rowSize(2) = 14 bytes
                    int rowCount = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 4, 4));
                    int columnCount = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 8, 2));
                    int indicatorSize = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 10, 2));
                    int rowSize = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 12, 2));
                    if (rowSize > 0) result.RowSize = rowSize;
                    
                    int indicatorTotal = rowCount * columnCount * indicatorSize;
                    int dataOffset = dataStart + 14 + indicatorTotal;

                    if (indicatorSize > 0)
                    {
                        int indicatorOffset = dataStart + 14;
                        for (int i = 0; i < rowCount; i++)
                        {
                            int[] rowIndicators = new int[columnCount];
                            for (int j = 0; j < columnCount; j++)
                            {
                                if (indicatorOffset + indicatorSize <= reply.Length)
                                {
                                    rowIndicators[j] = indicatorSize == 2
                                        ? BinaryPrimitives.ReadInt16BigEndian(reply.AsSpan(indicatorOffset, 2))
                                        : BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(indicatorOffset, 4));
                                }
                                indicatorOffset += indicatorSize;
                            }
                            result.NullIndicators.Add(rowIndicators);
                        }
                    }

                    for (int i = 0; i < rowCount; i++)
                    {
                        if (dataOffset + rowSize > reply.Length) break;
                        byte[] rowData = new byte[rowSize];
                        Array.Copy(reply, dataOffset, rowData, 0, rowSize);
                        result.Rows.Add(rowData);
                        dataOffset += rowSize;
                    }
                }
                else if (cp == 0x380E && ll > 20)
                {
                    // Extended Result Data (JTOpen: DBExtendedData)
                    // Header: consistency(4) + rowCount(4) + columnCount(2) + indicatorSize(2) + reserved(4) + rowSize(4) = 20 bytes
                    int rowCount = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 4, 4));
                    int columnCount = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 8, 2));
                    int indicatorSize = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 10, 2));
                    int rowSize = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 16, 4));
                    if (rowSize > 0) result.RowSize = rowSize;
                    
                    int indicatorTotal = rowCount * columnCount * indicatorSize;
                    int dataOffset = dataStart + 20 + indicatorTotal;

                    if (indicatorSize > 0)
                    {
                        int indicatorOffset = dataStart + 20;
                        for (int i = 0; i < rowCount; i++)
                        {
                            int[] rowIndicators = new int[columnCount];
                            for (int j = 0; j < columnCount; j++)
                            {
                                if (indicatorOffset + indicatorSize <= reply.Length)
                                {
                                    rowIndicators[j] = indicatorSize == 2
                                        ? BinaryPrimitives.ReadInt16BigEndian(reply.AsSpan(indicatorOffset, 2))
                                        : BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(indicatorOffset, 4));
                                }
                                indicatorOffset += indicatorSize;
                            }
                            result.NullIndicators.Add(rowIndicators);
                        }
                    }

                    for (int i = 0; i < rowCount; i++)
                    {
                        if (dataOffset + rowSize > reply.Length) break;
                        byte[] rowData = new byte[rowSize];
                        Array.Copy(reply, dataOffset, rowData, 0, rowSize);
                        result.Rows.Add(rowData);
                        dataOffset += rowSize;
                    }
                }
                
                offset += ll;
            }
        }
    }
}
