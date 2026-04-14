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
    /// <summary>
    /// A static helper class containing the low-level byte manipulation routines necessary 
    /// for speaking the DRDA protocol directly to an IBM i Database Host Server.
    /// This abstracts away the heavy lifting of framing packets and parsing raw hex replies.
    /// </summary>
    public static class QueryExecutor
    {
        /// <summary>
        /// Formats a "Prepare and Describe" (0x1803) packet.
        /// This asks the database to compile a SQL statement and return its column definitions without executing it.
        /// </summary>
        public static void WritePrepareRequest(Memory<byte> buffer, int rpbId, string sql, string statementName, out int length)
        {
            // SQL statements over DRDA must be encoded in UTF-16 Big Endian.
            var sqlBytes = System.Text.Encoding.BigEndianUnicode.GetBytes(sql);
            int sqlLL = 12 + sqlBytes.Length; // 12 bytes overhead for the LLCP header
            
            // Resource names like Statement IDs and Cursor IDs are strictly EBCDIC, 10 bytes, space-padded
            var stmtNameBytes = CcsidConverter.GetBytes(37, statementName.PadRight(10, ' ').Substring(0, 10));
            int stmtNameLL = 10 + stmtNameBytes.Length;

            int parms = 4; // We are sending 4 specific parameters in this request
            length = 40 + sqlLL + stmtNameLL + 7 + 7; // Total packet length equation
            
            // --- 20-Byte Standard Request Header ---
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(0, 4), (uint)length);
            buffer.Span[4] = 0; buffer.Span[5] = 0;
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(6, 2), 0xE004); // Target: Database Server
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(8, 4), 0); // Instance unused
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(12, 4), 1); // Message Correlator
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(16, 2), 20); // Template Length
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), 0x1803); // Request Action: Prepare & Describe
            
            // --- 20-Byte Prepare Template ---
            // ORS Bitmap - 0x88020000 = Return Immediate, Request Data Format, Request Extended Columns
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(20, 4), 0x88020000); 
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(24, 4), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(28, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(30, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(32, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(34, 2), (ushort)rpbId); // Link to previously established Request Parameter Block
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(36, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(38, 2), (ushort)parms); // Parm count
            
            int offset = 40;
            
            // P1: 0x3806 Prepare Statement Name mapping
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)stmtNameLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3806);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 37); // String CCSID: EBCDIC 37
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+8, 2), (ushort)stmtNameBytes.Length);
            stmtNameBytes.CopyTo(buffer.Span.Slice(offset+10, stmtNameBytes.Length));
            offset += stmtNameLL;

            // P2: 0x3831 Extended SQL Statement Text
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), (uint)sqlLL);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3831);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+6, 2), 13488); // String CCSID: UTF-16 BE (13488)
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset+8, 4), (uint)sqlBytes.Length);
            sqlBytes.CopyTo(buffer.Span.Slice(offset+12, sqlBytes.Length));
            offset += sqlLL;

            // P3: 0x380A Describe Option
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 7);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x380A);
            buffer.Span[offset+6] = 0xD5; offset += 7; // 0xD5 requests detailed type info

            // P4: 0x3829 Extended Column Descriptor
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 7);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3829);
            buffer.Span[offset+6] = 0xF1; offset += 7;
        }

        /// <summary>
        /// Formats a "Create Request Parameter Block" (0x1D00) packet.
        /// This initializes tracking structures on the DB2 server for a new cursor/statement combination.
        /// Required before performing practically any SQL work.
        /// </summary>
        public static void WriteCreateRpb(Memory<byte> buffer, int rpbId, string statementName, string cursorName, out int length)
        {
            var stmtNameBytes = CcsidConverter.GetBytes(37, statementName.PadRight(10, ' ').Substring(0, 10));
            int stmtNameLL = 10 + stmtNameBytes.Length;
            var cursorNameBytes = CcsidConverter.GetBytes(37, cursorName.PadRight(10, ' ').Substring(0, 10));
            int cursorNameLL = 10 + cursorNameBytes.Length;
            
            // Hardcoded fallback package details based on JTOpen defaults
            var pkgNameBytes = CcsidConverter.GetBytes(37, "QZDAPKG   ");
            int pkgNameLL = 10 + pkgNameBytes.Length;
            var pkgLibBytes = CcsidConverter.GetBytes(37, "QGPL      ");
            int pkgLibLL = 10 + pkgLibBytes.Length;

            int parms = 4;
            length = 40 + stmtNameLL + cursorNameLL + pkgNameLL + pkgLibLL;
            
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(0, 4), (uint)length);
            buffer.Span[4] = 0; buffer.Span[5] = 0;
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(6, 2), 0xE004);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(8, 4), 0);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(12, 4), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(16, 2), 20);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), 0x1D00); // 0x1D00 Action: Create RPB
            
            // Request server acknowledgment so we know the RPB is ready before proceeding.
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(20, 4), 0x80000000);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(24, 4), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(28, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(30, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(32, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(34, 2), (ushort)rpbId); // Assigning ID
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(36, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(38, 2), (ushort)parms);
            
            int offset = 40;
            
            // Parameter Mapping: Statement, Cursor, Object Package, Object Library
            
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

        /// <summary>
        /// Formats an "Execute Or Open & Describe" (0x1812) packet.
        /// This is an optimized network combination command when you need to run queries immediately.
        /// Currently partially implemented; primarily relies on WritePrepareAndExecute instead for simpler paths.
        /// </summary>
        public static void WriteExecuteOrOpenAndDescribe(Memory<byte> buffer, int rpbId, string sql, string cursorName, out int length)
        {
            // Similar mapping to Prepare, with combined options logic.
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
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), 0x1812); // Action: Execute or Open & Describe
            
            // ORS Bitmap - 0x8C000000 = Request Return Immediate, Data Format shape, Result Data mapping
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

            // 0x3809 Open Attributes (0x80 = READ_ONLY)
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 7);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3809);
            buffer.Span[offset+6] = 0x80; offset += 7;

            // 0x380A Describe Option
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 7);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x380A);
            buffer.Span[offset+6] = 0xD5; offset += 7;

            // 0x380C Blocking Factor
            // Ask the server to send up to 32 rows per network response
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 10);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x380C);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset+6, 4), 32); offset += 10;
        }

        /// <summary>
        /// Formats a "Prepare and Execute" (0x180D) packet.
        /// Ideal for Non-Query modifications (INSERT/UPDATE/DELETE).
        /// </summary>
        public static void WritePrepareAndExecute(Memory<byte> buffer, int rpbId, string sql, string statementName, out int length)
        {
            var sqlBytes = System.Text.Encoding.BigEndianUnicode.GetBytes(sql);
            int sqlLL = 12 + sqlBytes.Length;

            var stmtNameBytes = CcsidConverter.GetBytes(37, statementName.PadRight(10, ' ').Substring(0, 10));
            int stmtNameLL = 10 + stmtNameBytes.Length;

            int parms = 2; // Only sending the SQL texts
            length = 40 + sqlLL + stmtNameLL;

            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(0, 4), (uint)length);
            buffer.Span[4] = 0; buffer.Span[5] = 0;
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(6, 2), 0xE004);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(8, 4), 0);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(12, 4), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(16, 2), 20);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), 0x180D); // Action: Prepare and Execute

            // ORS Bitmap - SEND_REPLY_IMMED (0x80) | SQLCA (0x02) = 0x82000000
            // SQLCA prompts DB2 to return success/fail structures natively within the response.
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

        /// <summary>
        /// A network response parser specifically hunting for an SQL Communications Area (SQLCA) block within a reply.
        /// Extracts the integer representing rows updated/inserted/deleted by a Non-Query command.
        /// </summary>
        public static int ParseUpdateCount(byte[] reply)
        {
            if (reply.Length < 40) return -1;

            int length = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(0, 4));
            int offset = 40; // Skip envelope header

            // Walk the linked list of Length-Length-Code-Point (LLCP) network attributes
            while (offset + 6 <= reply.Length && offset < length)
            {
                int ll = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(offset, 4)); // LL (Length)
                if (ll < 6 || offset + ll > reply.Length) break;

                int cp = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(offset+4, 2)); // CP (Code Point flag)

                // 0x3807 corresponds to a returned SQLCA block.
                if (cp == 0x3807 && ll >= 6 + 108)
                {
                    int dataStart = offset + 6;
                    // According to IBM i specifications, the "Row Count" in an SQLCA starts 104 bytes into the block structure
                    return BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 104, 4));
                }

                offset += ll; // Advance to next attribute
            }

            return -1;
        }

        /// <summary>
        /// Formats an "Open, Describe, and Fetch" (0x180E) packet.
        /// Instructs the server to open an existing cursor previously linked to a Prepared Statement,
        /// then retrieve the First N Rows of its execution result set.
        /// </summary>
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
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(18, 2), 0x180E); // Action: Open Describe Fetch
            
            // ORS Bitmap - 0x86000000 = Return Immediate, Request Result Data blocks, Request Server SQLCA 
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(20, 4), 0x86000000); 
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(24, 4), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(28, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(30, 2), 1);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(32, 2), 0);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(34, 2), (ushort)rpbId); // Matching previous RPB!
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

            // 0x3809 Open Attributes (0x80 = READ_ONLY hint to DB2 Engine)
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 7);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x3809);
            buffer.Span[offset+6] = 0x80; offset += 7;

            // 0x380C Blocking Factor: Request up to 256 rows directly in the reply to save latency
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset, 4), 10);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Span.Slice(offset+4, 2), 0x380C);
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Span.Slice(offset+6, 4), 256); offset += 10;
        }

        /// <summary>
        /// Reads a raw server network reply block, searching dynamically for Column Definitions ("Format") 
        /// and physical row bytes ("Results").
        /// This intelligently dissects IBM's densely packed descriptor lists into C# ColumnDef definitions.
        /// </summary>
        public static void ParseFormatAndResults(byte[] reply, QueryResult result)
        {
            if (reply.Length < 40) return;
            
            int length = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(0, 4));
            int offset = 40; // Step over header
            
            // Loop through LLCP chains
            while (offset + 6 <= reply.Length && offset < length)
            {
                int ll = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(offset, 4)); // Block length
                if (ll < 6 || offset + ll > reply.Length) break;
                
                int cp = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(offset+4, 2)); // Block ID
                int dataStart = offset + 6;

                // --- Parsing Format Options / Column Definitions ---
                // Depending on the Database version/settings, we may receive different versions of Data Map Formats.

                if (cp == 0x3805 && ll > 8)
                {
                    // Original Data Format (JTOpen: DBOriginalDataFormat)
                    // Used mostly by legacy statements or older V5/V6 machines
                    // Fixed header: 8 bytes, then 54-byte field descriptors
                    int numFields = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 4, 2));
                    int recordSize = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 6, 2));
                    result.RowSize = recordSize;

                    if (result.Columns.Count == 0) // Only initialize if not previously tracked
                    {
                        for (int i = 0; i < numFields; i++)
                        {
                            // A descriptor is 54 bytes mapping Type, scale, length, precision and column name
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
                    // Standard descriptor reply for V7+ DRDA
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
                    // Allows handling of very large strings, CLOBs, DBCLOBs, mapping over older 2byte constraints
                    int numFields = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 4, 4));
                    int recordSize = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 12, 4));
                    result.RowSize = recordSize;

                    if (result.Columns.Count == 0)
                    {
                        int fieldBase = dataStart + 16;
                        for (int i = 0; i < numFields; i++)
                        {
                            int foff = fieldBase + i * 48; // Note: Super descriptors are 48 bytes long
                            if (foff + 14 > reply.Length) break;
                            var col = new ColumnDef();
                            col.Type = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff + 2, 2));
                            col.Length = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(foff + 4, 4)); // 4 byte lengths!
                            col.Scale = BinaryPrimitives.ReadInt16BigEndian(reply.AsSpan(foff + 8, 2));
                            col.Precision = BinaryPrimitives.ReadInt16BigEndian(reply.AsSpan(foff + 10, 2));
                            col.Ccsid = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(foff + 12, 2));
                            col.Name = "Col" + i;
                            result.Columns.Add(col);
                        }
                    }
                }
                
                // --- Parsing Row Datas ---
                
                else if (cp == 0x3806 && ll > 14)
                {
                    // Original Result Data (JTOpen: DBOriginalData)
                    // The actual returned grid data blocks (bytes of rows).
                    // Header map: consistency(4) + rowCount(4) + columnCount(2) + indicatorSize(2) + rowSize(2) = 14 bytes overhead
                    int rowCount = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 4, 4));
                    int columnCount = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 8, 2));
                    int indicatorSize = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 10, 2)); // NULL map bytes
                    int rowSize = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 12, 2));
                    if (rowSize > 0) result.RowSize = rowSize;
                    
                    int indicatorTotal = rowCount * columnCount * indicatorSize;
                    int dataOffset = dataStart + 14 + indicatorTotal;

                    // Null indicators are provided in a sequential array BEFORE the actual tabular row data
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
                                    // A negative value in an indicator frame signifies database 'NULL'
                                    rowIndicators[j] = indicatorSize == 2
                                        ? BinaryPrimitives.ReadInt16BigEndian(reply.AsSpan(indicatorOffset, 2))
                                        : BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(indicatorOffset, 4));
                                }
                                indicatorOffset += indicatorSize;
                            }
                            result.NullIndicators.Add(rowIndicators);
                        }
                    }

                    // Slice the raw continuous buffer up into segmented rows
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (dataOffset + rowSize > reply.Length) break;
                        byte[] rowData = new byte[rowSize];
                        Array.Copy(reply, dataOffset, rowData, 0, rowSize);
                        result.Rows.Add(rowData); // Stash byte arrays into result tracker structure
                        dataOffset += rowSize;
                    }
                }
                else if (cp == 0x380E && ll > 20)
                {
                    // Extended Result Data (JTOpen: DBExtendedData)
                    // Same as Original, but mapping supports payloads with larger offsets natively.
                    // Header: consistency(4) + rowCount(4) + columnCount(2) + indicatorSize(2) + reserved(4) + rowSize(4) = 20 bytes
                    int rowCount = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 4, 4));
                    int columnCount = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 8, 2));
                    int indicatorSize = BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(dataStart + 10, 2));
                    int rowSize = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(dataStart + 16, 4));
                    if (rowSize > 0) result.RowSize = rowSize;
                    
                    int indicatorTotal = rowCount * columnCount * indicatorSize;
                    int dataOffset = dataStart + 20 + indicatorTotal;

                    // Load Null Indicators
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

                    // Pull physical row streams
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (dataOffset + rowSize > reply.Length) break;
                        byte[] rowData = new byte[rowSize];
                        Array.Copy(reply, dataOffset, rowData, 0, rowSize);
                        result.Rows.Add(rowData);
                        dataOffset += rowSize;
                    }
                }
                
                offset += ll; // Advance index block
            }
        }
    }
}
