/* Oracle Redo Generic OpCode
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

OpenLogReplicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

OpenLogReplicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with OpenLogReplicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include "OpCode.h"
#include "OracleAnalyzer.h"
#include "Reader.h"
#include "RedoLogException.h"
#include "RedoLogRecord.h"

namespace OpenLogReplicator {
    void OpCode::process(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord) {
        if (oracleAnalyzer->dumpRedoLog >= 1) {
            bool encrypted = false;
            if ((redoLogRecord->typ & 0x80) != 0)
                encrypted = true;

            if (oracleAnalyzer->version < REDO_VERSION_12_1) {
                if (redoLogRecord->typ == 6)
                    oracleAnalyzer->dumpStream << "CHANGE #" << std::dec << (uint64_t)redoLogRecord->vectorNo <<
                        " MEDIA RECOVERY MARKER" <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << (uint64_t)redoLogRecord->seq <<
                        " OP:" << (uint64_t)(redoLogRecord->opCode >> 8) << "." << (uint64_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << (uint64_t)encrypted << std::endl;
                else
                    oracleAnalyzer->dumpStream << "CHANGE #" << std::dec << (uint64_t)redoLogRecord->vectorNo <<
                        " TYP:" << (uint64_t)redoLogRecord->typ <<
                        " CLS:" << redoLogRecord->cls <<
                        " AFN:" << redoLogRecord->afn <<
                        " DBA:0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba <<
                        " OBJ:" << std::dec << redoLogRecord->recordDataObj <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << (uint64_t)redoLogRecord->seq <<
                        " OP:" << (uint64_t)(redoLogRecord->opCode >> 8) << "." << (uint64_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << (uint64_t)encrypted <<
                        " RBL:" << std::dec << redoLogRecord->rbl << std::endl;
            } else if (oracleAnalyzer->version < REDO_VERSION_12_2) {
                if (redoLogRecord->typ == 6)
                    oracleAnalyzer->dumpStream << "CHANGE #" << std::dec << (uint64_t)redoLogRecord->vectorNo <<
                        " MEDIA RECOVERY MARKER" <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << (uint64_t)redoLogRecord->seq <<
                        " OP:" << (uint64_t)(redoLogRecord->opCode >> 8) << "." << (uint64_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << (uint64_t)encrypted <<
                        " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << std::endl;
                else
                    oracleAnalyzer->dumpStream << "CHANGE #" << std::dec << (uint64_t)redoLogRecord->vectorNo <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " TYP:" << (uint64_t)redoLogRecord->typ <<
                        " CLS:" << redoLogRecord->cls <<
                        " AFN:" << redoLogRecord->afn <<
                        " DBA:0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba <<
                        " OBJ:" << std::dec << redoLogRecord->recordDataObj <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << (uint64_t)redoLogRecord->seq <<
                        " OP:" << (uint64_t)(redoLogRecord->opCode >> 8) << "." << (uint64_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << (uint64_t)encrypted <<
                        " RBL:" << std::dec << redoLogRecord->rbl <<
                        " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << std::endl;
            } else {
                if (redoLogRecord->typ == 6)
                    oracleAnalyzer->dumpStream << "CHANGE #" << std::dec << (uint64_t)redoLogRecord->vectorNo <<
                        " MEDIA RECOVERY MARKER" <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " SCN:" << PRINTSCN64(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << (uint64_t)redoLogRecord->seq <<
                        " OP:" << (uint64_t)(redoLogRecord->opCode >> 8) << "." << (uint64_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << (uint64_t)encrypted <<
                        " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << std::endl;
                else
                    oracleAnalyzer->dumpStream << "CHANGE #" << std::dec << (uint64_t)redoLogRecord->vectorNo <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " TYP:" << (uint64_t)redoLogRecord->typ <<
                        " CLS:" << redoLogRecord->cls <<
                        " AFN:" << redoLogRecord->afn <<
                        " DBA:0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->dba <<
                        " OBJ:" << std::dec << redoLogRecord->recordDataObj <<
                        " SCN:" << PRINTSCN64(redoLogRecord->scnRecord) <<
                        " SEQ:" << std::dec << (uint64_t)redoLogRecord->seq <<
                        " OP:" << (uint64_t)(redoLogRecord->opCode >> 8) << "." << (uint64_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << std::dec << (uint64_t)encrypted <<
                        " RBL:" << std::dec << redoLogRecord->rbl <<
                        " FLG:0x" << std::setw(4) << std::hex << redoLogRecord->flgRecord << std::endl;
            }
        }

        if (oracleAnalyzer->dumpRawData)
            redoLogRecord->dumpHex(oracleAnalyzer->dumpStream, oracleAnalyzer);
    }

    void OpCode::ktbRedo(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 8)
            return;

        if (redoLogRecord->opc == 0x0A16)
            oracleAnalyzer->dumpStream << "index undo for leaf key operations" << std::endl;
        else if (redoLogRecord->opc == 0x0B01)
            oracleAnalyzer->dumpStream << "KDO undo record:" << std::endl;

        int8_t op = redoLogRecord->data[fieldPos + 0];
        uint8_t flg = redoLogRecord->data[fieldPos + 1];
        uint8_t ver = flg & 0x03;
        if (oracleAnalyzer->dumpRedoLog >= 1) {
            oracleAnalyzer->dumpStream << "KTB Redo " << std::endl;
            oracleAnalyzer->dumpStream << "op: 0x" << std::setfill('0') << std::setw(2) << std::hex << (int32_t)op << " " <<
                    " ver: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)ver << "  " << std::endl;
            oracleAnalyzer->dumpStream << "compat bit: " << std::dec << (uint64_t)(flg & 0x04) << " ";
            if ((flg & 0x04) != 0)
                oracleAnalyzer->dumpStream << "(post-11)";
            else
                oracleAnalyzer->dumpStream << "(pre-11)";

            uint64_t padding = ((flg & 0x10) != 0) ? 0 : 1;
            oracleAnalyzer->dumpStream << " padding: " << padding << std::endl;
        }
        char opCode = '?';

        if ((op & 0x0F) == KTBOP_C) {
            if (fieldLength < 16) {
                WARNING("too short field KTB Redo C: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
                return;
            }

            opCode = 'C';
            if ((flg & 0x04) != 0)
                redoLogRecord->uba = oracleAnalyzer->read56(redoLogRecord->data + fieldPos + 8);
            else
                redoLogRecord->uba = oracleAnalyzer->read56(redoLogRecord->data + fieldPos + 4);
            if (oracleAnalyzer->dumpRedoLog >= 1) {
                oracleAnalyzer->dumpStream << "op: " << opCode << " " << " uba: " << PRINTUBA(redoLogRecord->uba) << std::endl;
            }
        } else if ((op & 0x0F) == KTBOP_Z) {
            opCode = 'Z';
            if (oracleAnalyzer->dumpRedoLog >= 1) {
                oracleAnalyzer->dumpStream << "op: " << opCode << std::endl;
            }
        } else if ((op & 0x0F) == KTBOP_L) {
            opCode = 'L';
            if ((flg & 0x08) == 0) {
                if (fieldLength < 28) {
                    WARNING("too short field KTB Redo L: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
                    return;
                }
                redoLogRecord->uba = oracleAnalyzer->read56(redoLogRecord->data + fieldPos + 12);

                if (oracleAnalyzer->dumpRedoLog >= 1) {
                    typeXID itlXid = XID(oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 4),
                            oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 6),
                            oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 8));

                    oracleAnalyzer->dumpStream << "op: " << opCode << " " <<
                            " itl:" <<
                            " xid:  " << PRINTXID(itlXid) <<
                            " uba: " << PRINTUBA(redoLogRecord->uba) << std::endl;

                    uint8_t lkc = redoLogRecord->data[fieldPos + 20];
                    uint8_t flag = redoLogRecord->data[fieldPos + 19];
                    char flagStr[5] = "----";
                    if ((flag & 0x80) != 0) flagStr[0] = 'C';
                    if ((flag & 0x40) != 0) flagStr[1] = 'B';
                    if ((flag & 0x20) != 0) flagStr[2] = 'U';
                    if ((flag & 0x10) != 0) flagStr[3] = 'T';
                    typeSCN scnx = oracleAnalyzer->readSCNr(redoLogRecord->data + fieldPos + 26);

                    if (oracleAnalyzer->version < REDO_VERSION_12_2)
                        oracleAnalyzer->dumpStream << "                     " <<
                                " flg: " << flagStr << "   " <<
                                " lkc:  " << (uint64_t)lkc << "    " <<
                                " fac: " << PRINTSCN48(scnx) << std::endl;
                    else
                        oracleAnalyzer->dumpStream << "                     " <<
                                " flg: " << flagStr << "   " <<
                                " lkc:  " << (uint64_t)lkc << "    " <<
                                " fac:  " << PRINTSCN64(scnx) << std::endl;
                }

            } else {
                if (fieldLength < 32) {
                    WARNING("too short field KTB Redo L2: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
                    return;
                }
                redoLogRecord->uba = oracleAnalyzer->read56(redoLogRecord->data + fieldPos + 16);

                if (oracleAnalyzer->dumpRedoLog >= 1) {
                    typeXID itlXid = XID(oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 8),
                            oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 10),
                            oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 12));

                    oracleAnalyzer->dumpStream << "op: " << opCode << " " <<
                            " itl:" <<
                            " xid:  " << PRINTXID(itlXid) <<
                            " uba: " << PRINTUBA(redoLogRecord->uba) << std::endl;

                    uint8_t lkc;
                    uint8_t flag;
                    if (oracleAnalyzer->isBigEndian()){
                        lkc = redoLogRecord->data[fieldPos + 25];
                        flag = redoLogRecord->data[fieldPos + 24];
                    } else {
                        lkc = redoLogRecord->data[fieldPos + 24];
                        flag = redoLogRecord->data[fieldPos + 25];
                    }
                    char flagStr[5] = "----";
                    if ((flag & 0x80) != 0) flagStr[0] = 'C';
                    if ((flag & 0x40) != 0) flagStr[1] = 'B';
                    if ((flag & 0x20) != 0) flagStr[2] = 'U';
                    if ((flag & 0x10) != 0) flagStr[3] = 'T';
                    typeSCN scnx = oracleAnalyzer->readSCNr(redoLogRecord->data + fieldPos + 26);

                    if (oracleAnalyzer->version < REDO_VERSION_12_2)
                        oracleAnalyzer->dumpStream << "                     " <<
                                " flg: " << flagStr << "   " <<
                                " lkc:  " << (uint64_t)lkc << "    " <<
                                " scn: " << PRINTSCN48(scnx) << std::endl;
                    else
                        oracleAnalyzer->dumpStream << "                     " <<
                                " flg: " << flagStr << "   " <<
                                " lkc:  " << (uint64_t)lkc << "    " <<
                                " scn:  " << PRINTSCN64(scnx) << std::endl;
                }
            }

        } else if ((op & 0x0F) == KTBOP_N) {
            opCode = 'N';
            if (oracleAnalyzer->dumpRedoLog >= 1) {
                oracleAnalyzer->dumpStream << "op: " << opCode << std::endl;
            }

        } else if ((op & 0x0F) == KTBOP_F) {
            if (fieldLength < 24) {
                WARNING("too short field KTB Redo F: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
                return;
            }

            opCode = 'F';
            redoLogRecord->xid = XID(oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 8),
                    oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 10),
                    oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 12));
            redoLogRecord->uba = oracleAnalyzer->read56(redoLogRecord->data + fieldPos + 16);

            if (oracleAnalyzer->dumpRedoLog >= 1) {

                oracleAnalyzer->dumpStream << "op: " << opCode << " " <<
                        " xid:  " << PRINTXID(redoLogRecord->xid) <<
                        "    uba: " << PRINTUBA(redoLogRecord->uba) << std::endl;
            }
        }

        //block cleanout record
        if ((op & KTBOP_BLOCKCLEANOUT) != 0) {
            if (oracleAnalyzer->dumpRedoLog >= 1) {
                typeSCN scn = oracleAnalyzer->readSCN(redoLogRecord->data + fieldPos + 48);
                uint8_t opt = redoLogRecord->data[fieldPos + 44];
                uint8_t ver = redoLogRecord->data[fieldPos + 46];
                uint8_t entries = redoLogRecord->data[fieldPos + 45];

                if (oracleAnalyzer->version < REDO_VERSION_12_2)
                    oracleAnalyzer->dumpStream << "Block cleanout record, scn: " <<
                            " " << PRINTSCN48(scn) <<
                            " ver: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)ver <<
                            " opt: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)opt <<
                            ", entries follow..." << std::endl;
                else {
                    char bigscn = 'N';
                    char compat = 'N';
                    if ((ver & 0x08) != 0)
                        bigscn = 'Y';
                    if ((ver & 0x04) != 0)
                        compat = 'Y';
                    uint32_t spare = 0; //FIXME
                    ver &= 0x03;
                    oracleAnalyzer->dumpStream << "Block cleanout record, scn: " <<
                            " " << PRINTSCN64(scn) <<
                            " ver: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)ver <<
                            " opt: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)opt <<
                            " bigscn: " << bigscn <<
                            " compact: " << compat <<
                            " spare: " << std::setfill('0') << std::setw(8) << std::hex << spare <<
                            ", entries follow..." << std::endl;
                }

                if (fieldLength < 56 + entries * (uint64_t)8) {
                    WARNING("too short field KTB Redo F2: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
                    return;
                }

                for (uint64_t j = 0; j < entries; ++j) {
                    uint8_t itli = redoLogRecord->data[fieldPos + 56 + j * 8];
                    uint8_t flg = redoLogRecord->data[fieldPos + 57 + j * 8];
                    typeSCN scnx = oracleAnalyzer->readSCNr(redoLogRecord->data + fieldPos + 58 + j * 8);
                    if (oracleAnalyzer->version < REDO_VERSION_12_1)
                        oracleAnalyzer->dumpStream << "  itli: " << std::dec << (uint64_t)itli << " " <<
                                " flg: " << (uint64_t)flg << " " <<
                                " scn: " << PRINTSCN48(scnx) << std::endl;
                    else if (oracleAnalyzer->version < REDO_VERSION_12_2)
                        oracleAnalyzer->dumpStream << "  itli: " << std::dec << (uint64_t)itli << " " <<
                                " flg: (opt=" << (uint64_t)(flg & 0x03) << " whr=" << (uint64_t)(flg >>2) << ") " <<
                                " scn: " << PRINTSCN48(scnx) << std::endl;
                    else {
                        uint8_t opt = flg & 0x03;
                        uint8_t whr = flg >> 2;
                        oracleAnalyzer->dumpStream << "  itli: " << std::dec << (uint64_t)itli << " " <<
                                " flg: (opt=" << (uint64_t)opt << " whr=" << (uint64_t)whr << ") " <<
                                " scn:  " << PRINTSCN64(scnx) << std::endl;
                    }
                }
            }
        }
    }

    void OpCode::kdoOpCodeIRP(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 48) {
            WARNING("too short field KDO OpCode IRP: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->fb = redoLogRecord->data[fieldPos + 16];
        redoLogRecord->cc = redoLogRecord->data[fieldPos + 18];
        redoLogRecord->sizeDelt = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 40);
        redoLogRecord->slot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 42);
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 44];

        if ((redoLogRecord->fb & FB_L) == 0) {
            redoLogRecord->nridBdba = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 28);
            redoLogRecord->nridSlot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 32);
        }

        if (fieldLength < 45 + ((uint64_t)redoLogRecord->cc + 7) / 8) {
            WARNING("too short field KDO OpCode IRP for nulls: " << std::dec << fieldLength <<
                    " (cc: " << std::dec << (uint64_t)redoLogRecord->cc << ") offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->nullsDelta = fieldPos + 45;
        uint8_t* nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
        uint8_t bits = 1;
        for (uint64_t i = 0; i < (uint64_t)redoLogRecord->cc; ++i) {
            if ((*nulls & bits) == 0)
                redoLogRecord->ccData = i + 1;
            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
        }

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            oracleAnalyzer->dumpStream << "tabn: " << (uint64_t)redoLogRecord->tabn <<
                    " slot: " << std::dec << (uint64_t)redoLogRecord->slot << "(0x" << std::hex << redoLogRecord->slot << ")" <<
                    " size/delt: " << std::dec << redoLogRecord->sizeDelt << std::endl;

            char fbStr[9] = "--------";
            processFbFlags(redoLogRecord->fb, fbStr);
            uint8_t lb = redoLogRecord->data[fieldPos + 17];

            oracleAnalyzer->dumpStream << "fb: " << fbStr <<
                    " lb: 0x" << std::hex << (uint64_t)lb << " " <<
                    " cc: " << std::dec << (uint64_t)redoLogRecord->cc;
            if (fbStr[1] == 'C') {
                uint8_t cki = redoLogRecord->data[fieldPos + 19];
                oracleAnalyzer->dumpStream << " cki: " << std::dec << (uint64_t)cki << std::endl;
            } else
                oracleAnalyzer->dumpStream << std::endl;

            if ((redoLogRecord->fb & FB_F) != 0  && (redoLogRecord->fb & FB_H) == 0) {
                typeDBA hrid1 = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 20);
                typeSLOT hrid2 = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24);
                oracleAnalyzer->dumpStream << "hrid: 0x" << std::setfill('0') << std::setw(8) << std::hex << hrid1 << "." << std::hex << hrid2 << std::endl;
            }

            //next DBA/SLT
            if ((redoLogRecord->fb & FB_L) == 0) {
                oracleAnalyzer->dumpStream << "nrid:  0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->nridBdba << "." << std::hex << redoLogRecord->nridSlot << std::endl;
            }

            if ((redoLogRecord->fb & FB_K) != 0) {
                uint8_t curc = 0; //FIXME
                uint8_t comc = 0; //FIXME
                uint32_t pk = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 20);
                uint16_t pk1 = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24);
                uint32_t nk = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 28);
                uint16_t nk1 = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 32);

                oracleAnalyzer->dumpStream << "curc: " << std::dec << (uint64_t)curc <<
                        " comc: " << std::dec << (uint64_t)comc <<
                        " pk: 0x" << std::setfill('0') << std::setw(8) << std::hex << pk << "." << std::hex << pk1 <<
                        " nk: 0x" << std::setfill('0') << std::setw(8) << std::hex << nk << "." << std::hex << nk1 << std::endl;
            }

            oracleAnalyzer->dumpStream << "null:";
            if (redoLogRecord->cc >= 11)
                oracleAnalyzer->dumpStream << std::endl << "01234567890123456789012345678901234567890123456789012345678901234567890123456789" << std::endl;
            else
                oracleAnalyzer->dumpStream << " ";

            uint8_t* nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
            uint8_t bits = 1;
            for (uint64_t i = 0; i < redoLogRecord->cc; ++i) {

                if ((*nulls & bits) != 0)
                    oracleAnalyzer->dumpStream << "N";
                else
                    oracleAnalyzer->dumpStream << "-";
                if ((i % 80) == 79 && i < redoLogRecord->cc)
                    oracleAnalyzer->dumpStream << std::endl;

                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }
            oracleAnalyzer->dumpStream << std::endl;
        }
    }

    void OpCode::kdoOpCodeDRP(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 20) {
            WARNING("too short field KDO OpCode DRP: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->slot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 16);
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 18];

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            oracleAnalyzer->dumpStream << "tabn: " << (uint64_t)redoLogRecord->tabn <<
                    " slot: " << std::dec << (uint64_t)redoLogRecord->slot << "(0x" << std::hex << redoLogRecord->slot << ")" << std::endl;
        }
    }

    void OpCode::kdoOpCodeLKR(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 20) {
            WARNING("too short field KDO OpCode LKR: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->slot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 16);
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 18];

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint8_t to = redoLogRecord->data[fieldPos + 19];
            oracleAnalyzer->dumpStream << "tabn: "<< (uint64_t)redoLogRecord->tabn <<
                " slot: " << std::dec << redoLogRecord->slot <<
                " to: " << std::dec << (uint64_t)to << std::endl;
        }
    }

    void OpCode::kdoOpCodeURP(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 28) {
            WARNING("too short field KDO OpCode URP: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->fb = redoLogRecord->data[fieldPos + 16];
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 19];
        redoLogRecord->slot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 20);
        redoLogRecord->cc = redoLogRecord->data[fieldPos + 23];

        if (fieldLength < 26 + ((uint64_t)redoLogRecord->cc + 7) / 8) {
            WARNING("too short field KDO OpCode IRP for nulls: " << std::dec << fieldLength <<
                    " (cc: " << std::dec << (uint64_t)redoLogRecord->cc << ") offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->nullsDelta = fieldPos + 26;
        uint8_t* nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
        uint8_t bits = 1;
        for (uint64_t i = 0; i < (uint64_t)redoLogRecord->cc; ++i) {
            if ((*nulls & bits) == 0)
                redoLogRecord->ccData = i + 1;
            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
        }

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint8_t lock = redoLogRecord->data[fieldPos + 17];
            uint8_t ckix = redoLogRecord->data[fieldPos + 18];
            uint8_t ncol = redoLogRecord->data[fieldPos + 22];
            int16_t size = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24); //signed

            oracleAnalyzer->dumpStream << "tabn: "<< (uint64_t)redoLogRecord->tabn <<
                    " slot: " << std::dec << redoLogRecord->slot << "(0x" << std::hex << redoLogRecord->slot << ")" <<
                    " flag: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)redoLogRecord->fb <<
                    " lock: " << std::dec << (uint64_t)lock <<
                    " ckix: " << std::dec << (uint64_t)ckix << std::endl;
            oracleAnalyzer->dumpStream << "ncol: " << std::dec << (uint64_t)ncol <<
                    " nnew: " << std::dec << (uint64_t)redoLogRecord->cc <<
                    " size: " << std::dec << size << std::endl;
        }
    }

    void OpCode::kdoOpCodeCFA(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 32) {
            WARNING("too short field KDO OpCode ORP: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->nridBdba = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 16);
        redoLogRecord->nridSlot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 20);
        redoLogRecord->slot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24);
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 27];

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint8_t flag = redoLogRecord->data[fieldPos + 26];
            uint8_t lock = redoLogRecord->data[fieldPos + 28];
            oracleAnalyzer->dumpStream <<
                    "tabn: " << std::dec << (uint64_t)redoLogRecord->tabn <<
                    " slot: " << std::dec << redoLogRecord->slot << "(0x" << std::hex << redoLogRecord->slot << ")" <<
                    " flag: 0x" << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)flag << std::endl <<
                    "lock: " << std::dec << (uint64_t)lock <<
                    " nrid: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->nridBdba << "." << std::hex << redoLogRecord->nridSlot << std::endl;
        }
    }

    void OpCode::kdoOpCodeSKL(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 20) {
            WARNING("too short field KDO OpCode SKL: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->slot = redoLogRecord->data[fieldPos + 27];

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint8_t flagStr[3] = "--";
            uint8_t lock = redoLogRecord->data[fieldPos + 29];
            uint8_t flag = redoLogRecord->data[fieldPos + 28];
            if ((flag & 0x01) != 0) flagStr[0] = 'F';
            if ((flag & 0x02) != 0) flagStr[1] = 'B';

            oracleAnalyzer->dumpStream << "flag: " << flagStr <<
                    " lock: " << std::dec << (uint64_t)lock <<
                    " slot: " << std::dec << redoLogRecord->slot << "(0x" << std::hex << redoLogRecord->slot << ")" << std::endl;

            if ((flag & 0x01) != 0) {
                uint8_t fwd[4];
                uint16_t fwd2 = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 20);
                memcpy(fwd, redoLogRecord->data + fieldPos + 16, 4);
                oracleAnalyzer->dumpStream << "fwd: 0x" <<
                        std::setfill('0') << std::setw(2) << std::hex << (uint64_t)fwd[0] <<
                        std::setfill('0') << std::setw(2) << std::hex << (uint64_t)fwd[1] <<
                        std::setfill('0') << std::setw(2) << std::hex << (uint64_t)fwd[2] <<
                        std::setfill('0') << std::setw(2) << std::hex << (uint64_t)fwd[3] << "." <<
                        std::dec << fwd2 << " " << std::endl;
            }

            if ((flag & 0x02) != 0) {
                uint8_t bkw[4];
                uint16_t bkw2 = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 26);
                memcpy(bkw, redoLogRecord->data + fieldPos + 22, 4);
                oracleAnalyzer->dumpStream << "bkw: 0x" <<
                        std::setfill('0') << std::setw(2) << std::hex << (uint64_t)bkw[0] <<
                        std::setfill('0') << std::setw(2) << std::hex << (uint64_t)bkw[1] <<
                        std::setfill('0') << std::setw(2) << std::hex << (uint64_t)bkw[2] <<
                        std::setfill('0') << std::setw(2) << std::hex << (uint64_t)bkw[3] << "." <<
                        std::dec << bkw2 << std::endl;
            }
        }
    }

    void OpCode::kdoOpCodeORP(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 48) {
            WARNING("too short field KDO OpCode ORP: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->fb = redoLogRecord->data[fieldPos + 16];
        redoLogRecord->cc = redoLogRecord->data[fieldPos + 18];
        redoLogRecord->slot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 42);
        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 44];

        if (fieldLength < 45 + ((uint64_t)redoLogRecord->cc + 7) / 8) {
            WARNING("too short field KDO OpCode ORP for nulls: " << std::dec << fieldLength <<
                    " (cc: " << std::dec << (uint64_t)redoLogRecord->cc << ") offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->nullsDelta = fieldPos + 45;
        uint8_t* nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
        uint8_t bits = 1;
        for (uint64_t i = 0; i < (uint64_t)redoLogRecord->cc; ++i) {
            if ((*nulls & bits) == 0)
                redoLogRecord->ccData = i + 1;
            bits <<= 1;
            if (bits == 0) {
                bits = 1;
                ++nulls;
            }
        }

        if ((redoLogRecord->fb & FB_L) == 0) {
            redoLogRecord->nridBdba = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 28);
            redoLogRecord->nridSlot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 32);
        }
        redoLogRecord->sizeDelt = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 40);

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            oracleAnalyzer->dumpStream << "tabn: "<< (uint64_t)redoLogRecord->tabn <<
                " slot: " << std::dec << (uint64_t)redoLogRecord->slot << "(0x" << std::hex << (uint64_t)redoLogRecord->slot << ")" <<
                " size/delt: " << std::dec << redoLogRecord->sizeDelt << std::endl;

            char fbStr[9] = "--------";
            processFbFlags(redoLogRecord->fb, fbStr);
            uint8_t lb = redoLogRecord->data[fieldPos + 17];

            oracleAnalyzer->dumpStream << "fb: " << fbStr <<
                    " lb: 0x" << std::hex << (uint64_t)lb << " " <<
                    " cc: " << std::dec << (uint64_t)redoLogRecord->cc;
            if (fbStr[1] == 'C') {
                uint8_t cki = redoLogRecord->data[fieldPos + 19];
                oracleAnalyzer->dumpStream << " cki: " << std::dec << (uint64_t)cki << std::endl;
            } else
                oracleAnalyzer->dumpStream << std::endl;

            if ((redoLogRecord->fb & FB_L) == 0) {
                oracleAnalyzer->dumpStream << "nrid:  0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->nridBdba << "." << std::hex << redoLogRecord->nridSlot << std::endl;
            }

            oracleAnalyzer->dumpStream << "null:";
            if (redoLogRecord->cc >= 11)
                oracleAnalyzer->dumpStream << std::endl << "01234567890123456789012345678901234567890123456789012345678901234567890123456789" << std::endl;
            else
                oracleAnalyzer->dumpStream << " ";

            uint8_t* nulls = redoLogRecord->data + redoLogRecord->nullsDelta;
            uint8_t bits = 1;
            for (uint64_t i = 0; i < redoLogRecord->cc; ++i) {

                if ((*nulls & bits) != 0)
                    oracleAnalyzer->dumpStream << "N";
                else
                    oracleAnalyzer->dumpStream << "-";
                if ((i % 80) == 79 && i < redoLogRecord->cc)
                    oracleAnalyzer->dumpStream << std::endl;

                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }
            oracleAnalyzer->dumpStream << std::endl;
        }
    }

    void OpCode::kdoOpCodeQM(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 24) {
            WARNING("too short field KDO OpCode QMI (1): " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->tabn = redoLogRecord->data[fieldPos + 16];
        redoLogRecord->nrow = redoLogRecord->data[fieldPos + 18];
        redoLogRecord->slotsDelta = fieldPos + 20;

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint8_t lock = redoLogRecord->data[fieldPos + 17];

            oracleAnalyzer->dumpStream << "tabn: "<< (uint64_t)redoLogRecord->tabn <<
                " lock: " << std::dec << (uint64_t)lock <<
                " nrow: " << std::dec << (uint64_t)redoLogRecord->nrow << std::endl;

            if (fieldLength < 22 + (uint64_t)redoLogRecord->nrow * 2) {
                WARNING("too short field KDO OpCode QMI (2): " << std::dec << fieldLength << ", " <<
                        (uint64_t)redoLogRecord->nrow << " offset: " << redoLogRecord->dataOffset);
                return;
            }
        }
    }

    void OpCode::kdoOpCode(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength) {
        if (fieldLength < 16) {
            WARNING("too short field KDO OpCode: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->bdba = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->op = redoLogRecord->data[fieldPos + 10];
        redoLogRecord->flags = redoLogRecord->data[fieldPos + 11];
        redoLogRecord->itli = redoLogRecord->data[fieldPos + 12];

        if (oracleAnalyzer->dumpRedoLog >= 1) {
            typeDBA hdba = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 4);
            uint16_t maxFr = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 8);
            uint8_t ispac = redoLogRecord->data[fieldPos + 13];

            const char* opCode = "???";
            switch (redoLogRecord->op & 0x1F) {
                case OP_IUR: opCode = "IUR"; break; //Interpret Undo Redo
                case OP_IRP: opCode = "IRP"; break; //Insert Row Piece
                case OP_DRP: opCode = "DRP"; break; //Delete Row Piece
                case OP_LKR: opCode = "LKR"; break; //LocK Row
                case OP_URP: opCode = "URP"; break; //Update Row Piece
                case OP_ORP: opCode = "ORP"; break; //Overwrite Row Piece
                case OP_MFC: opCode = "MFC"; break; //Manipulate First Column
                case OP_CFA: opCode = "CFA"; break; //Change Forwarding Address
                case OP_CKI: opCode = "CKI"; break; //Change Cluster key Index
                case OP_SKL: opCode = "SKL"; break; //Set Key Links
                case OP_QMI: opCode = "QMI"; break; //Quick Multi-row Insert
                case OP_QMD: opCode = "QMD"; break; //Quick Multi-row Delete
                case OP_DSC: opCode = "DSC"; break;
                case OP_LMN: opCode = "LMN"; break;
                case OP_LLB: opCode = "LLB"; break;
                case OP_SHK: opCode = "SHK"; break;
                case OP_CMP: opCode = "CMP"; break;
                case OP_DCU: opCode = "DCU"; break;
                case OP_MRK: opCode = "MRK"; break;
                case OP__21: opCode = " 21"; break;
                default:
                    opCode = "XXX";
                    if (oracleAnalyzer->dumpRedoLog >= 1)
                        oracleAnalyzer->dumpStream << "DEBUG op: " << std::dec << (uint64_t)(redoLogRecord->op & 0x1F) << std::endl;
            }

            const char* xtype("0");
            const char* rtype("");
            switch (redoLogRecord->flags & 0x03) {
            case FLAGS_XA:
                xtype = "XA"; //redo
                break;
            case FLAGS_XR:
                xtype = "XR"; //rollback
                break;
            case FLAGS_CR:
                xtype = "CR"; //unknown
                break;
            }
            redoLogRecord->flags &= 0xFC;

            if ((redoLogRecord->flags & FLAGS_KDO_KDOM2) != 0)
                rtype = "xtype KDO_KDOM2";

            const char* rowDependencies("Disabled");
            if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0)
                rowDependencies = "Enabled";

            oracleAnalyzer->dumpStream << "KDO Op code: " << opCode << " row dependencies " << rowDependencies << std::endl;
            oracleAnalyzer->dumpStream << "  xtype: " << xtype << rtype <<
                    " flags: 0x" << std::setfill('0') << std::setw(8) << std::hex << (uint64_t)redoLogRecord->flags << " " <<
                    " bdba: 0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->bdba << " " <<
                    " hdba: 0x" << std::setfill('0') << std::setw(8) << std::hex << hdba << std::endl;
            oracleAnalyzer->dumpStream << "itli: " << std::dec << (uint64_t)redoLogRecord->itli << " " <<
                    " ispac: " << std::dec << (uint64_t)ispac << " " <<
                    " maxfr: " << std::dec << (uint64_t)maxFr << std::endl;

            switch ((redoLogRecord->op & 0x1F)) {
            case OP_SKL:
                if (fieldLength >= 32) {
                    char fwdFl = '-';
                    uint32_t fwd = (((uint32_t)redoLogRecord->data[fieldPos + 16]) << 24) |
                            (((uint32_t)redoLogRecord->data[fieldPos + 17]) << 16) |
                            (((uint32_t)redoLogRecord->data[fieldPos + 18]) << 8) |
                            (((uint32_t)redoLogRecord->data[fieldPos + 19]));
                    uint16_t fwdPos = (((uint16_t)redoLogRecord->data[fieldPos + 20]) << 8) |
                            (((uint16_t)redoLogRecord->data[fieldPos + 21]));
                    char bkwFl = '-';
                    uint32_t bkw = (((uint32_t)redoLogRecord->data[fieldPos + 22]) << 24) |
                            (((uint32_t)redoLogRecord->data[fieldPos + 23]) << 16) |
                            (((uint32_t)redoLogRecord->data[fieldPos + 24]) << 8) |
                            (((uint32_t)redoLogRecord->data[fieldPos + 25]));
                    uint16_t bkwPos = (((uint16_t)redoLogRecord->data[fieldPos + 26]) << 8) |
                            (((uint16_t)redoLogRecord->data[fieldPos + 27]));
                    uint8_t fl = redoLogRecord->data[fieldPos + 28];
                    uint8_t lock = redoLogRecord->data[fieldPos + 29];
                    uint8_t slot = redoLogRecord->data[fieldPos + 30];

                    if (fl & 0x01) fwdFl = 'F';
                    if (fl & 0x02) bkwFl = 'B';

                    oracleAnalyzer->dumpStream << "flag: " << fwdFl << bkwFl <<
                            " lock: " << std::dec << (uint64_t)lock <<
                            " slot: " << std::dec << (uint64_t)slot << "(0x" << std::hex << (uint64_t)slot << ")" << std::endl;

                    if (fwdFl == 'F')
                        oracleAnalyzer->dumpStream << "fwd: 0x" << std::setfill('0') << std::setw(8) << std::hex << fwd <<"." << fwdPos << " " << std::endl;
                    if (bkwFl == 'B')
                        oracleAnalyzer->dumpStream << "bkw: 0x" << std::setfill('0') << std::setw(8) << std::hex << bkw <<"." << bkwPos << std::endl;
                }
                break;

            case OP_DSC:
                if (fieldLength >= 24) {
                    uint16_t slot = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 16);
                    uint8_t tabn = redoLogRecord->data[fieldPos + 18];
                    uint8_t rel = redoLogRecord->data[fieldPos + 19];

                    oracleAnalyzer->dumpStream << "tabn: " << std::dec << (uint64_t)tabn << " slot: " << slot << "(0x" << std::hex << slot << ")" << std::endl;
                    oracleAnalyzer->dumpStream << "piece relative column number: " << std::dec << (uint64_t)rel << std::endl;
                }
                break;
            }
        }

        switch (redoLogRecord->op & 0x1F) {
        case OP_IRP: kdoOpCodeIRP(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_DRP: kdoOpCodeDRP(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_LKR: kdoOpCodeLKR(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_URP: kdoOpCodeURP(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_ORP: kdoOpCodeORP(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_CKI: kdoOpCodeSKL(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_CFA: kdoOpCodeCFA(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);
                     break;
        case OP_QMI:
        case OP_QMD: kdoOpCodeQM(oracleAnalyzer, redoLogRecord, fieldPos, fieldLength);
                     break;
        }
    }

    void OpCode::ktub(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength, bool isKtubl) {
        if (fieldLength < 24) {
            WARNING("too short field ktub: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
            return;
        }

        redoLogRecord->obj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->dataObj = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 4);
        redoLogRecord->tsn = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 8);
        redoLogRecord->undo = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 12);
        redoLogRecord->opc = (((typeOP1)redoLogRecord->data[fieldPos + 16]) << 8) | redoLogRecord->data[fieldPos + 17];
        redoLogRecord->slt = redoLogRecord->data[fieldPos + 18];
        redoLogRecord->rci = redoLogRecord->data[fieldPos + 19];
        redoLogRecord->flg = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 20);

        const char* ktuType("ktubu");
        const char* prevObj("");
        const char* postObj("");
        bool ktubl = false;

        if ((redoLogRecord->flg & FLG_BEGIN_TRANS) != 0 && isKtubl) {
            ktubl = true;
            ktuType = "ktubl";
            if (oracleAnalyzer->version < REDO_VERSION_19_0) {
                prevObj = "[";
                postObj = "]";
            }
        }

        if (oracleAnalyzer->version < REDO_VERSION_19_0) {
            oracleAnalyzer->dumpStream <<
                    ktuType << " redo:" <<
                    " slt: " << std::dec << (uint64_t)redoLogRecord->slt <<
                    " rci: " << std::dec << (uint64_t)redoLogRecord->rci <<
                    " opc: " << std::dec << (uint64_t)(redoLogRecord->opc >> 8) << "." << (uint64_t)(redoLogRecord->opc & 0xFF) <<
                    " " << prevObj << "objn: " << std::dec << redoLogRecord->obj <<
                    " objd: " << std::dec << redoLogRecord->dataObj <<
                    " tsn: " << std::dec << redoLogRecord->tsn << postObj << std::endl;
        } else {
            typeDBA prevDba = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 12);
            uint16_t wrp = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 22);

            oracleAnalyzer->dumpStream <<
                    ktuType << " redo:" <<
                    " slt: "  << std::dec << (uint64_t)redoLogRecord->slt <<
                    " wrp: " << std::dec << wrp <<
                    " flg: 0x" << std::setfill('0') << std::setw(4) << std::hex << redoLogRecord->flg <<
                    " prev dba:  0x" << std::setfill('0') << std::setw(8) << std::hex << prevDba <<
                    " rci: " << std::dec << (uint64_t)redoLogRecord->rci <<
                    " opc: " << std::dec << (uint64_t)(redoLogRecord->opc >> 8) << "." << (uint64_t)(redoLogRecord->opc & 0xFF) <<
                    " [objn: " << std::dec << redoLogRecord->obj <<
                    " objd: " << std::dec << redoLogRecord->dataObj <<
                    " tsn: " << std::dec << redoLogRecord->tsn << "]" << std::endl;
        }

        const char* lastBufferSplit;
        if ((redoLogRecord->flg & FLG_LASTBUFFERSPLIT) != 0)
            lastBufferSplit = "Yes";
        else {
            if (oracleAnalyzer->version < REDO_VERSION_19_0)
                lastBufferSplit = "No";
            else
                lastBufferSplit = " No";
        }

        const char* userUndoDone;
        if ((redoLogRecord->flg & FLG_USERUNDODDONE) != 0)
            userUndoDone = "Yes";
        else {
            if (oracleAnalyzer->version < REDO_VERSION_19_0)
                userUndoDone = "No";
            else
                userUndoDone = " No";
        }

        const char* undoType;
        if (oracleAnalyzer->version < REDO_VERSION_12_2) {
            if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOHEAD) != 0)
                undoType = "Multi-block undo - HEAD";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOTAIL) != 0)
                undoType = "Multi-Block undo - TAIL";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOMID) != 0)
                undoType = "Multi-block undo - MID";
            else
                undoType = "Regular undo      ";
        } else if (oracleAnalyzer->version < REDO_VERSION_19_0) {
            if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOHEAD) != 0)
                undoType = "Multi-block undo - HEAD";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOTAIL) != 0)
                undoType = "Multi-Block undo - TAIL";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOMID) != 0)
                undoType = "Multi-Block undo - MID";
            else
                undoType = "Regular undo      ";
        } else {
            if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOHEAD) != 0)
                undoType = "MBU - HEAD  ";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOTAIL) != 0)
                undoType = "MBU - TAIL  ";
            else if ((redoLogRecord->flg & FLG_MULTIBLOCKUNDOMID) != 0)
                undoType = "MBU - MID   ";
            else
                undoType = "Regular undo";
        }

        const char* tempObject;
        if ((redoLogRecord->flg & FLG_ISTEMPOBJECT) != 0)
            tempObject = "Yes";
        else {
            if (oracleAnalyzer->version < REDO_VERSION_19_0)
                tempObject = "No";
            else
                tempObject = " No";
        }

        const char* tablespaceUndo;
        if ((redoLogRecord->flg & FLG_TABLESPACEUNDO) != 0)
            tablespaceUndo = "Yes";
        else {
            if (oracleAnalyzer->version < REDO_VERSION_19_0)
                tablespaceUndo = "No";
            else
                tablespaceUndo = " No";
        }

        const char* userOnly(" No");
        if ((redoLogRecord->flg & FLG_USERONLY) != 0)
            userOnly = "Yes";
        else {
            if (oracleAnalyzer->version < REDO_VERSION_19_0)
                userOnly = "No";
            else
                userOnly = " No";
        }

        if (ktubl) {
            //KTUBL
            if (fieldLength < 28) {
                WARNING("too short field ktubl: " << std::dec << fieldLength << " offset: " << redoLogRecord->dataOffset);
                return;
            }

            if (fieldLength == 28) {
                if (oracleAnalyzer->dumpRedoLog >= 1) {
                    uint16_t flg2 = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24);
                    int16_t buExtIdx = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 26);

                    if (oracleAnalyzer->version < REDO_VERSION_12_2) {
                        oracleAnalyzer->dumpStream <<
                                "Undo type:  " << undoType << "  " <<
                                "Begin trans    Last buffer split:  " << lastBufferSplit << " " << std::endl <<
                                "Temp Object:  " << tempObject << " " << std::endl <<
                                "Tablespace Undo:  " << tablespaceUndo << " " << std::endl <<
                                "             0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->undo << " " << std::endl;

                        oracleAnalyzer->dumpStream <<
                                " BuExt idx: " << std::dec << buExtIdx <<
                                " flg2: " << std::hex << flg2 << std::endl;
                    } else if (oracleAnalyzer->version < REDO_VERSION_19_0) {
                        oracleAnalyzer->dumpStream <<
                                "Undo type:  " << undoType << "  " <<
                                "Begin trans    Last buffer split:  " << lastBufferSplit << " " << std::endl <<
                                "Temp Object:  " << tempObject << " " << std::endl <<
                                "Tablespace Undo:  " << tablespaceUndo << " " << std::endl <<
                                "             0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->undo << " " << std::endl;

                        oracleAnalyzer->dumpStream <<
                                " BuExt idx: " << std::dec << buExtIdx <<
                                " flg2: " << std::hex << flg2 << std::endl;
                    } else {
                        oracleAnalyzer->dumpStream <<
                                "[Undo type  ] " << undoType << " " <<
                                " [User undo done   ] " << userUndoDone << " " <<
                                " [Last buffer split] " << lastBufferSplit << " " << std::endl <<
                                "[Temp object]          " << tempObject << " " <<
                                " [Tablespace Undo  ] " << tablespaceUndo << " " <<
                                " [User only        ] " << userOnly << " " << std::endl <<
                                "Begin trans    " << std::endl;

                        oracleAnalyzer->dumpStream <<
                                "BuExt idx: " << std::dec << buExtIdx <<
                                " flg2: " << std::hex << flg2 << std::endl;
                    }
                }
            } else if (fieldLength >= 76) {
                if (oracleAnalyzer->dumpRedoLog >= 1) {
                    uint16_t flg2 = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24);
                    int16_t buExtIdx = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 26);
                    typeUBA prevCtlUba = oracleAnalyzer->read56(redoLogRecord->data + fieldPos + 28);
                    typeSCN prevCtlMaxCmtScn = oracleAnalyzer->readSCN(redoLogRecord->data + fieldPos + 36);
                    typeSCN prevTxCmtScn = oracleAnalyzer->readSCN(redoLogRecord->data + fieldPos + 44);
                    typeSCN txStartScn = oracleAnalyzer->readSCN(redoLogRecord->data + fieldPos + 56);
                    uint32_t prevBrb = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 64);
                    uint32_t prevBcl = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 68);
                    uint32_t logonUser = oracleAnalyzer->read32(redoLogRecord->data + fieldPos + 72);

                    if (oracleAnalyzer->version < REDO_VERSION_12_2) {
                        oracleAnalyzer->dumpStream <<
                                "Undo type:  " << undoType << "  " <<
                                "Begin trans    Last buffer split:  " << lastBufferSplit << " " << std::endl <<
                                "Temp Object:  " << tempObject << " " << std::endl <<
                                "Tablespace Undo:  " << tablespaceUndo << " " << std::endl <<
                                "             0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->undo << " " <<
                                " prev ctl uba: " << PRINTUBA(prevCtlUba) << " " << std::endl <<
                                "prev ctl max cmt scn:  " << PRINTSCN48(prevCtlMaxCmtScn) << " " <<
                                " prev tx cmt scn:  " << PRINTSCN48(prevTxCmtScn) << " " << std::endl;

                        oracleAnalyzer->dumpStream <<
                                "txn start scn:  " << PRINTSCN48(txStartScn) << " " <<
                                " logon user: " << std::dec << logonUser << " " <<
                                " prev brb: " << prevBrb << " " <<
                                " prev bcl: " << std::dec << prevBcl;

                        oracleAnalyzer->dumpStream <<
                                " BuExt idx: " << std::dec << buExtIdx <<
                                " flg2: " << std::hex << flg2 << std::endl;
                    } else if (oracleAnalyzer->version < REDO_VERSION_19_0) {
                        oracleAnalyzer->dumpStream <<
                                "Undo type:  " << undoType << "  " <<
                                "Begin trans    Last buffer split:  " << lastBufferSplit << " " << std::endl <<
                                "Temp Object:  " << tempObject << " " << std::endl <<
                                "Tablespace Undo:  " << tablespaceUndo << " " << std::endl <<
                                "             0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->undo << " " <<
                                " prev ctl uba: " << PRINTUBA(prevCtlUba) << " " << std::endl <<
                                "prev ctl max cmt scn:  " << PRINTSCN64(prevCtlMaxCmtScn) << " " <<
                                " prev tx cmt scn:  " << PRINTSCN64(prevTxCmtScn) << " " << std::endl;

                        oracleAnalyzer->dumpStream <<
                                "txn start scn:  " << PRINTSCN64(txStartScn) << " " <<
                                " logon user: " << std::dec << logonUser << " " <<
                                " prev brb: " << prevBrb << " " <<
                                " prev bcl: " << std::dec << prevBcl;

                        oracleAnalyzer->dumpStream <<
                                " BuExt idx: " << std::dec << buExtIdx <<
                                " flg2: " << std::hex << flg2 << std::endl;
                    } else {
                        oracleAnalyzer->dumpStream <<
                                "[Undo type  ] " << undoType << " " <<
                                " [User undo done   ] " << userUndoDone << " " <<
                                " [Last buffer split] " << lastBufferSplit << " " << std::endl <<
                                "[Temp object]          " << tempObject << " " <<
                                " [Tablespace Undo  ] " << tablespaceUndo << " " <<
                                " [User only        ] " << userOnly << " " << std::endl <<
                                "Begin trans    " << std::endl <<
                                " prev ctl uba: " << PRINTUBA(prevCtlUba) <<
                                " prev ctl max cmt scn:  " << PRINTSCN64(prevCtlMaxCmtScn) << " " << std::endl <<
                                " prev tx cmt scn:  " << PRINTSCN64(prevTxCmtScn) << " " << std::endl;

                        oracleAnalyzer->dumpStream <<
                                " txn start scn:  " << PRINTSCN64(txStartScn) <<
                                "  logon user: " << std::dec << logonUser << std::endl <<
                                " prev brb:  0x" << std::setfill('0') << std::setw(8) << std::hex << prevBrb <<
                                "  prev bcl:  0x" << std::setfill('0') << std::setw(8) << std::hex << prevBcl << std::endl;

                        oracleAnalyzer->dumpStream <<
                                "BuExt idx: " << std::dec << buExtIdx <<
                                " flg2: " << std::hex << flg2 << std::endl;
                    }
                }
            }
        } else {
            //KTUBU
            if (oracleAnalyzer->dumpRedoLog >= 1) {
                if (oracleAnalyzer->version < REDO_VERSION_19_0) {
                    oracleAnalyzer->dumpStream <<
                            "Undo type:  " << undoType << " " <<
                            "Undo type:  ";
                    if ((redoLogRecord->flg & FLG_USERUNDODDONE) != 0)
                        oracleAnalyzer->dumpStream << "User undo done   ";
                    if ((redoLogRecord->flg & FLG_BEGIN_TRANS) != 0)
                        oracleAnalyzer->dumpStream << " Begin trans    ";
                    oracleAnalyzer->dumpStream <<
                            "Last buffer split:  " << lastBufferSplit << " " << std::endl <<
                            "Tablespace Undo:  " << tablespaceUndo << " " << std::endl <<
                            "             0x" << std::setfill('0') << std::setw(8) << std::hex << redoLogRecord->undo << std::endl;

                    if ((redoLogRecord->flg & FLG_BUEXT) != 0) {
                        uint16_t flg2 = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 24);
                        int16_t buExtIdx = oracleAnalyzer->read16(redoLogRecord->data + fieldPos + 26);

                        oracleAnalyzer->dumpStream <<
                                "BuExt idx: " << std::dec << buExtIdx <<
                                " flg2: " << std::hex << flg2 << std::endl;
                    }

                } else {
                    oracleAnalyzer->dumpStream <<
                            "[Undo type  ] " << undoType << " " <<
                            " [User undo done   ] " << userUndoDone << " " <<
                            " [Last buffer split] " << lastBufferSplit << " " << std::endl <<
                            "[Temp object]          " << tempObject << " " <<
                            " [Tablespace Undo  ] " << tablespaceUndo << " " <<
                            " [User only        ] " << userOnly << " " << std::endl;
                }
            }
        }
    }

    void OpCode::dumpColsVector(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint8_t* data, uint64_t colnum, uint16_t fieldLength) {
        uint64_t pos = 0;

        oracleAnalyzer->dumpStream << "Vector content: " << std::endl;

        for (uint64_t k = 0; k < redoLogRecord->cc; ++k) {
            uint16_t fieldLength = data[pos];
            ++pos;
            uint8_t isNull = (fieldLength == 0xFF);

            if (fieldLength == 0xFE) {
                fieldLength = oracleAnalyzer->read16(data + pos);
                pos += 2;
            }

            dumpCols(oracleAnalyzer, redoLogRecord, data + pos, colnum + k, fieldLength, isNull);

            if (!isNull)
                pos += fieldLength;
        }
    }

    void OpCode::dumpCompressed(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint8_t* data, uint16_t fieldLength) {
        std::stringstream ss;
        ss << "kdrhccnt=" << std::dec << (uint64_t)redoLogRecord->cc << ",full row:";
        ss << std::uppercase;

        for (uint64_t j = 0; j < fieldLength; ++j) {
            ss << " " << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)data[j];
            if (ss.str().length() > 256) {
                oracleAnalyzer->dumpStream << ss.str() << std::endl;
                ss.str(std::string());
            }
        }

        if (ss.str().length()  > 0) {
            oracleAnalyzer->dumpStream << ss.str() << std::endl;
        }
    }

    void OpCode::dumpCols(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint8_t* data, uint64_t colnum, uint16_t fieldLength, uint8_t isNull) {
        if (isNull) {
            oracleAnalyzer->dumpStream << "col " << std::setfill(' ') << std::setw(2) << std::dec << colnum << ": *NULL*" << std::endl;
        } else {
            oracleAnalyzer->dumpStream << "col " << std::setfill(' ') << std::setw(2) << std::dec << colnum << ": " <<
                    "[" << std::setfill(' ') << std::setw(2) << std::dec << fieldLength << "]";

            if (fieldLength <= 20)
                oracleAnalyzer->dumpStream << " ";
            else
                oracleAnalyzer->dumpStream << std::endl;

            for (uint64_t j = 0; j < fieldLength; ++j) {
                oracleAnalyzer->dumpStream << " " << std::setfill('0') << std::setw(2) << std::hex << (uint64_t)data[j];
                if ((j % 25) == 24 && j != (uint64_t)fieldLength - 1)
                    oracleAnalyzer->dumpStream << std::endl;
            }

            oracleAnalyzer->dumpStream << std::endl;
        }
    }

    void OpCode::dumpRows(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint8_t* data) {
        if (oracleAnalyzer->dumpRedoLog >= 1) {
            uint64_t pos = 0;
            char fbStr[9] = "--------";

            for (uint64_t r = 0; r < redoLogRecord->nrow; ++r) {
                oracleAnalyzer->dumpStream << "slot[" << std::dec << r << "]: " << std::dec << oracleAnalyzer->read16(redoLogRecord->data + redoLogRecord->slotsDelta + r * 2) << std::endl;
                processFbFlags(data[pos + 0], fbStr);
                uint8_t lb = data[pos + 1];
                uint8_t jcc = data[pos + 2];
                uint16_t tl = oracleAnalyzer->read16(redoLogRecord->data + redoLogRecord->rowLenghsDelta + r * 2);

                oracleAnalyzer->dumpStream << "tl: " << std::dec << tl <<
                        " fb: " << fbStr <<
                        " lb: 0x" << std::hex << (uint64_t)lb << " " <<
                        " cc: " << std::dec << (uint64_t)jcc << std::endl;
                pos += 3;

                if ((redoLogRecord->op & OP_ROWDEPENDENCIES) != 0) {
                    if (oracleAnalyzer->version < REDO_VERSION_12_2)
                        pos += 6;
                    else
                        pos += 8;
                }

                for (uint64_t k = 0; k < jcc; ++k) {
                    uint16_t fieldLength = data[pos];
                    ++pos;
                    uint8_t isNull = (fieldLength == 0xFF);

                    if (fieldLength == 0xFE) {
                        fieldLength = oracleAnalyzer->read16(data + pos);
                        pos += 2;
                    }

                    dumpCols(oracleAnalyzer, redoLogRecord, data + pos, k, fieldLength, isNull);

                    if (!isNull)
                        pos += fieldLength;
                }
            }
        }
    }

    void OpCode::dumpVal(OracleAnalyzer* oracleAnalyzer, RedoLogRecord* redoLogRecord, uint64_t fieldPos, uint64_t fieldLength, const char* msg) {
        if (oracleAnalyzer->dumpRedoLog >= 1) {
            oracleAnalyzer->dumpStream << msg;
            for (uint64_t i = 0; i < fieldLength; ++i)
                oracleAnalyzer->dumpStream << redoLogRecord->data[fieldPos + i];
            oracleAnalyzer->dumpStream << std::endl;
        }
    }

    void OpCode::processFbFlags(uint8_t fb, char* fbStr) {
        if ((fb & FB_N) != 0) fbStr[7] = 'N'; else fbStr[7] = '-'; //Last column continues in Next piece
        if ((fb & FB_P) != 0) fbStr[6] = 'P'; else fbStr[6] = '-'; //First column continues from Previous piece
        if ((fb & FB_L) != 0) fbStr[5] = 'L'; else fbStr[5] = '-'; //Last data piece
        if ((fb & FB_F) != 0) fbStr[4] = 'F'; else fbStr[4] = '-'; //First data piece
        if ((fb & FB_D) != 0) fbStr[3] = 'D'; else fbStr[3] = '-'; //Deleted row
        if ((fb & FB_H) != 0) fbStr[2] = 'H'; else fbStr[2] = '-'; //Head piece of row
        if ((fb & FB_C) != 0) fbStr[1] = 'C'; else fbStr[1] = '-'; //Clustered table member
        if ((fb & FB_K) != 0) fbStr[0] = 'K'; else fbStr[0] = '-'; //Cluster Key
        fbStr[8] = 0;
    }
}
