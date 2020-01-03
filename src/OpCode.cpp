/* Oracle Redo Generic OpCode
   Copyright (C) 2018-2020 Adam Leszczynski.

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <iostream>
#include <iomanip>
#include <string.h>
#include "OpCode.h"
#include "OracleEnvironment.h"
#include "RedoLogRecord.h"

using namespace std;

namespace OpenLogReplicator {

    OpCode::OpCode(OracleEnvironment *oracleEnvironment, RedoLogRecord *redoLogRecord):
            oracleEnvironment(oracleEnvironment),
            redoLogRecord(redoLogRecord) {
    }

    OpCode::~OpCode() {
    }

    uint16_t OpCode::getOpCode(void) {
        return redoLogRecord->opCode;
    }

    void OpCode::process() {
        if (oracleEnvironment->dumpLogFile) {
            bool encrypted = false;
            if ((redoLogRecord->typ & 0x80) == 0x80)
                encrypted = true;

            if (oracleEnvironment->version < 12000) {
                if (redoLogRecord->typ == 6)
                    oracleEnvironment->dumpStream << "CHANGE #" << dec << (uint32_t)redoLogRecord->vectorNo <<
                        " MEDIA RECOVERY MARKER" <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << dec << (uint32_t)redoLogRecord->seq <<
                        " OP:" << (uint32_t)(redoLogRecord->opCode >> 8) << "." << (uint32_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << dec << (uint32_t)encrypted << endl;
                else
                    oracleEnvironment->dumpStream << "CHANGE #" << dec << (uint32_t)redoLogRecord->vectorNo <<
                        " TYP:" << (uint32_t)redoLogRecord->typ <<
                        " CLS:" << redoLogRecord->cls <<
                        " AFN:" << redoLogRecord->afn <<
                        " DBA:0x" << setfill('0') << setw(8) << hex << redoLogRecord->dba <<
                        " OBJ:" << dec << redoLogRecord->recordObjd <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << dec << (uint32_t)redoLogRecord->seq <<
                        " OP:" << (uint32_t)(redoLogRecord->opCode >> 8) << "." << (uint32_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << dec << (uint32_t)encrypted <<
                        " RBL:" << dec << redoLogRecord->rbl << endl;
            } else if (oracleEnvironment->version < 12200) {
                if (redoLogRecord->typ == 6)
                    oracleEnvironment->dumpStream << "CHANGE #" << dec << (uint32_t)redoLogRecord->vectorNo <<
                        " MEDIA RECOVERY MARKER" <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << dec << (uint32_t)redoLogRecord->seq <<
                        " OP:" << (uint32_t)(redoLogRecord->opCode >> 8) << "." << (uint32_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << dec << (uint32_t)encrypted <<
                        " FLG:0x" << setw(4) << hex << redoLogRecord->flgRecord << endl;
                else
                    oracleEnvironment->dumpStream << "CHANGE #" << dec << (uint32_t)redoLogRecord->vectorNo <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " TYP:" << (uint32_t)redoLogRecord->typ <<
                        " CLS:" << redoLogRecord->cls <<
                        " AFN:" << redoLogRecord->afn <<
                        " DBA:0x" << setfill('0') << setw(8) << hex << redoLogRecord->dba <<
                        " OBJ:" << dec << redoLogRecord->recordObjd <<
                        " SCN:" << PRINTSCN48(redoLogRecord->scnRecord) <<
                        " SEQ:" << dec << (uint32_t)redoLogRecord->seq <<
                        " OP:" << (uint32_t)(redoLogRecord->opCode >> 8) << "." << (uint32_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << dec << (uint32_t)encrypted <<
                        " RBL:" << dec << redoLogRecord->rbl <<
                        " FLG:0x" << setw(4) << hex << redoLogRecord->flgRecord << endl;
            } else {
                if (redoLogRecord->typ == 6)
                    oracleEnvironment->dumpStream << "CHANGE #" << dec << (uint32_t)redoLogRecord->vectorNo <<
                        " MEDIA RECOVERY MARKER" <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " SCN:" << PRINTSCN64(redoLogRecord->scnRecord) <<
                        " SEQ:" << dec << (uint32_t)redoLogRecord->seq <<
                        " OP:" << (uint32_t)(redoLogRecord->opCode >> 8) << "." << (uint32_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << dec << (uint32_t)encrypted <<
                        " FLG:0x" << setw(4) << hex << redoLogRecord->flgRecord << endl;
                else
                    oracleEnvironment->dumpStream << "CHANGE #" << dec << (uint32_t)redoLogRecord->vectorNo <<
                        " CON_ID:" << redoLogRecord->conId <<
                        " TYP:" << (uint32_t)redoLogRecord->typ <<
                        " CLS:" << redoLogRecord->cls <<
                        " AFN:" << redoLogRecord->afn <<
                        " DBA:0x" << setfill('0') << setw(8) << hex << redoLogRecord->dba <<
                        " OBJ:" << dec << redoLogRecord->recordObjd <<
                        " SCN:" << PRINTSCN64(redoLogRecord->scnRecord) <<
                        " SEQ:" << dec << (uint32_t)redoLogRecord->seq <<
                        " OP:" << (uint32_t)(redoLogRecord->opCode >> 8) << "." << (uint32_t)(redoLogRecord->opCode & 0xFF) <<
                        " ENC:" << dec << (uint32_t)encrypted <<
                        " RBL:" << dec << redoLogRecord->rbl <<
                        " FLG:0x" << setw(4) << hex << redoLogRecord->flgRecord << endl;
            }
        }

        if (oracleEnvironment->dumpData) {
            uint32_t fieldOffset = 24;
            if (oracleEnvironment->version >= 12102) fieldOffset = 32;

            oracleEnvironment->dumpStream << "##: " << dec << fieldOffset;
            for (uint32_t j = 0; j < fieldOffset; ++j) {
                if ((j & 0xF) == 0)
                    oracleEnvironment->dumpStream << endl << "##  " << hex << setfill(' ') << setw(2) <<  j << ": ";
                if ((j & 0x7) == 0)
                    oracleEnvironment->dumpStream << " ";
                oracleEnvironment->dumpStream << hex << setfill('0') << setw(2) << (uint32_t) oracleEnvironment->recordBuffer[j] << " ";
            }
            oracleEnvironment->dumpStream << endl;

            uint32_t fieldPos = redoLogRecord->fieldPos;
            for (uint32_t i = 1; i <= redoLogRecord->fieldNum; ++i) {
                oracleEnvironment->dumpStream << "##: " << dec << ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] << " (" << i << ")";
                for (uint32_t j = 0; j < ((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i]; ++j) {
                    if ((j & 0xF) == 0)
                        oracleEnvironment->dumpStream << endl << "##  " << hex << setfill(' ') << setw(2) <<  j << ": ";
                    if ((j & 0x7) == 0)
                        oracleEnvironment->dumpStream << " ";
                    oracleEnvironment->dumpStream << hex << setfill('0') << setw(2) << (uint32_t) redoLogRecord->data[fieldPos + j] << " ";
                }
                oracleEnvironment->dumpStream << endl;

                fieldPos += (((uint16_t*)(redoLogRecord->data + redoLogRecord->fieldLengthsDelta))[i] + 3) & 0xFFFC;
            }
        }
    }

    void OpCode::ktbRedo(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 8) {
            oracleEnvironment->dumpStream << "ERROR: too short field KTB Redo: " << dec << fieldLength << endl;
            return;
        }

        if (isKdoUndo())
            oracleEnvironment->dumpStream << "KDO undo record:" << endl;

        uint8_t op = redoLogRecord->data[fieldPos + 0];
        if (oracleEnvironment->dumpLogFile) {
            uint8_t ver = redoLogRecord->data[fieldPos + 1] & 0x03;

            oracleEnvironment->dumpStream << "KTB Redo " << endl;
            oracleEnvironment->dumpStream << "op: 0x" << setfill('0') << setw(2) << hex << (uint32_t)op << " " <<
                    " ver: 0x" << setfill('0') << setw(2) << hex << (uint32_t)ver << "  " << endl;
            oracleEnvironment->dumpStream << "compat bit: 4 (post-11) padding: 1" << endl;
        }
        char opCode = '?';

        if ((op & 0x0F) == 0x02) {
            if (fieldLength < 16) {
                oracleEnvironment->dumpStream << "ERROR: too short field KTB Redo 4: " << dec << fieldLength << endl;
                return;
            }

            redoLogRecord->uba = oracleEnvironment->read56(redoLogRecord->data + fieldPos + 8);

            if (oracleEnvironment->dumpLogFile) {
                opCode = 'C';
                oracleEnvironment->dumpStream << "op: " << opCode << " " << " uba: " << PRINTUBA(redoLogRecord->uba) << endl;
            }
        } else if ((op & 0x0F) == 0x03) {
            if (oracleEnvironment->dumpLogFile) {
                opCode = 'Z';
                oracleEnvironment->dumpStream << "op: " << opCode << endl;
            }
        } else if ((op & 0x0F) == 0x04) {
            if (fieldLength < 32) {
                oracleEnvironment->dumpStream << "ERROR: too short field KTB Redo 4: " << dec << fieldLength << endl;
                return;
            }

            redoLogRecord->uba = oracleEnvironment->read56(redoLogRecord->data + fieldPos + 16);

            if (oracleEnvironment->dumpLogFile) {
                opCode = 'L';
                typexid itlXid = XID(oracleEnvironment->read16(redoLogRecord->data + fieldPos + 8),
                        oracleEnvironment->read16(redoLogRecord->data + fieldPos + 10),
                        oracleEnvironment->read32(redoLogRecord->data + fieldPos + 12));

                oracleEnvironment->dumpStream << "op: " << opCode << " " <<
                        " itl:" <<
                        " xid:  " << PRINTXID(itlXid) <<
                        " uba: " << PRINTUBA(redoLogRecord->uba) << endl;

                uint8_t lkc = redoLogRecord->data[fieldPos + 24]; //FIXME
                uint8_t flag = redoLogRecord->data[fieldPos + 25];
                char flagStr[5] = "----";
                if ((flag & 0x80) == 0x80) flagStr[0] = 'C';
                if ((flag & 0x40) == 0x40) flagStr[1] = '?';
                if ((flag & 0x20) == 0x20) flagStr[2] = 'U';
                if ((flag & 0x10) == 0x10) flagStr[3] = '?';
                typescn scnx = oracleEnvironment->readSCNr(redoLogRecord->data + fieldPos + 26);

                if (oracleEnvironment->version < 12200)
                    oracleEnvironment->dumpStream << "                     " <<
                            " flg: " << flagStr << "   " <<
                            " lkc:  " << (uint32_t)lkc << "    " <<
                            " scn: " << PRINTSCN48(scnx) << endl;
                else
                    oracleEnvironment->dumpStream << "                     " <<
                            " flg: " << flagStr << "   " <<
                            " lkc:  " << (uint32_t)lkc << "    " <<
                            " scn:  " << PRINTSCN64(scnx) << endl;
            }

        } else if ((op & 0x0F) == 0x01) {
            if (fieldLength < 24) {
                oracleEnvironment->dumpStream << "ERROR: too short field KTB Redo F: " << dec << fieldLength << endl;
                return;
            }

            redoLogRecord->xid = XID(oracleEnvironment->read16(redoLogRecord->data + fieldPos + 8),
                    oracleEnvironment->read16(redoLogRecord->data + fieldPos + 10),
                    oracleEnvironment->read32(redoLogRecord->data + fieldPos + 12));
            redoLogRecord->uba = oracleEnvironment->read56(redoLogRecord->data + fieldPos + 16);

            if (oracleEnvironment->dumpLogFile) {
                opCode = 'F';

                oracleEnvironment->dumpStream << "op: " << opCode << " " <<
                        " xid:  " << PRINTXID(redoLogRecord->xid) <<
                        "    uba: " << PRINTUBA(redoLogRecord->uba) << endl;
            }
        }

        //block cleanout record
        if ((op & 0x10) == 0x10) {
            if (oracleEnvironment->dumpLogFile) {
                typescn scn = oracleEnvironment->readSCN(redoLogRecord->data + fieldPos + 48);
                uint8_t opt = redoLogRecord->data[fieldPos + 44];
                uint8_t ver = redoLogRecord->data[fieldPos + 46];
                uint8_t entries = redoLogRecord->data[fieldPos + 45];

                if (oracleEnvironment->version < 12200)
                    oracleEnvironment->dumpStream << "Block cleanout record, scn: " <<
                            " " << PRINTSCN48(scn) <<
                            " ver: 0x" << setfill('0') << setw(2) << hex << (uint32_t)ver <<
                            " opt: 0x" << setfill('0') << setw(2) << hex << (uint32_t)opt <<
                            ", entries follow..." << endl;
                else {
                    char bigscn = 'N', compat = 'N';
                    if ((ver & 0x08) == 0x08)
                        bigscn = 'Y';
                    if ((ver & 0x04) == 0x04)
                        compat = 'Y';
                    uint32_t spare = 0; //FIXME
                    ver &= 0x03;
                    oracleEnvironment->dumpStream << "Block cleanout record, scn: " <<
                            " " << PRINTSCN64(scn) <<
                            " ver: 0x" << setfill('0') << setw(2) << hex << (uint32_t)ver <<
                            " opt: 0x" << setfill('0') << setw(2) << hex << (uint32_t)opt <<
                            " bigscn: " << bigscn <<
                            " compact: " << compat <<
                            " spare: " << setfill('0') << setw(8) << hex << spare <<
                            ", entries follow..." << endl;
                }

                if (fieldLength < 56 + entries * (uint32_t)8) {
                    oracleEnvironment->dumpStream << "ERROR: too short field KTB Redo F 0x11: " << dec << fieldLength << endl;
                    return;
                }

                for (uint32_t j = 0; j < entries; ++j) {
                    uint8_t itli = redoLogRecord->data[fieldPos + 56 + j * 8];
                    uint8_t flg = redoLogRecord->data[fieldPos + 57 + j * 8];
                    typescn scn = oracleEnvironment->readSCNr(redoLogRecord->data + fieldPos + 58 + j * 8);
                    if (oracleEnvironment->version < 12100)
                        oracleEnvironment->dumpStream << "  itli: " << dec <<     (uint32_t)itli << " " <<
                                " flg: " << (uint32_t)flg << " " <<
                                " scn: " << PRINTSCN48(scn) << endl;
                    else if (oracleEnvironment->version < 12200)
                        oracleEnvironment->dumpStream << "  itli: " << dec <<     (uint32_t)itli << " " <<
                                " flg: (opt=" << (uint32_t)(flg & 0x03) << " whr=" << (uint32_t)(flg >>2) << ") " <<
                                " scn: " << PRINTSCN48(scn) << endl;
                    else {
                        uint8_t opt = flg & 0x03;
                        uint8_t whr = flg >> 2;
                        oracleEnvironment->dumpStream << "  itli: " << dec <<     (uint32_t)itli << " " <<
                                " flg: (opt=" << (uint32_t)opt << " whr=" << (uint32_t)whr << ") " <<
                                " scn:  " << PRINTSCN64(scn) << endl;
                    }
                }
            }
        }
    }

    void OpCode::kdoOpCodeIRP(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 48) {
            oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode IRP: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->slot = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 42);
        redoLogRecord->cc = redoLogRecord->data[fieldPos + 18]; //column count
        redoLogRecord->nullsDelta = fieldPos + 45;

        if (fieldLength < 45 + ((uint32_t)redoLogRecord->cc + 7) / 8) {
            oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode IRP for nulls: " << dec << fieldLength <<
                    " (cc: " << redoLogRecord->cc << ")" << endl;
            return;
        }

        if (oracleEnvironment->dumpLogFile) {
            uint8_t tabn = redoLogRecord->data[fieldPos + 44];
            uint16_t sizeDelt = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 40);
            oracleEnvironment->dumpStream << "tabn: " << (uint32_t)tabn <<
                    " slot: " << dec << (uint32_t)redoLogRecord->slot << "(0x" << hex << redoLogRecord->slot << ")" <<
                    " size/delt: " << dec << sizeDelt << endl;

            uint8_t fl = redoLogRecord->data[fieldPos + 16];
            uint8_t lb = redoLogRecord->data[fieldPos + 17];
            char flStr[9] = "--------";

            if ((fl & 0x01) == 0x01) flStr[7] = 'N'; //last column continues in Next piece
            if ((fl & 0x02) == 0x02) flStr[6] = 'P'; //first column continues from Previous piece
            if ((fl & 0x04) == 0x04) flStr[5] = 'L'; //Last data piece
            if ((fl & 0x08) == 0x08) flStr[4] = 'F'; //First data piece
            if ((fl & 0x10) == 0x10) flStr[3] = 'D'; //Deleted row
            if ((fl & 0x20) == 0x20) flStr[2] = 'H'; //Head piece of row
            if ((fl & 0x40) == 0x40) flStr[1] = 'C'; //Clustered table member
            if ((fl & 0x80) == 0x80) flStr[0] = 'K'; //cluster Key

            oracleEnvironment->dumpStream << "fb: " << flStr <<
                    " lb: 0x" << hex << (uint32_t)lb << " " <<
                    " cc: " << dec << (uint32_t)redoLogRecord->cc;
            if (flStr[1] == 'C') {
                uint8_t cki = redoLogRecord->data[fieldPos + 19];
                oracleEnvironment->dumpStream << " cki: " << dec << (uint32_t)cki << endl;
            } else
                oracleEnvironment->dumpStream << endl;

            if (fl == 0x20) {
                uint32_t nrid1 = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 28);
                uint16_t nrid2 = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 32);
                oracleEnvironment->dumpStream << "nrid:  0x" << setfill('0') << setw(8) << hex << nrid1 << "." << hex << nrid2 << endl;
            }

            if (flStr[2] != 'H') {
                uint32_t hrid1 = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 20);
                uint16_t hrid2 = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 24);
                oracleEnvironment->dumpStream << "hrid: 0x" << setfill('0') << setw(8) << hex << hrid1 << "." << hex << hrid2 << endl;
            }

            if (flStr[0] == 'K') {
                uint8_t curc = 0; //FIXME
                uint8_t comc = 0; //FIXME
                uint32_t pk = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 20);
                uint8_t pk1 = redoLogRecord->data[fieldPos + 24];
                uint32_t nk = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 28);
                uint8_t nk1 = redoLogRecord->data[fieldPos + 32];

                oracleEnvironment->dumpStream << "curc: " << dec << (uint32_t)curc <<
                        " comc: " << dec << (uint32_t)comc <<
                        " pk: 0x" << setfill('0') << setw(8) << hex << pk << "." << hex << (uint32_t)pk1 <<
                        " nk: 0x" << setfill('0') << setw(8) << hex << nk << "." << hex << (uint32_t)nk1 << endl;
            }

            oracleEnvironment->dumpStream << "null:";
            if (redoLogRecord->cc >= 11)
                oracleEnvironment->dumpStream << endl << "01234567890123456789012345678901234567890123456789012345678901234567890123456789" << endl;
            else
                oracleEnvironment->dumpStream << " ";

            uint8_t *nulls = redoLogRecord->data + redoLogRecord->nullsDelta, bits = 1;
            for (uint32_t i = 0; i < redoLogRecord->cc; ++i) {

                if ((*nulls & bits) != 0)
                    oracleEnvironment->dumpStream << "N";
                else
                    oracleEnvironment->dumpStream << "-";

                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }
            oracleEnvironment->dumpStream << endl;
        }
    }

    void OpCode::kdoOpCodeDRP(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 20) {
            oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode DRP: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->slot = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 16);

        if (oracleEnvironment->dumpLogFile) {
            uint8_t tabn = redoLogRecord->data[fieldPos + 18];
            oracleEnvironment->dumpStream << "tabn: " << (uint32_t)tabn <<
                    " slot: " << dec << (uint32_t)redoLogRecord->slot << "(0x" << hex << redoLogRecord->slot << ")" << endl;
        }
    }

    void OpCode::kdoOpCodeLKR(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 20) {
            oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode LKR: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->slot = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 16);

        if (oracleEnvironment->dumpLogFile) {
            uint8_t tabn = redoLogRecord->data[fieldPos + 18];
            uint8_t to = redoLogRecord->data[fieldPos + 19];
            oracleEnvironment->dumpStream << "tabn: "<< (uint32_t)tabn <<
                " slot: " << dec << redoLogRecord->slot <<
                " to: " << dec << (uint32_t)to << endl;
        }
    }

    void OpCode::kdoOpCodeURP(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 28) {
            oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode URP: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->slot = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 20);
        redoLogRecord->nullsDelta = fieldPos + 26;

        if (fieldLength < 26 + ((uint32_t)redoLogRecord->cc + 7) / 8) {
            oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode IRP for nulls: " << dec <<
                    fieldLength << " (cc: " << redoLogRecord->cc << ")" << endl;
            return;
        }

        redoLogRecord->cc = redoLogRecord->data[fieldPos + 23]; //nnew field
        if (oracleEnvironment->dumpLogFile) {
            uint8_t flag = redoLogRecord->data[fieldPos + 16];
            uint8_t lock = redoLogRecord->data[fieldPos + 17];
            uint8_t ckix = redoLogRecord->data[fieldPos + 18];
            uint8_t tabn = redoLogRecord->data[fieldPos + 19];
            uint8_t ncol = redoLogRecord->data[fieldPos + 22];
            int16_t size = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 24); //signed

            oracleEnvironment->dumpStream << "tabn: "<< (uint32_t)tabn <<
                    " slot: " << dec << redoLogRecord->slot << "(0x" << hex << redoLogRecord->slot << ")" <<
                    " flag: 0x" << setfill('0') << setw(2) << hex << (uint32_t)flag <<
                    " lock: " << dec << (uint32_t)lock <<
                    " ckix: " << dec << (uint32_t)ckix << endl;
            oracleEnvironment->dumpStream << "ncol: " << dec << (uint32_t)ncol <<
                    " nnew: " << dec << (uint32_t)redoLogRecord->cc <<
                    " size: " << size << endl;
        }
    }

    void OpCode::kdoOpCodeSKL(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 20) {
            oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode SKL: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->slot = redoLogRecord->data[fieldPos + 27];

        if (oracleEnvironment->dumpLogFile) {
            uint8_t flagStr[3] = "--";
            uint8_t lock = redoLogRecord->data[fieldPos + 29];
            uint8_t flag = redoLogRecord->data[fieldPos + 28];
            if ((flag & 0x1) == 0x1) flagStr[0] = 'F';
            if ((flag & 0x2) == 0x2) flagStr[1] = 'B';

            oracleEnvironment->dumpStream << "flag: " << flagStr <<
                    " lock: " << dec << (uint32_t)lock <<
                    " slot: " << dec << redoLogRecord->slot << "(0x" << hex << redoLogRecord->slot << ")" << endl;

            if ((flag & 0x1) == 0x1) {
                uint8_t fwd[4];
                uint16_t fwd2 = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 20);
                memcpy(fwd, redoLogRecord->data + fieldPos + 16, 4);
                oracleEnvironment->dumpStream << "fwd: 0x" <<
                        setfill('0') << setw(2) << hex << (uint32_t)fwd[0] <<
                        setfill('0') << setw(2) << hex << (uint32_t)fwd[1] <<
                        setfill('0') << setw(2) << hex << (uint32_t)fwd[2] <<
                        setfill('0') << setw(2) << hex << (uint32_t)fwd[3] << "." <<
                        dec << fwd2 << " " << endl;
            }

            if ((flag & 0x2) == 0x2) {
                uint8_t bkw[4];
                uint16_t bkw2 = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 26);
                memcpy(bkw, redoLogRecord->data + fieldPos + 22, 4);
                oracleEnvironment->dumpStream << "bkw: 0x" <<
                        setfill('0') << setw(2) << hex << (uint32_t)bkw[0] <<
                        setfill('0') << setw(2) << hex << (uint32_t)bkw[1] <<
                        setfill('0') << setw(2) << hex << (uint32_t)bkw[2] <<
                        setfill('0') << setw(2) << hex << (uint32_t)bkw[3] << "." <<
                        dec << bkw2 << endl;
            }

        }
    }

    void OpCode::kdoOpCodeORP(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 48) {
            oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode ORP: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->slot = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 42);
        redoLogRecord->cc = redoLogRecord->data[fieldPos + 18]; //column count
        redoLogRecord->nullsDelta = fieldPos + 45;

        if (fieldLength < 45 + ((uint32_t)redoLogRecord->cc + 7) / 8) {
            oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode ORP for nulls: " << dec << fieldLength <<
                    " (cc: " << redoLogRecord->cc << ")" << endl;
            return;
        }

        if (oracleEnvironment->dumpLogFile) {
            uint8_t tabn = redoLogRecord->data[fieldPos + 44];
            uint16_t sizeDelt = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 40);
            oracleEnvironment->dumpStream << "tabn: "<< (uint32_t)tabn <<
                " slot: " << dec << (uint32_t)redoLogRecord->slot << "(0x" << hex << (uint32_t)redoLogRecord->slot << ")" <<
                " size/delt: " << dec << sizeDelt << endl;

            uint8_t fl = redoLogRecord->data[fieldPos + 16];
            uint8_t lb = redoLogRecord->data[fieldPos + 17];
            char flStr[9] = "--------";

            if ((fl & 0x01) == 0x01) flStr[7] = 'N'; //last column continues in Next piece
            if ((fl & 0x02) == 0x02) flStr[6] = 'P'; //first column continues from Previous piece
            if ((fl & 0x04) == 0x04) flStr[5] = 'L'; //Last data piece
            if ((fl & 0x08) == 0x08) flStr[4] = 'F'; //First data piece
            if ((fl & 0x10) == 0x10) flStr[3] = 'D'; //Deleted row
            if ((fl & 0x20) == 0x20) flStr[2] = 'H'; //Head piece of row
            if ((fl & 0x40) == 0x40) flStr[1] = 'C'; //Clustered table member
            if ((fl & 0x80) == 0x80) flStr[0] = 'K'; //cluster Key

            oracleEnvironment->dumpStream << "fb: " << flStr <<
                    " lb: 0x" << hex << (uint32_t) lb << " " <<
                    " cc: " << dec << (uint32_t) redoLogRecord->cc;
            if (flStr[1] == 'C') {
                uint8_t cki = redoLogRecord->data[fieldPos + 19];
                oracleEnvironment->dumpStream << " cki: " << dec << (uint32_t)cki << endl;
            } else
                oracleEnvironment->dumpStream << endl;

            uint32_t nrid1 = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 28);
            uint8_t nrid2 = redoLogRecord->data[fieldPos + 32];
            if (fl == 0x20)
                oracleEnvironment->dumpStream << "nrid:  0x" << setfill('0') << setw(8) << hex << nrid1 << "." << hex << (uint32_t) nrid2 << endl;

            oracleEnvironment->dumpStream << "null:";
            if (redoLogRecord->cc >= 11)
                oracleEnvironment->dumpStream << endl << "01234567890123456789012345678901234567890123456789012345678901234567890123456789" << endl;
            else
                oracleEnvironment->dumpStream << " ";

            uint8_t *nulls = redoLogRecord->data + redoLogRecord->nullsDelta, bits = 1;
            for (uint32_t i = 0; i < redoLogRecord->cc; ++i) {

                if ((*nulls & bits) != 0)
                    oracleEnvironment->dumpStream << "N";
                else
                    oracleEnvironment->dumpStream << "-";

                bits <<= 1;
                if (bits == 0) {
                    bits = 1;
                    ++nulls;
                }
            }
            oracleEnvironment->dumpStream << endl;
        }
    }

    void OpCode::kdoOpCodeQM(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 24) {
            oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode QMI (1): " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->slotsDelta = fieldPos + 20;
        redoLogRecord->nrow = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 18);

        if (oracleEnvironment->dumpLogFile) {
            uint8_t tabn = redoLogRecord->data[fieldPos + 16];
            uint8_t lock = redoLogRecord->data[fieldPos + 17];

            oracleEnvironment->dumpStream << "tabn: "<< (uint32_t)tabn <<
                " lock: " << dec << (uint32_t)lock <<
                " nrow: " << dec << redoLogRecord->nrow << endl;

            if (fieldLength < 22 + (uint32_t)redoLogRecord->nrow * 2) {
                oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode QMI (2): " << dec << fieldLength << ", " <<
                        redoLogRecord->nrow << endl;
                return;
            }
        }
    }

    void OpCode::kdoOpCode(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 16) {
            oracleEnvironment->dumpStream << "ERROR: too short field KDO OpCode: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->itli = redoLogRecord->data[fieldPos + 12];
        redoLogRecord->op = redoLogRecord->data[fieldPos + 10];
        redoLogRecord->bdba = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->xtype = redoLogRecord->data[fieldPos + 11];

        if (oracleEnvironment->dumpLogFile) {
            uint32_t hdba = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 4);
            uint16_t maxFr = oracleEnvironment->read16(redoLogRecord->data + fieldPos + 8);
            uint32_t flags = 0;
            uint8_t ispac = redoLogRecord->data[fieldPos + 13];

            const char* opCode = "???";
            switch (redoLogRecord->op & 0x1F) {
                case 0x01: opCode = "IUR"; break; //Interpret Undo Redo
                case 0x02: opCode = "IRP"; break; //Insert Row Piece
                case 0x03: opCode = "DRP"; break; //Delete Row Piece
                case 0x04: opCode = "LKR"; break; //LocK Row
                case 0x05: opCode = "URP"; break; //Update Row Piece
                case 0x06: opCode = "ORP"; break; //Overwrite Row Piece
                case 0x07: opCode = "MFC"; break; //Manipulate First Column
                case 0x08: opCode = "CFA"; break; //Change Forwarding Address
                case 0x09: opCode = "CKI"; break; //Change Cluster key Index
                case 0x0A: opCode = "SKL"; break; //Set Key Links
                case 0x0B: opCode = "QMI"; break; //Quick Multi-row Insert
                case 0x0C: opCode = "QMD"; break; //Quick Multi-row Delete
                case 0x0D: opCode = "TBF"; break;
                case 0x0E: opCode = "DSC"; break;
                case 0x10: opCode = "LMN"; break;
                case 0x11: opCode = "LLB"; break;
                case 0x13: opCode = " 21"; break;
                default:
                    if (oracleEnvironment->dumpLogFile)
                        oracleEnvironment->dumpStream << "DEBUG op: " << dec << (uint32_t)(redoLogRecord->op & 0x1F) << endl;
            }

            string xtypeStr;
            if (redoLogRecord->xtype == 0x01) xtypeStr = "XA"; //redo
            else if (redoLogRecord->xtype == 0x81) {
                xtypeStr = "XAxtype KDO_KDOM2"; //redo
                flags |= 0x80;
            } else if (redoLogRecord->xtype == 0x02) xtypeStr = "XR"; //rollback
            else if (redoLogRecord->xtype == 0x03) xtypeStr = "CR"; //unknown
            else {
                if (oracleEnvironment->dumpLogFile)
                    oracleEnvironment->dumpStream << "DEBUG xtype: " << dec << redoLogRecord->xtype << endl;
                xtypeStr = "??";
            }

            oracleEnvironment->dumpStream << "KDO Op code: " << opCode << " row dependencies Disabled" << endl;
            oracleEnvironment->dumpStream << "  xtype: " << xtypeStr <<
                    " flags: 0x" << setfill('0') << setw(8) << hex << flags << " " <<
                    " bdba: 0x" << setfill('0') << setw(8) << hex << redoLogRecord->bdba << " " <<
                    " hdba: 0x" << setfill('0') << setw(8) << hex << hdba << endl;
            oracleEnvironment->dumpStream << "itli: " << dec << (uint32_t)redoLogRecord->itli << " " <<
                    " ispac: " << dec << (uint32_t)ispac << " " <<
                    " maxfr: " << dec << (uint32_t)maxFr << endl;
        }

        switch (redoLogRecord->op & 0x1F) {
        case 0x02:
            kdoOpCodeIRP(fieldPos, fieldLength);
            break;

        case 0x03:
            kdoOpCodeDRP(fieldPos, fieldLength);
            break;

        case 0x04:
            kdoOpCodeLKR(fieldPos, fieldLength);
            break;

        case 0x05:
            kdoOpCodeURP(fieldPos, fieldLength);
            break;

        case 0x06:
            kdoOpCodeORP(fieldPos, fieldLength);
            break;

        case 0x0A:
            kdoOpCodeSKL(fieldPos, fieldLength);
            break;

        case 0x0B:
        case 0x0C:
            kdoOpCodeQM(fieldPos, fieldLength);
            break;
        }
    }

    void OpCode::ktub(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 24) {
            oracleEnvironment->dumpStream << "ERROR: too short field ktub: " << dec << fieldLength << endl;
            return;
        }

        redoLogRecord->objn = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 0);
        redoLogRecord->objd = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 4);
        redoLogRecord->tsn = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 8);
        redoLogRecord->undo = oracleEnvironment->read32(redoLogRecord->data + fieldPos + 12);
        redoLogRecord->slt = redoLogRecord->data[fieldPos + 18];
        redoLogRecord->rci = redoLogRecord->data[fieldPos + 19];
        redoLogRecord->flg = redoLogRecord->data[fieldPos + 20];
        redoLogRecord->opc = (((uint16_t)redoLogRecord->data[fieldPos + 16]) << 8) |
                redoLogRecord->data[fieldPos + 17];

        if (oracleEnvironment->dumpLogFile) {
            //uint8_t isKtUbl = redoLogRecord->data[fieldPos + 20];
        }
    }


    void OpCode::ktubu(uint32_t fieldPos, uint32_t fieldLength) {
        if (fieldLength < 24) {
            oracleEnvironment->dumpStream << "ERROR: too short field ktubu.B.1: " << dec << fieldLength << endl;
            return;
        }

        if (oracleEnvironment->dumpLogFile) {
            string lastBufferSplit = "No "; //FIXME
            string tablespaceUndo = "No "; //FIXME

            oracleEnvironment->dumpStream << "ktubu redo:" <<
                    " slt: " << dec << (uint32_t)redoLogRecord->slt <<
                    " rci: " << dec << (uint32_t)redoLogRecord->rci <<
                    " opc: " << dec << (uint32_t)(redoLogRecord->opc >> 8) << "." << (uint32_t)(redoLogRecord->opc & 0xFF) <<
                    " objn: " << redoLogRecord->objn << " objd: " << redoLogRecord->objd << " tsn: " << redoLogRecord->tsn << endl;
            oracleEnvironment->dumpStream << "Undo type:  Regular undo       Undo type:  " << getUndoType() << "Last buffer split:  " << lastBufferSplit << endl;
            oracleEnvironment->dumpStream << "Tablespace Undo:  " << tablespaceUndo << endl;
            oracleEnvironment->dumpStream << "             0x" << hex << setfill('0') << setw(8) << redoLogRecord->undo << endl;
        }
    }

    const char* OpCode::getUndoType() {
        return "";
    }

    bool OpCode::isKdoUndo() {
        return false;
    }

    void OpCode::dumpColsVector(uint8_t *data, uint16_t colnum, uint16_t fieldLength) {
        uint32_t pos = 0;

        if (oracleEnvironment->dumpLogFile)
            oracleEnvironment->dumpStream << "Vector content: " << endl;

        for (uint32_t k = 0; k < redoLogRecord->cc; ++k) {
            uint16_t fieldLength = data[pos];
            ++pos;
            uint8_t isNull = (fieldLength == 0xFF);

            if (fieldLength == 0xFE) {
                fieldLength = oracleEnvironment->read16(data + pos);
                pos += 2;
            }

            dumpCols(data + pos, colnum + k, fieldLength, isNull);

            if (!isNull)
                pos += fieldLength;
        }
    }

    void OpCode::dumpCols(uint8_t *data, uint16_t colnum, uint16_t fieldLength, uint8_t isNull) {
        if (isNull) {
            oracleEnvironment->dumpStream << "col " << setfill(' ') << setw(2) << dec << colnum << ": *NULL*" << endl;
        } else {
            oracleEnvironment->dumpStream << "col " << setfill(' ') << setw(2) << dec << colnum << ": " <<
                    "[" << setfill(' ') << setw(2) << dec << fieldLength << "]";

            if (fieldLength <= 20)
                oracleEnvironment->dumpStream << " ";
            else
                oracleEnvironment->dumpStream << endl;

            for (uint32_t j = 0; j < fieldLength; ++j) {
                oracleEnvironment->dumpStream << " " << setfill('0') << setw(2) << hex << (uint32_t)data[j];
                if ((j % 25) == 24 && j != (uint32_t)fieldLength - 1)
                    oracleEnvironment->dumpStream << endl;
            }

            oracleEnvironment->dumpStream << endl;
        }
    }
}
