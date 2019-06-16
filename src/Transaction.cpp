/* Transaction from Oracle database
   Copyright (C) 2018-2019 Adam Leszczynski.

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
#include <string>
#include "types.h"
#include "CommandBuffer.h"
#include "OracleEnvironment.h"
#include "Transaction.h"
#include "TransactionBuffer.h"
#include "TransactionChunk.h"
#include "RedoLogRecord.h"
#include "OpCode.h"
#include "OpCode0501.h"
#include "OpCode0502.h"
#include "OpCode0504.h"
#include "OpCode0B02.h"
#include "OpCode0B03.h"
#include "OpCode0B05.h"
#include "OpCode0B0B.h"
#include "OpCode0B0C.h"
#include "OpCode1801.h"

using namespace std;

namespace OpenLogReplicatorOracle {

    bool Transaction::operator< (Transaction &p) {
        if (isCommit && !p.isCommit)
            return true;
        if (!isCommit && p.isCommit)
            return false;

        bool ret = lastScn < p.lastScn;
        if (ret != false)
            return ret;
        return lastScn == p.lastScn && xid < p.xid;
    }

    void Transaction::touch(typescn scn) {
        if (firstScn == ZERO_SCN || firstScn > scn)
            firstScn = scn;
        if (lastScn == ZERO_SCN || lastScn < scn)
            lastScn = scn;
    }

    void Transaction::add(uint32_t objn, uint32_t objd, typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci, RedoLogRecord *redoLogRecord1, RedoLogRecord *redoLogRecord2,
            TransactionBuffer *transactionBuffer) {
        tcLast = transactionBuffer->addTransactionChunk(tcLast, objn, objd, uba, dba, slt, rci, redoLogRecord1, redoLogRecord2);
        ++opCodes;
        touch(redoLogRecord1->scn);
    }

    bool Transaction::rollbackPreviousOp(typescn scn, TransactionBuffer *transactionBuffer, typeuba uba, uint32_t dba, uint8_t slt, uint8_t rci) {
        if (transactionBuffer->deleteTransactionPart(tcLast, uba, dba, slt, rci)) {
            --opCodes;
            if (lastScn == ZERO_SCN || lastScn < scn)
                lastScn = scn;
            return true;
        } else
            return false;
    }

    void Transaction::rollbackLastOp(typescn scn, TransactionBuffer *transactionBuffer) {
        tcLast = transactionBuffer->rollbackTransactionChunk(tcLast, lastUba, lastDba, lastSlt, lastRci);

        --opCodes;
        if (lastScn == ZERO_SCN || lastScn < scn)
            lastScn = scn;
    }

    void Transaction::flush(OracleEnvironment *oracleEnvironment) {
        TransactionChunk *tcTemp = tc;
        bool hasPrev = false;

        //transaction that has some DML's

        if (opCodes > 0 && !isRollback) {
            if (oracleEnvironment->trace >= 1) {
                cout << "Transaction xid:  " << PRINTXID(xid) <<
                        " SCN: " << PRINTSCN(firstScn) <<
                        " - " << PRINTSCN(lastScn) <<
                        " opCodes: " << dec << opCodes <<  endl;
            }

            if (oracleEnvironment->commandBuffer->posEnd >= INTRA_THREAD_BUFFER_SIZE - MAX_TRANSACTION_SIZE)
                oracleEnvironment->commandBuffer->rewind();

            switch (oracleEnvironment->commandBuffer->type) {
            case COMMAND_BUFFER_JSON:
                oracleEnvironment->commandBuffer
                        ->beginTran()
                        ->append("{\"scn\": \"")
                        ->append(to_string(lastScn))
                        ->append("\", dml: [");
                break;
            case COMMAND_BUFFER_REDIS:
                oracleEnvironment->commandBuffer
                        ->beginTran();
                break;
            }

            while (tcTemp != nullptr) {
                uint32_t pos = 0;
                typescn oldScn = 0;
                for (uint32_t i = 0; i < tcTemp->elements; ++i) {
                    uint32_t op = *((uint32_t*)(tcTemp->buffer + pos + 8));

                    RedoLogRecord *redoLogRecord1 = ((RedoLogRecord *)(tcTemp->buffer + pos + 12));
                    RedoLogRecord *redoLogRecord2 = ((RedoLogRecord *)(tcTemp->buffer + pos + 12 + sizeof(struct RedoLogRecord)));
                    typescn scn = *((typescn *)(tcTemp->buffer + pos + 20 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) +
                            redoLogRecord1->length + redoLogRecord2->length));
                    redoLogRecord1->data = tcTemp->buffer + pos + 12 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord);
                    redoLogRecord2->data = tcTemp->buffer + pos + 12 + sizeof(struct RedoLogRecord) + sizeof(struct RedoLogRecord) + redoLogRecord1->length;

                    if (oracleEnvironment->trace >= 1) {
                        uint32_t objn = *((uint32_t*)(tcTemp->buffer + pos));
                        uint32_t objd = *((uint32_t*)(tcTemp->buffer + pos + 4));
                        cout << "Row: " << dec << redoLogRecord1->length << ":" << redoLogRecord2->length <<
                                " op: " << setfill('0') << setw(8) << hex << op <<
                                " objn: " << dec << objn <<
                                " objd: " << dec << objd <<
                                " scn: " << PRINTSCN(scn) << endl;
                        if (oldScn != 0 && oldScn > scn)
                            cerr << "ERROR: SCN swap" << endl;
                    }
                    pos += redoLogRecord1->length + redoLogRecord2->length + ROW_HEADER_MEMORY;

                    switch (op) {
                    //insert row piece
                    case 0x05010B02:
                        {
                            if (hasPrev) {
                                switch (oracleEnvironment->commandBuffer->type) {
                                case COMMAND_BUFFER_JSON:
                                    oracleEnvironment->commandBuffer->append(", ");
                                    break;
                                }
                            }
                            OpCode0B02 *opCode0B02 = new OpCode0B02(oracleEnvironment, redoLogRecord2);
                            opCode0B02->parseInsert(redoLogRecord1->objn, redoLogRecord1->objd);
                            hasPrev = true;
                            delete opCode0B02;
                        }
                        break;

                        //update row piece
                        case 0x05010B05:
                            {
                                if (hasPrev) {
                                    switch (oracleEnvironment->commandBuffer->type) {
                                    case COMMAND_BUFFER_JSON:
                                        oracleEnvironment->commandBuffer->append(", ");
                                        break;
                                    }
                                }
                                OpCode0501 *opCode0501 = new OpCode0501(oracleEnvironment, redoLogRecord1);
                                OpCode0B05 *opCode0B05 = new OpCode0B05(oracleEnvironment, redoLogRecord2);
                                opCode0B05->parseUpdate(redoLogRecord1->objn, redoLogRecord1->objd, opCode0501);
                                hasPrev = true;
                                delete opCode0501;
                                delete opCode0B05;
                            }
                            break;

                    //insert multiple rows
                    case 0x05010B0B:
                        {
                            if (hasPrev) {
                                switch (oracleEnvironment->commandBuffer->type) {
                                case COMMAND_BUFFER_JSON:
                                    oracleEnvironment->commandBuffer->append(", ");
                                    break;
                                }
                            }
                            OpCode0B0B *opCode0B0B = new OpCode0B0B(oracleEnvironment, redoLogRecord2);
                            opCode0B0B->parseInsert(redoLogRecord1->objn, redoLogRecord1->objd);
                            hasPrev = true;
                            delete opCode0B0B;
                        }
                        break;

                    //delete row piece
                    case 0x05010B03:
                        {
                            if (hasPrev) {
                                switch (oracleEnvironment->commandBuffer->type) {
                                case COMMAND_BUFFER_JSON:
                                    oracleEnvironment->commandBuffer->append(", ");
                                    break;
                                }
                            }
                            OpCode0501 *opCode0501 = new OpCode0501(oracleEnvironment, redoLogRecord1);
                            opCode0501->parseDelete(redoLogRecord2->afn);
                            hasPrev = true;
                            delete opCode0501;
                        }
                        break;

                    //truncate table
                    case 0x18010000:
                        {
                            if (hasPrev) {
                                switch (oracleEnvironment->commandBuffer->type) {
                                case COMMAND_BUFFER_JSON:
                                    oracleEnvironment->commandBuffer->append(", ");
                                    break;
                                }
                            }
                            OpCode1801 *opCode1801 = new OpCode1801(oracleEnvironment, redoLogRecord1);
                            opCode1801->parseDDL();
                            hasPrev = true;
                            delete opCode1801;
                        }
                        break;

                    default:
                        cerr << "ERROR: Unknown OpCode " << hex << op << endl;
                    }

                    if (oracleEnvironment->commandBuffer->currentTranSize() >= MAX_TRANSACTION_SIZE) {
                        cerr << "WARNING: Big transaction divided (" << oracleEnvironment->commandBuffer->currentTranSize() << ")" << endl;
                        switch (oracleEnvironment->commandBuffer->type) {
                        case COMMAND_BUFFER_JSON:
                            oracleEnvironment->commandBuffer
                                    ->append("]}")
                                    ->commitTran();
                            break;
                        case COMMAND_BUFFER_REDIS:
                            oracleEnvironment->commandBuffer
                                    ->append("EXEC\n")
                                    ->commitTran();
                            break;
                        }

                        if (oracleEnvironment->commandBuffer->posEnd >= INTRA_THREAD_BUFFER_SIZE - MAX_TRANSACTION_SIZE)
                            oracleEnvironment->commandBuffer->rewind();

                        switch (oracleEnvironment->commandBuffer->type) {
                        case COMMAND_BUFFER_JSON:
                            oracleEnvironment->commandBuffer
                                    ->beginTran()
                                    ->append("{\"scn\": \"")
                                    ->append(to_string(lastScn))
                                    ->append("\", dml: [");
                            break;
                        case COMMAND_BUFFER_REDIS:
                            oracleEnvironment->commandBuffer
                                    ->beginTran();
                            break;
                        }

                    }

                    oldScn = scn;
                }
                tcTemp = tcTemp->next;
            }

            switch (oracleEnvironment->commandBuffer->type) {
            case COMMAND_BUFFER_JSON:
                oracleEnvironment->commandBuffer
                        ->append("]}")
                        ->commitTran();
                break;
            case COMMAND_BUFFER_REDIS:
                oracleEnvironment->commandBuffer
                        ->commitTran();
                break;
            }
        }

        oracleEnvironment->transactionBuffer.deleteTransactionChunks(tc, tcLast);
    }

    Transaction::Transaction(typexid xid, TransactionBuffer *transactionBuffer) :
            xid(xid),
            firstScn(ZERO_SCN),
            lastScn(ZERO_SCN),
            opCodes(0),
            pos(0),
            lastUba(0),
            lastDba(0),
            lastSlt(0),
            lastRci(0),
            isBegin(false),
            isCommit(false),
            isRollback(false),
            next(nullptr) {
        tc = transactionBuffer->newTransactionChunk();
        tcLast = tc;
    }

    Transaction::~Transaction() {
    }

    ostream& operator<<(ostream& os, const Transaction& tran) {
        os << "xid: " << PRINTXID(tran.xid) <<
                " scn: " << PRINTSCN(tran.firstScn) << " - " << PRINTSCN(tran.lastScn) <<
                " begin: " << tran.isBegin <<
                " commit: " << tran.isCommit <<
                " rollback: " << tran.isRollback;
        return os;
    }
}

