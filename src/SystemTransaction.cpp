/* System transaction to change schema
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "OracleAnalyzer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OutputBuffer.h"
#include "RuntimeException.h"
#include "Schema.h"
#include "SystemTransaction.h"

using namespace std;

namespace OpenLogReplicator {

    SystemTransaction::SystemTransaction(OracleAnalyzer *oracleAnalyzer, OutputBuffer *outputBuffer) :
                oracleAnalyzer(oracleAnalyzer),
                outputBuffer(outputBuffer) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: begin");
    }

    SystemTransaction::~SystemTransaction() {
    }

    void SystemTransaction::updateNumber16(int16_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr && outputBuffer->values[pos][VALUE_AFTER].length[0] > 0) {
            char *retPtr;
            if (object->columns[i]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " type found " << object->columns[i]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[pos][VALUE_AFTER].data[0], outputBuffer->values[pos][VALUE_AFTER].length[0]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            val = strtol(outputBuffer->valueBuffer, &retPtr, 10);
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << ")");
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            val = 0;
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": NULL)");
        }
    }

    void SystemTransaction::updateNumber16u(uint16_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr && outputBuffer->values[pos][VALUE_AFTER].length[0] > 0) {
            char *retPtr;
            if (object->columns[i]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " type found " << object->columns[i]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[pos][VALUE_AFTER].data[0], outputBuffer->values[pos][VALUE_AFTER].length[0]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0 || (outputBuffer->valueLength > 0 && outputBuffer->valueBuffer[0] == '-')) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " value found " << outputBuffer->valueBuffer);
            }
            val = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << ")");
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            val = 0;
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": NULL)");
        }
    }

    void SystemTransaction::updateNumber32u(uint32_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr && outputBuffer->values[pos][VALUE_AFTER].length[0] > 0) {
            char *retPtr;
            if (object->columns[i]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " type found " << object->columns[i]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[pos][VALUE_AFTER].data[0], outputBuffer->values[pos][VALUE_AFTER].length[0]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0 || (outputBuffer->valueLength > 0 && outputBuffer->valueBuffer[0] == '-')) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " value found " << outputBuffer->valueBuffer);
            }
            val = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << ")");
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            val = 0;
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": NULL)");
        }
    }

    void SystemTransaction::updateNumber64(int64_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr && outputBuffer->values[pos][VALUE_AFTER].length[0] > 0) {
            char *retPtr;
            if (object->columns[i]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " type found " << object->columns[i]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[pos][VALUE_AFTER].data[0], outputBuffer->values[pos][VALUE_AFTER].length[0]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " value found " << outputBuffer->valueBuffer);
            }
            val = strtol(outputBuffer->valueBuffer, &retPtr, 10);
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << ")");
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            val = 0;
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": NULL)");
        }
    }

    void SystemTransaction::updateNumber64u(uint64_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr && outputBuffer->values[pos][VALUE_AFTER].length[0] > 0) {
            char *retPtr;
            if (object->columns[i]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " type found " << object->columns[i]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[pos][VALUE_AFTER].data[0], outputBuffer->values[pos][VALUE_AFTER].length[0]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0 || (outputBuffer->valueLength > 0 && outputBuffer->valueBuffer[0] == '-')) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " value found " << outputBuffer->valueBuffer);
            }
            val = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << ")");
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            val = 0;
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": NULL)");
        }
    }

    void SystemTransaction::updateNumberXu(uintX_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr && outputBuffer->values[pos][VALUE_AFTER].length[0] > 0) {
            char *retPtr;
            if (object->columns[i]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " type found " << object->columns[i]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[pos][VALUE_AFTER].data[0], outputBuffer->values[pos][VALUE_AFTER].length[0]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0 || (outputBuffer->valueLength > 0 && outputBuffer->valueBuffer[0] == '-')) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " value found " << outputBuffer->valueBuffer);
            }
            val.setStr(outputBuffer->valueBuffer, outputBuffer->valueLength);
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << ")");
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            val.set(0, 0);
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": NULL)");
        }
    }

    void SystemTransaction::updateString(string &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr && outputBuffer->values[pos][VALUE_AFTER].length[0] > 0) {
            char *retPtr;
            if (object->columns[i]->typeNo != 1 && object->columns[i]->typeNo != 96) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " type found " << object->columns[i]->typeNo);
            }
            outputBuffer->parseString(outputBuffer->values[pos][VALUE_AFTER].data[0], outputBuffer->values[pos][VALUE_AFTER].length[0], object->columns[i]->charsetId);
            val.assign(outputBuffer->valueBuffer, outputBuffer->valueLength);
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": '" << val << "')");
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            val.assign("");
            TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": '')");
        }
    }

    void SystemTransaction::processInsert(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        RowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);

        TRACE(TRACE2_SYSTEM, "SYSTEM: insert table (name: " << object->owner << "." << object->name << ", rowid: " << rowId << ")");

        if (object->systemTable == TABLE_SYS_CCOL) {
            if (oracleAnalyzer->schema->sysCColMapRowId.find(rowId) != oracleAnalyzer->schema->sysCColMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.CCOL$: (rowid: " << rowId << ") for insert");
            }
            SysCCol *sysCCol = new SysCCol(rowId, 0, 0, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("CON#") == 0)
                    updateNumber32u(sysCCol->con, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("INTCOL#") == 0)
                    updateNumber16(sysCCol->intCol, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysCCol->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SPARE1") == 0)
                    updateNumberXu(sysCCol->spare1, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysCColMapRowId[rowId] = sysCCol;

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            if (oracleAnalyzer->schema->sysCDefMapRowId.find(rowId) != oracleAnalyzer->schema->sysCDefMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEF$: (rowid: " << rowId << ") for insert");
            }
            SysCDef *sysCDef = new SysCDef(rowId, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("CON#") == 0)
                    updateNumber32u(sysCDef->con, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysCDef->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TYPE#") == 0)
                    updateNumber16u(sysCDef->type, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysCDefMapRowId[rowId] = sysCDef;

        } else if (object->systemTable == TABLE_SYS_COL) {
            if (oracleAnalyzer->schema->sysColMapRowId.find(rowId) != oracleAnalyzer->schema->sysColMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.COL$: (rowid: " << rowId << ")for insert");
            }
            SysCol *sysCol = new SysCol(rowId, 0, 0, 0, 0, "", 0, 0, -1, -1, 0, 0, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysCol->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("COL#") == 0)
                    updateNumber16(sysCol->col, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SEGCOL#") == 0)
                    updateNumber16(sysCol->segCol, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("INTCOL#") == 0)
                    updateNumber16(sysCol->intCol, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("NAME") == 0)
                    updateString(sysCol->name, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TYPE#") == 0)
                    updateNumber16u(sysCol->type, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("LENGTH") == 0)
                    updateNumber64u(sysCol->length, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("PRECISION#") == 0)
                    updateNumber64(sysCol->precision, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SCALE") == 0)
                    updateNumber64(sysCol->scale, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("CHARSETFORM") == 0)
                    updateNumber64u(sysCol->charsetForm, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("CHARSETID") == 0)
                    updateNumber64u(sysCol->charsetId, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("NULL$") == 0)
                    updateNumber64(sysCol->null_, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("PROPERTY") == 0)
                    updateNumberXu(sysCol->property, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysColMapRowId[rowId] = sysCol;

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            if (oracleAnalyzer->schema->sysDeferredStgMapRowId.find(rowId) != oracleAnalyzer->schema->sysDeferredStgMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEFERRED_STG$: (rowid: " << rowId << ") for insert");
            }
            SysDeferredStg *sysDeferredStg = new SysDeferredStg(rowId, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateNumber32u(sysDeferredStg->obj, i, pos, object, rowId);
                } else if (object->columns[i]->name.compare("FLAGS_STG") == 0)
                    updateNumberXu(sysDeferredStg->flagsStg, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysDeferredStgMapRowId[rowId] = sysDeferredStg;

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            if (oracleAnalyzer->schema->sysEColMapRowId.find(rowId) != oracleAnalyzer->schema->sysEColMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.ECOL$: (rowid: " << rowId << ") for insert");
            }
            SysECol *sysECol = new SysECol(rowId, 0, 0, -1, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("TABOBJ#") == 0)
                    updateNumber32u(sysECol->tabObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("COLNUM") == 0)
                    updateNumber16(sysECol->colNum, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("GUARD_ID") == 0)
                    updateNumber32u(sysECol->guardId, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysEColMapRowId[rowId] = sysECol;

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            SysObj *sysObj = oracleAnalyzer->schema->sysObjMapRowId[rowId];
            if (sysObj != nullptr) {
                RUNTIME_FAIL("DDL: duplicate SYS.OBJ$: (rowid: " << rowId << ") for insert");
            }
            sysObj = new SysObj(rowId, 0, 0, 0, 0, "", 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OWNER#") == 0)
                    updateNumber32u(sysObj->owner, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysObj->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysObj->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("NAME") == 0)
                    updateString(sysObj->name, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TYPE#") == 0)
                    updateNumber16u(sysObj->type, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("FLAGS") == 0)
                    updateNumberXu(sysObj->flags, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysObjMapRowId[rowId] = sysObj;

        } else if (object->systemTable == TABLE_SYS_SEG) {
            if (oracleAnalyzer->schema->sysSegMapRowId.find(rowId) != oracleAnalyzer->schema->sysSegMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.SEG$: (rowid: " << rowId << ") for insert");
            }
            SysSeg *sysSeg = new SysSeg(rowId, 0, 0, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("FILE#") == 0)
                    updateNumber32u(sysSeg->file, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("BLOCK#") == 0)
                    updateNumber32u(sysSeg->block, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TS#") == 0)
                    updateNumber32u(sysSeg->ts, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SPARE1") == 0)
                    updateNumberXu(sysSeg->spare1, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysSegMapRowId[rowId] = sysSeg;

        } else if (object->systemTable == TABLE_SYS_TAB) {
            if (oracleAnalyzer->schema->sysTabMapRowId.find(rowId) != oracleAnalyzer->schema->sysTabMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TAB$: (rowid: " << rowId << ") for insert");
            }
            SysTab *sysTab = new SysTab(rowId, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysTab->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTab->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TS#") == 0)
                    updateNumber32u(sysTab->ts, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("FILE#") == 0)
                    updateNumber32u(sysTab->file, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("BLOCK#") == 0)
                    updateNumber32u(sysTab->block, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("CLUCOLS") == 0)
                    updateNumber16(sysTab->cluCols, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("FLAGS") == 0)
                    updateNumberXu(sysTab->flags, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("PROPERTY") == 0)
                    updateNumberXu(sysTab->property, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysTabMapRowId[rowId] = sysTab;

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            if (oracleAnalyzer->schema->sysTabComPartMapRowId.find(rowId) != oracleAnalyzer->schema->sysTabComPartMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABCOMPART$: (rowid: " << rowId << ") for insert");
            }
            SysTabComPart *sysTabComPart = new SysTabComPart(rowId, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysTabComPart->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTabComPart->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("BO#") == 0)
                    updateNumber32u(sysTabComPart->bo, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysTabComPartMapRowId[rowId] = sysTabComPart;

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            if (oracleAnalyzer->schema->sysTabPartMapRowId.find(rowId) != oracleAnalyzer->schema->sysTabPartMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABPART$: (rowid: " << rowId << ") for insert");
            }
            SysTabPart *sysTabPart = new SysTabPart(rowId, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysTabPart->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTabPart->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("BO#") == 0)
                    updateNumber32u(sysTabPart->bo, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysTabPartMapRowId[rowId] = sysTabPart;

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            if (oracleAnalyzer->schema->sysTabSubPartMapRowId.find(rowId) != oracleAnalyzer->schema->sysTabSubPartMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABSUBPART$: (rowid: " << rowId << ") for insert");
            }
            SysTabSubPart *sysTabSubPart = new SysTabSubPart(rowId, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysTabSubPart->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTabSubPart->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("POBJ#") == 0)
                    updateNumber32u(sysTabSubPart->pObj, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysTabSubPartMapRowId[rowId] = sysTabSubPart;

        } else if (object->systemTable == TABLE_SYS_USER) {
            if (oracleAnalyzer->schema->sysUserMapRowId.find(rowId) != oracleAnalyzer->schema->sysUserMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.USER$: (rowid: " << rowId << ") for insert");
            }
            SysUser *sysUser = new SysUser(rowId, 0, "", 0, 0, false, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("USER#") == 0)
                    updateNumber32u(sysUser->user, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("NAME") == 0)
                    updateString(sysUser->name, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SPARE1") == 0)
                    updateNumberXu(sysUser->spare1, i, pos, object, rowId);
            }

            oracleAnalyzer->schema->sysUserMapRowId[rowId] = sysUser;
        }
    }

    void SystemTransaction::processUpdate(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        RowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        TRACE(TRACE2_SYSTEM, "SYSTEM: update table (name: " << object->owner << "." << object->name << ", rowid: " << rowId << ")");

        if (object->systemTable == TABLE_SYS_CCOL) {
            auto sysCColIt = oracleAnalyzer->schema->sysCColMapRowId.find(rowId);
            if (sysCColIt == oracleAnalyzer->schema->sysCColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCCol *sysCCol = sysCColIt->second;
            sysCCol->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("CON#") == 0)
                    updateNumber32u(sysCCol->con, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("INTCOL#") == 0)
                    updateNumber16(sysCCol->intCol, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysCCol->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SPARE1") == 0)
                    updateNumberXu(sysCCol->spare1, i, pos, object, rowId);
            }

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            auto sysCDefIt = oracleAnalyzer->schema->sysCDefMapRowId.find(rowId);
            if (sysCDefIt == oracleAnalyzer->schema->sysCDefMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCDef *sysCDef = sysCDefIt->second;
            sysCDef->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("CON#") == 0)
                    updateNumber32u(sysCDef->con, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysCDef->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TYPE#") == 0)
                    updateNumber16u(sysCDef->type, i, pos, object, rowId);
            }

        } else if (object->systemTable == TABLE_SYS_COL) {
            auto sysColIt = oracleAnalyzer->schema->sysColMapRowId.find(rowId);
            if (sysColIt == oracleAnalyzer->schema->sysColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCol *sysCol = sysColIt->second;
            sysCol->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysCol->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("COL#") == 0)
                    updateNumber16(sysCol->col, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SEGCOL#") == 0)
                    updateNumber16(sysCol->segCol, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("INTCOL#") == 0)
                    updateNumber16(sysCol->intCol, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("NAME") == 0)
                    updateString(sysCol->name, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TYPE#") == 0)
                    updateNumber16u(sysCol->type, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("LENGTH") == 0)
                    updateNumber64u(sysCol->length, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("PRECISION#") == 0)
                    updateNumber64(sysCol->precision, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SCALE") == 0)
                    updateNumber64(sysCol->scale, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("CHARSETFORM") == 0)
                    updateNumber64u(sysCol->charsetForm, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("CHARSETID") == 0)
                    updateNumber64u(sysCol->charsetId, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("NULL$") == 0)
                    updateNumber64(sysCol->null_, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("PROPERTY") == 0)
                    updateNumberXu(sysCol->property, i, pos, object, rowId);
            }

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            auto sysDeferredStgIt = oracleAnalyzer->schema->sysDeferredStgMapRowId.find(rowId);
            if (sysDeferredStgIt == oracleAnalyzer->schema->sysDeferredStgMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysDeferredStg *sysDeferredStg = sysDeferredStgIt->second;
            sysDeferredStg->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysDeferredStg->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("FLAGS_STG") == 0)
                    updateNumberXu(sysDeferredStg->flagsStg, i, pos, object, rowId);
            }

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            auto sysEColIt = oracleAnalyzer->schema->sysEColMapRowId.find(rowId);
            if (sysEColIt == oracleAnalyzer->schema->sysEColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysECol *sysECol = sysEColIt->second;
            sysECol->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("TABOBJ#") == 0)
                    updateNumber32u(sysECol->tabObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("COLNUM") == 0)
                    updateNumber16(sysECol->colNum, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("GUARD_ID") == 0)
                    updateNumber32u(sysECol->guardId, i, pos, object, rowId);
            }

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            auto sysObjIt = oracleAnalyzer->schema->sysObjMapRowId.find(rowId);
            if (sysObjIt == oracleAnalyzer->schema->sysObjMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysObj *sysObj = sysObjIt->second;
            sysObj->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OWNER#") == 0)
                    updateNumber32u(sysObj->owner, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysObj->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysObj->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("NAME") == 0)
                    updateString(sysObj->name, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TYPE#") == 0)
                    updateNumber16u(sysObj->type, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("FLAGS") == 0)
                    updateNumberXu(sysObj->flags, i, pos, object, rowId);
            }

        } else if (object->systemTable == TABLE_SYS_SEG) {
            auto sysSegIt = oracleAnalyzer->schema->sysSegMapRowId.find(rowId);
            if (sysSegIt == oracleAnalyzer->schema->sysSegMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysSeg *sysSeg = sysSegIt->second;
            sysSeg->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("FILE#") == 0)
                    updateNumber32u(sysSeg->file, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("BLOCK#") == 0)
                    updateNumber32u(sysSeg->block, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TS#") == 0)
                    updateNumber32u(sysSeg->ts, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SPARE1") == 0)
                    updateNumberXu(sysSeg->spare1, i, pos, object, rowId);
            }

        } else if (object->systemTable == TABLE_SYS_TAB) {
            auto sysTabIt = oracleAnalyzer->schema->sysTabMapRowId.find(rowId);
            if (sysTabIt == oracleAnalyzer->schema->sysTabMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTab *sysTab = sysTabIt->second;
            sysTab->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysTab->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTab->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TS#") == 0)
                    updateNumber32u(sysTab->ts, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("FILE#") == 0)
                    updateNumber32u(sysTab->file, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("BLOCK#") == 0)
                    updateNumber32u(sysTab->block, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("CLUCOLS") == 0)
                    updateNumber16(sysTab->cluCols, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("FLAGS") == 0)
                    updateNumberXu(sysTab->flags, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("PROPERTY") == 0)
                    updateNumberXu(sysTab->property, i, pos, object, rowId);
            }

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            auto sysTabComPartIt = oracleAnalyzer->schema->sysTabComPartMapRowId.find(rowId);
            if (sysTabComPartIt == oracleAnalyzer->schema->sysTabComPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabComPart *sysTabComPart = sysTabComPartIt->second;
            sysTabComPart->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysTabComPart->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTabComPart->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("BO#") == 0)
                    updateNumber32u(sysTabComPart->bo, i, pos, object, rowId);
            }

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            auto sysTabPartIt = oracleAnalyzer->schema->sysTabPartMapRowId.find(rowId);
            if (sysTabPartIt == oracleAnalyzer->schema->sysTabPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabPart *sysTabPart = sysTabPartIt->second;
            sysTabPart->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysTabPart->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTabPart->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("BO#") == 0)
                    updateNumber32u(sysTabPart->bo, i, pos, object, rowId);
            }

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            auto sysTabSubPartIt = oracleAnalyzer->schema->sysTabSubPartMapRowId.find(rowId);
            if (sysTabSubPartIt == oracleAnalyzer->schema->sysTabSubPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabSubPart *sysTabSubPart = sysTabSubPartIt->second;
            sysTabSubPart->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateNumber32u(sysTabSubPart->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTabSubPart->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("POBJ#") == 0)
                    updateNumber32u(sysTabSubPart->pObj, i, pos, object, rowId);
            }

        } else if (object->systemTable == TABLE_SYS_USER) {
            auto sysUserIt = oracleAnalyzer->schema->sysUserMapRowId.find(rowId);
            if (sysUserIt == oracleAnalyzer->schema->sysUserMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysUser *sysUser = sysUserIt->second;
            sysUser->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("USER#") == 0)
                    updateNumber32u(sysUser->user, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("NAME") == 0)
                    updateString(sysUser->name, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SPARE1") == 0)
                    updateNumberXu(sysUser->spare1, i, pos, object, rowId);
            }
        }
    }

    void SystemTransaction::processDelete(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        RowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete table (name: " << object->owner << "." << object->name << ", rowid: " << rowId << ")");

        if (object->systemTable == TABLE_SYS_CCOL) {
            if (oracleAnalyzer->schema->sysCColMapRowId.find(rowId) == oracleAnalyzer->schema->sysCColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCCol *sysCCol = oracleAnalyzer->schema->sysCColMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (CON#: " << dec << sysCCol->con << ", INTCOL#: " << sysCCol->intCol << ", OBJ#: " <<
                    sysCCol->obj << ", SPARE1: " << sysCCol->spare1 << ")");
            oracleAnalyzer->schema->sysCColMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysCColSetDropped.insert(sysCCol);

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            if (oracleAnalyzer->schema->sysCDefMapRowId.find(rowId) == oracleAnalyzer->schema->sysCDefMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCDef *sysCDef = oracleAnalyzer->schema->sysCDefMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (CON#: " << dec << sysCDef->con << ", OBJ#: " << sysCDef->obj << ", type: " << sysCDef->type << ")");
            oracleAnalyzer->schema->sysCDefMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysCDefSetDropped.insert(sysCDef);

        } else if (object->systemTable == TABLE_SYS_COL) {
            if (oracleAnalyzer->schema->sysColMapRowId.find(rowId) == oracleAnalyzer->schema->sysColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCol *sysCol = oracleAnalyzer->schema->sysColMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysCol->obj << ", COL#: " << sysCol->col << ", SEGCOL#: " << sysCol->segCol <<
                    ", INTCOL#: " << sysCol->intCol << ", NAME: '" << sysCol->name << "', TYPE#: " << sysCol->type << ", LENGTH: " << sysCol->length <<
                    ", PRECISION#: " << sysCol->precision << ", SCALE: " << sysCol->scale << ", CHARSETFORM: " << sysCol->charsetForm <<
                    ", CHARSETID: " << sysCol->charsetId << ", NULL$: " << sysCol->null_ << ", PROPERTY: " << sysCol->property << ")");
            oracleAnalyzer->schema->sysColMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysColSetDropped.insert(sysCol);

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            if (oracleAnalyzer->schema->sysDeferredStgMapRowId.find(rowId) == oracleAnalyzer->schema->sysDeferredStgMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysDeferredStg *sysDeferredStg = oracleAnalyzer->schema->sysDeferredStgMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysDeferredStg->obj << ", FLAGS_STG: " << sysDeferredStg->flagsStg << ")");
            oracleAnalyzer->schema->sysDeferredStgMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysDeferredStgSetDropped.insert(sysDeferredStg);

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            if (oracleAnalyzer->schema->sysEColMapRowId.find(rowId) == oracleAnalyzer->schema->sysEColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysECol *sysECol = oracleAnalyzer->schema->sysEColMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (TABOBJ#: " << dec << sysECol->tabObj << ", COLNUM: " << sysECol->colNum << ", GUARD_ID: " <<
                    sysECol->guardId << ")");
            oracleAnalyzer->schema->sysEColMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysEColSetDropped.insert(sysECol);

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            if (oracleAnalyzer->schema->sysObjMapRowId.find(rowId) == oracleAnalyzer->schema->sysObjMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysObj *sysObj = oracleAnalyzer->schema->sysObjMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OWNER#: " << dec << sysObj->owner << ", OBJ#: " << sysObj->obj << ", DATAOBJ#: " <<
                    sysObj->dataObj << ", TYPE#: " << sysObj->type << ", NAME: '" << sysObj->name << "', FLAGS: " << sysObj->flags << ")");
            oracleAnalyzer->schema->sysObjMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysObjSetDropped.insert(sysObj);

        } else if (object->systemTable == TABLE_SYS_SEG) {
            if (oracleAnalyzer->schema->sysSegMapRowId.find(rowId) == oracleAnalyzer->schema->sysSegMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysSeg *sysSeg = oracleAnalyzer->schema->sysSegMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (FILE#: " << dec << sysSeg->file << ", BLOCK#: " << sysSeg->block << ", TS#: " <<
                    sysSeg->ts << ", SPARE1: " << sysSeg->spare1 << ")");
            oracleAnalyzer->schema->sysSegMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysSegSetDropped.insert(sysSeg);

        } else if (object->systemTable == TABLE_SYS_TAB) {
            if (oracleAnalyzer->schema->sysTabMapRowId.find(rowId) == oracleAnalyzer->schema->sysTabMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTab *sysTab = oracleAnalyzer->schema->sysTabMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTab->obj << ", DATAOBJ#: " << sysTab->dataObj << ", TS#: " <<
                    sysTab->ts << ", FILE#: " << sysTab->file << ", BLOCK#: " << sysTab->block << ", CLUCOLS: " << sysTab->cluCols << ", FLAGS: " <<
                    sysTab->flags << ", PROPERTY: " << sysTab->property << ")");
            oracleAnalyzer->schema->sysTabMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysTabSetDropped.insert(sysTab);

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            if (oracleAnalyzer->schema->sysTabComPartMapRowId.find(rowId) == oracleAnalyzer->schema->sysTabComPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabComPart *sysTabComPart = oracleAnalyzer->schema->sysTabComPartMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabComPart->obj << ", DATAOBJ#: " << sysTabComPart->dataObj << ", BO#: " <<
                    sysTabComPart->bo << ")");
            oracleAnalyzer->schema->sysTabComPartMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysTabComPartSetDropped.insert(sysTabComPart);

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            if (oracleAnalyzer->schema->sysTabPartMapRowId.find(rowId) == oracleAnalyzer->schema->sysTabPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabPart *sysTabPart = oracleAnalyzer->schema->sysTabPartMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabPart->obj << ", DATAOBJ#: " << sysTabPart->dataObj << ", BO#: " <<
                    sysTabPart->bo << ")");
            oracleAnalyzer->schema->sysTabPartMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysTabPartSetDropped.insert(sysTabPart);

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            if (oracleAnalyzer->schema->sysTabSubPartMapRowId.find(rowId) == oracleAnalyzer->schema->sysTabSubPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabSubPart *sysTabSubPart = oracleAnalyzer->schema->sysTabSubPartMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabSubPart->obj << ", DATAOBJ#: " << sysTabSubPart->dataObj << ", POBJ#: " <<
                    sysTabSubPart->pObj << ")");
            oracleAnalyzer->schema->sysTabSubPartMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysTabSubPartSetDropped.insert(sysTabSubPart);

        } else if (object->systemTable == TABLE_SYS_USER) {
            if (oracleAnalyzer->schema->sysUserMapRowId.find(rowId) == oracleAnalyzer->schema->sysUserMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysUser *sysUser = oracleAnalyzer->schema->sysUserMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (USER#: " << dec << sysUser->user << ", NAME: " << sysUser->name << ", SPARE1: " <<
                    sysUser->spare1 << ")");
            oracleAnalyzer->schema->sysUserMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysUserSetDropped.insert(sysUser);
        }
    }

    void SystemTransaction::commit(void) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: commit");

        oracleAnalyzer->schema->refreshIndexes();
        oracleAnalyzer->schema->rebuildMaps(oracleAnalyzer);
        INFO("schema updated");
    }
}
