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
            SysCCol *sysCCol = new SysCCol(rowId, 0, 0, 0, 0, 0);

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

            SysCColKey sysCColKey(sysCCol->obj, sysCCol->intCol, sysCCol->con);
            if (oracleAnalyzer->schema->sysCColMapKey.find(sysCColKey) != oracleAnalyzer->schema->sysCColMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.CCOL$: (OBJ#: " << dec << sysCCol->obj << ", INTCOL#: " << sysCCol->intCol << ", CON#: " << sysCCol->con << ") for insert");
            }
            oracleAnalyzer->schema->sysCColMapRowId[rowId] = sysCCol;
            oracleAnalyzer->schema->sysCColMapKey[sysCColKey] = sysCCol;

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            if (oracleAnalyzer->schema->sysCDefMapRowId.find(rowId) != oracleAnalyzer->schema->sysCDefMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEF$: (rowid: " << rowId << ") for insert");
            }
            SysCDef *sysCDef = new SysCDef(rowId, 0, 0, 0);

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

            SysCDefKey sysCDefKey(sysCDef->obj, sysCDef->con);
            if (oracleAnalyzer->schema->sysCDefMapKey.find(sysCDefKey) != oracleAnalyzer->schema->sysCDefMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEF$: (OBJ#: " << dec << sysCDef->obj << ", CON#: " << sysCDef->con << ") for insert");
            }
            oracleAnalyzer->schema->sysCDefMapRowId[rowId] = sysCDef;
            oracleAnalyzer->schema->sysCDefMapKey[sysCDefKey] = sysCDef;

        } else if (object->systemTable == TABLE_SYS_COL) {
            if (oracleAnalyzer->schema->sysColMapRowId.find(rowId) != oracleAnalyzer->schema->sysColMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.COL$: (rowid: " << rowId << ")for insert");
            }
            SysCol *sysCol = new SysCol(rowId, 0, 0, 0, 0, "", 0, 0, -1, -1, 0, 0, 0, 0, 0);

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

            SysColKey sysColKey(sysCol->obj, sysCol->intCol);
            if (oracleAnalyzer->schema->sysColMapKey.find(sysColKey) != oracleAnalyzer->schema->sysColMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.COL$: (OBJ#: " << dec << sysCol->obj << ", INTCOL#: " << sysCol->intCol << ") for insert");
            }
            SysColSeg sysColSeg(sysCol->obj, sysCol->segCol);
            if (oracleAnalyzer->schema->sysColMapSeg.find(sysColSeg) != oracleAnalyzer->schema->sysColMapSeg.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.COL$: (OBJ#: " << dec << sysCol->obj << ", SEGCOL#: " << sysCol->segCol << ") for insert");
            }
            oracleAnalyzer->schema->sysColMapRowId[rowId] = sysCol;
            oracleAnalyzer->schema->sysColMapKey[sysColKey] = sysCol;
            oracleAnalyzer->schema->sysColMapSeg[sysColSeg] = sysCol;

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            if (oracleAnalyzer->schema->sysDeferredStgMapRowId.find(rowId) != oracleAnalyzer->schema->sysDeferredStgMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEFERRED_STG$: (rowid: " << rowId << ") for insert");
            }
            SysDeferredStg *sysDeferredStg = new SysDeferredStg(rowId, 0, 0, 0);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateNumber32u(sysDeferredStg->obj, i, pos, object, rowId);
                } else if (object->columns[i]->name.compare("FLAGS_STG") == 0)
                    updateNumberXu(sysDeferredStg->flagsStg, i, pos, object, rowId);
            }

            if (oracleAnalyzer->schema->sysDeferredStgMapObj.find(sysDeferredStg->obj) != oracleAnalyzer->schema->sysDeferredStgMapObj.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEFERRED_STG$: (OBJ#: " << dec << sysDeferredStg->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysDeferredStgMapRowId[rowId] = sysDeferredStg;
            oracleAnalyzer->schema->sysDeferredStgMapObj[sysDeferredStg->obj] = sysDeferredStg;

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            if (oracleAnalyzer->schema->sysEColMapRowId.find(rowId) != oracleAnalyzer->schema->sysEColMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.ECOL$: (rowid: " << rowId << ") for insert");
            }
            SysECol *sysECol = new SysECol(rowId, 0, 0, -1);

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

            SysEColKey sysEColKey(sysECol->tabObj, sysECol->colNum);
            if (oracleAnalyzer->schema->sysEColMapKey.find(sysEColKey) != oracleAnalyzer->schema->sysEColMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.ECOL$: (TABOBJ#: " << dec << sysECol->tabObj << ", COLNUM: " << sysECol->colNum << ") for insert");
            }
            oracleAnalyzer->schema->sysEColMapRowId[rowId] = sysECol;
            oracleAnalyzer->schema->sysEColMapKey[sysEColKey] = sysECol;

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            SysObj *sysObj = oracleAnalyzer->schema->sysObjMapRowId[rowId];
            if (sysObj != nullptr) {
                RUNTIME_FAIL("DDL: duplicate SYS.OBJ$: (rowid: " << rowId << ") for insert");
            }
            sysObj = new SysObj(rowId, 0, 0, 0, 0, "", 0, 0);

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

            if (oracleAnalyzer->schema->sysObjMapObj.find(sysObj->obj) != oracleAnalyzer->schema->sysObjMapObj.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.OBJ$: (OBJ#: " << dec << sysObj->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysObjMapRowId[rowId] = sysObj;
            oracleAnalyzer->schema->sysObjMapObj[sysObj->obj] = sysObj;

        } else if (object->systemTable == TABLE_SYS_SEG) {
            if (oracleAnalyzer->schema->sysSegMapRowId.find(rowId) != oracleAnalyzer->schema->sysSegMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.SEG$: (rowid: " << rowId << ") for insert");
            }
            SysSeg *sysSeg = new SysSeg(rowId, 0, 0, 0, 0, 0);

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

            SysSegKey sysSegKey(sysSeg->file, sysSeg->block, sysSeg->ts);
            if (oracleAnalyzer->schema->sysSegMapKey.find(sysSegKey) != oracleAnalyzer->schema->sysSegMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.SEG$: (FILE#: " << dec << sysSeg->file << ", BLOCK#: " << sysSeg->block << ", TS#: " << sysSeg->ts << ") for insert");
            }
            oracleAnalyzer->schema->sysSegMapRowId[rowId] = sysSeg;
            oracleAnalyzer->schema->sysSegMapKey[sysSegKey] = sysSeg;

        } else if (object->systemTable == TABLE_SYS_TAB) {
            if (oracleAnalyzer->schema->sysTabMapRowId.find(rowId) != oracleAnalyzer->schema->sysTabMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TAB$: (rowid: " << rowId << ") for insert");
            }
            SysTab *sysTab = new SysTab(rowId, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

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

            if (oracleAnalyzer->schema->sysTabMapObj.find(sysTab->obj) != oracleAnalyzer->schema->sysTabMapObj.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TAB$: (OBJ#: " << dec << sysTab->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysTabMapRowId[rowId] = sysTab;
            oracleAnalyzer->schema->sysTabMapObj[sysTab->obj] = sysTab;

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            if (oracleAnalyzer->schema->sysTabComPartMapRowId.find(rowId) != oracleAnalyzer->schema->sysTabComPartMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABCOMPART$: (rowid: " << rowId << ") for insert");
            }
            SysTabComPart *sysTabComPart = new SysTabComPart(rowId, 0, 0, 0);

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

            SysTabComPartKey sysTabComPartKey(sysTabComPart->bo, sysTabComPart->obj);
            if (oracleAnalyzer->schema->sysTabComPartMapKey.find(sysTabComPartKey) != oracleAnalyzer->schema->sysTabComPartMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABCOMPART$: (BO#: " << dec << sysTabComPart->bo << ", OBJ#: " << sysTabComPart->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysTabComPartMapRowId[rowId] = sysTabComPart;
            oracleAnalyzer->schema->sysTabComPartMapKey[sysTabComPartKey] = sysTabComPart;

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            if (oracleAnalyzer->schema->sysTabPartMapRowId.find(rowId) != oracleAnalyzer->schema->sysTabPartMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABPART$: (rowid: " << rowId << ") for insert");
            }
            SysTabPart *sysTabPart = new SysTabPart(rowId, 0, 0, 0);

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

            SysTabPartKey sysTabPartKey(sysTabPart->bo, sysTabPart->obj);
            if (oracleAnalyzer->schema->sysTabPartMapKey.find(sysTabPartKey) != oracleAnalyzer->schema->sysTabPartMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABPART$: (BO#: " << dec << sysTabPart->bo << ", OBJ#: " << sysTabPart->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysTabPartMapRowId[rowId] = sysTabPart;
            oracleAnalyzer->schema->sysTabPartMapKey[sysTabPartKey] = sysTabPart;

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            if (oracleAnalyzer->schema->sysTabSubPartMapRowId.find(rowId) != oracleAnalyzer->schema->sysTabSubPartMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABSUBPART$: (rowid: " << rowId << ") for insert");
            }
            SysTabSubPart *sysTabSubPart = new SysTabSubPart(rowId, 0, 0, 0);

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

            SysTabSubPartKey sysTabSubPartKey(sysTabSubPart->pObj, sysTabSubPart->obj);
            if (oracleAnalyzer->schema->sysTabSubPartMapKey.find(sysTabSubPartKey) != oracleAnalyzer->schema->sysTabSubPartMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABSUBPART$: (POBJ#: " << dec << sysTabSubPart->pObj << ", OBJ#: " << sysTabSubPart->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysTabSubPartMapRowId[rowId] = sysTabSubPart;
            oracleAnalyzer->schema->sysTabSubPartMapKey[sysTabSubPartKey] = sysTabSubPart;

        } else if (object->systemTable == TABLE_SYS_USER) {
            if (oracleAnalyzer->schema->sysUserMapRowId.find(rowId) != oracleAnalyzer->schema->sysUserMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.USER$: (rowid: " << rowId << ") for insert");
            }
            SysUser *sysUser = new SysUser(rowId, 0, "", 0, 0, false);

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

            if (oracleAnalyzer->schema->sysUserMapUser.find(sysUser->user) != oracleAnalyzer->schema->sysUserMapUser.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.USER$: (USER#: " << dec << sysUser->user << ") for insert");
            }
            oracleAnalyzer->schema->sysUserMapRowId[rowId] = sysUser;
            oracleAnalyzer->schema->sysUserMapUser[sysUser->user] = sysUser;
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
            SysCColKey sysCColKey(sysCCol->obj, sysCCol->intCol, sysCCol->con);
            if (oracleAnalyzer->schema->sysCColMapKey.find(sysCColKey) == oracleAnalyzer->schema->sysCColMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.CCOL$: (OBJ#: " << sysCCol->obj << ", INTCOL#: " << sysCCol->intCol << ", CON#: " << sysCCol->con << ") for delete");
            }
            oracleAnalyzer->schema->sysCColMapKey.erase(sysCColKey);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("CON#") == 0) {
                    updateNumber32u(sysCCol->con, i, pos, object, rowId);
                    sysCColKey.con = sysCCol->con;
                } else if (object->columns[i]->name.compare("INTCOL#") == 0) {
                    updateNumber16(sysCCol->intCol, i, pos, object, rowId);
                    sysCColKey.intCol = sysCCol->intCol;
                } else if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateNumber32u(sysCCol->obj, i, pos, object, rowId);
                    sysCColKey.obj = sysCCol->obj;
                } else if (object->columns[i]->name.compare("SPARE1") == 0)
                    updateNumberXu(sysCCol->spare1, i, pos, object, rowId);
            }

            if (oracleAnalyzer->schema->sysCColMapKey.find(sysCColKey) != oracleAnalyzer->schema->sysCColMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.CCOL$: (OBJ#: " << dec << sysCCol->obj << ", INTCOL#: " << sysCCol->intCol << ", CON#: " << sysCCol->con << ") for insert");
            }
            oracleAnalyzer->schema->sysCColMapKey[sysCColKey] = sysCCol;

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            auto sysCDefIt = oracleAnalyzer->schema->sysCDefMapRowId.find(rowId);
            if (sysCDefIt == oracleAnalyzer->schema->sysCDefMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCDef *sysCDef = sysCDefIt->second;
            SysCDefKey sysCDefKey(sysCDef->obj, sysCDef->con);
            if (oracleAnalyzer->schema->sysCDefMapKey.find(sysCDefKey) == oracleAnalyzer->schema->sysCDefMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.CDEF$: (OBJ#: " << sysCDef->obj << ", CON#: " << sysCDef->con << ") for delete");
            }
            oracleAnalyzer->schema->sysCDefMapKey.erase(sysCDefKey);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("CON#") == 0) {
                    updateNumber32u(sysCDef->con, i, pos, object, rowId);
                    sysCDefKey.con = sysCDef->con;
                } else if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateNumber32u(sysCDef->obj, i, pos, object, rowId);
                    sysCDefKey.obj = sysCDef->obj;
                } else if (object->columns[i]->name.compare("TYPE#") == 0)
                    updateNumber16u(sysCDef->type, i, pos, object, rowId);
            }

            if (oracleAnalyzer->schema->sysCDefMapKey.find(sysCDefKey) != oracleAnalyzer->schema->sysCDefMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEF$: (OBJ#: " << dec << sysCDef->obj << ", CON#: " << sysCDef->con << ") for insert");
            }
            oracleAnalyzer->schema->sysCDefMapKey[sysCDefKey] = sysCDef;

        } else if (object->systemTable == TABLE_SYS_COL) {
            auto sysColIt = oracleAnalyzer->schema->sysColMapRowId.find(rowId);
            if (sysColIt == oracleAnalyzer->schema->sysColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCol *sysCol = sysColIt->second;
            SysColKey sysColKey(sysCol->obj, sysCol->intCol);
            if (oracleAnalyzer->schema->sysColMapKey.find(sysColKey) == oracleAnalyzer->schema->sysColMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.COL$: (OBJ#: " << sysCol->obj << ", INTCOL#: " << sysCol->intCol << ") for delete");
            }
            oracleAnalyzer->schema->sysColMapKey.erase(sysColKey);
            SysColSeg sysColSeg(sysCol->obj, sysCol->segCol);
            if (oracleAnalyzer->schema->sysColMapSeg.find(sysColSeg) == oracleAnalyzer->schema->sysColMapSeg.end()) {
                RUNTIME_FAIL("DDL: missing SYS.COL$: (OBJ#: " << sysCol->obj << ", SEGCOL#: " << sysCol->segCol << ") for delete");
            }
            oracleAnalyzer->schema->sysColMapSeg.erase(sysColSeg);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateNumber32u(sysCol->obj, i, pos, object, rowId);
                    sysColKey.obj = sysCol->obj;
                    sysColSeg.obj = sysCol->obj;
                } else if (object->columns[i]->name.compare("COL#") == 0)
                    updateNumber16(sysCol->col, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SEGCOL#") == 0) {
                    updateNumber16(sysCol->segCol, i, pos, object, rowId);
                    sysColSeg.segCol = sysCol->segCol;
                } else if (object->columns[i]->name.compare("INTCOL#") == 0) {
                    updateNumber16(sysCol->intCol, i, pos, object, rowId);
                    sysColKey.intCol = sysCol->intCol;
                } else if (object->columns[i]->name.compare("NAME") == 0)
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

            if (oracleAnalyzer->schema->sysColMapKey.find(sysColKey) != oracleAnalyzer->schema->sysColMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.COL$: (OBJ#: " << dec << sysCol->obj << ", INTCOL#: " << sysCol->intCol << ") for insert");
            }
            oracleAnalyzer->schema->sysColMapKey[sysColKey] = sysCol;
            if (oracleAnalyzer->schema->sysColMapSeg.find(sysColSeg) != oracleAnalyzer->schema->sysColMapSeg.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.COL$: (OBJ#: " << dec << sysCol->obj << ", SEGCOL#: " << sysCol->segCol << ") for insert");
            }
            oracleAnalyzer->schema->sysColMapSeg[sysColSeg] = sysCol;

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            auto sysDeferredStgIt = oracleAnalyzer->schema->sysDeferredStgMapRowId.find(rowId);
            if (sysDeferredStgIt == oracleAnalyzer->schema->sysDeferredStgMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysDeferredStg *sysDeferredStg = sysDeferredStgIt->second;
            if (oracleAnalyzer->schema->sysDeferredStgMapObj.find(sysDeferredStg->obj) == oracleAnalyzer->schema->sysDeferredStgMapObj.end()) {
                RUNTIME_FAIL("DDL: missing SYS.DEFERRED_STG$: (OBJ#: " << sysDeferredStg->obj << ") for delete");
            }
            oracleAnalyzer->schema->sysDeferredStgMapObj.erase(sysDeferredStg->obj);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateNumber32u(sysDeferredStg->obj, i, pos, object, rowId);
                } else if (object->columns[i]->name.compare("FLAGS_STG") == 0)
                    updateNumberXu(sysDeferredStg->flagsStg, i, pos, object, rowId);
            }

            if (oracleAnalyzer->schema->sysDeferredStgMapObj.find(sysDeferredStg->obj) != oracleAnalyzer->schema->sysDeferredStgMapObj.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEFERRED_STG$: (OBJ#: " << dec << sysDeferredStg->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysDeferredStgMapObj[sysDeferredStg->obj] = sysDeferredStg;

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            auto sysEColIt = oracleAnalyzer->schema->sysEColMapRowId.find(rowId);
            if (sysEColIt == oracleAnalyzer->schema->sysEColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysECol *sysECol = sysEColIt->second;
            SysEColKey sysEColKey(sysECol->tabObj, sysECol->colNum);
            if (oracleAnalyzer->schema->sysEColMapKey.find(sysEColKey) == oracleAnalyzer->schema->sysEColMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.ECOL$: (TABOBJ#: " << sysECol->tabObj << ", COLNUM#: " << sysECol->colNum << ") for delete");
            }
            oracleAnalyzer->schema->sysEColMapKey.erase(sysEColKey);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("TABOBJ#") == 0) {
                    updateNumber32u(sysECol->tabObj, i, pos, object, rowId);
                    sysEColKey.tabObj = sysECol->tabObj;
                } else if (object->columns[i]->name.compare("COLNUM") == 0) {
                    updateNumber16(sysECol->colNum, i, pos, object, rowId);
                    sysEColKey.colNum = sysECol->colNum;
                } else if (object->columns[i]->name.compare("GUARD_ID") == 0)
                    updateNumber32u(sysECol->guardId, i, pos, object, rowId);
            }

            if (oracleAnalyzer->schema->sysEColMapKey.find(sysEColKey) != oracleAnalyzer->schema->sysEColMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.ECOL$: (TABOBJ#: " << dec << sysECol->tabObj << ", COLNUM: " << sysECol->colNum << ") for insert");
            }
            oracleAnalyzer->schema->sysEColMapKey[sysEColKey] = sysECol;

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            auto sysObjIt = oracleAnalyzer->schema->sysObjMapRowId.find(rowId);
            if (sysObjIt == oracleAnalyzer->schema->sysObjMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysObj *sysObj = sysObjIt->second;
            if (oracleAnalyzer->schema->sysObjMapObj.find(sysObj->obj) == oracleAnalyzer->schema->sysObjMapObj.end()) {
                RUNTIME_FAIL("DDL: missing SYS.OBJ$: (OBJ#: " << sysObj->obj << ") for delete");
            }
            oracleAnalyzer->schema->sysObjMapObj.erase(sysObj->obj);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OWNER#") == 0)
                    updateNumber32u(sysObj->owner, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateNumber32u(sysObj->obj, i, pos, object, rowId);
                } else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysObj->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("NAME") == 0)
                    updateString(sysObj->name, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TYPE#") == 0)
                    updateNumber16u(sysObj->type, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("FLAGS") == 0)
                    updateNumberXu(sysObj->flags, i, pos, object, rowId);
            }

            if (oracleAnalyzer->schema->sysObjMapObj.find(sysObj->obj) != oracleAnalyzer->schema->sysObjMapObj.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.OBJ$: (OBJ#: " << dec << sysObj->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysObjMapObj[sysObj->obj] = sysObj;

        } else if (object->systemTable == TABLE_SYS_SEG) {
            auto sysSegIt = oracleAnalyzer->schema->sysSegMapRowId.find(rowId);
            if (sysSegIt == oracleAnalyzer->schema->sysSegMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysSeg *sysSeg = sysSegIt->second;
            SysSegKey sysSegKey(sysSeg->file, sysSeg->block, sysSeg->ts);
            if (oracleAnalyzer->schema->sysSegMapKey.find(sysSegKey) == oracleAnalyzer->schema->sysSegMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.SEG$: (FILE#: " << sysSeg->file << ", BLOCK#: " << sysSeg->block << ", TS#: " << sysSeg->ts << ") for delete");
            }
            oracleAnalyzer->schema->sysSegMapKey.erase(sysSegKey);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("FILE#") == 0) {
                    updateNumber32u(sysSeg->file, i, pos, object, rowId);
                    sysSegKey.file = sysSeg->file;
                } else if (object->columns[i]->name.compare("BLOCK#") == 0) {
                    updateNumber32u(sysSeg->block, i, pos, object, rowId);
                    sysSegKey.block = sysSeg->block;
                } else if (object->columns[i]->name.compare("TS#") == 0) {
                    updateNumber32u(sysSeg->ts, i, pos, object, rowId);
                    sysSegKey.ts = sysSeg->ts;
                } else if (object->columns[i]->name.compare("SPARE1") == 0)
                    updateNumberXu(sysSeg->spare1, i, pos, object, rowId);
            }

            if (oracleAnalyzer->schema->sysSegMapKey.find(sysSegKey) != oracleAnalyzer->schema->sysSegMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.SEG$: (FILE#: " << dec << sysSeg->file << ", BLOCK#: " << sysSeg->block << ", TS#: " << sysSeg->ts << ") for insert");
            }
            oracleAnalyzer->schema->sysSegMapKey[sysSegKey] = sysSeg;

        } else if (object->systemTable == TABLE_SYS_TAB) {
            auto sysTabIt = oracleAnalyzer->schema->sysTabMapRowId.find(rowId);
            if (sysTabIt == oracleAnalyzer->schema->sysTabMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTab *sysTab = sysTabIt->second;
            if (oracleAnalyzer->schema->sysTabMapObj.find(sysTab->obj) == oracleAnalyzer->schema->sysTabMapObj.end()) {
                RUNTIME_FAIL("DDL: missing SYS.TAB$: (OBJ#: " << sysTab->obj << ") for delete");
            }
            oracleAnalyzer->schema->sysTabMapObj.erase(sysTab->obj);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateNumber32u(sysTab->obj, i, pos, object, rowId);
                } else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
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

            if (oracleAnalyzer->schema->sysTabMapObj.find(sysTab->obj) != oracleAnalyzer->schema->sysTabMapObj.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TAB$: (OBJ#: " << dec << sysTab->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysTabMapObj[sysTab->obj] = sysTab;

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            auto sysTabComPartIt = oracleAnalyzer->schema->sysTabComPartMapRowId.find(rowId);
            if (sysTabComPartIt == oracleAnalyzer->schema->sysTabComPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabComPart *sysTabComPart = sysTabComPartIt->second;
            SysTabComPartKey sysTabComPartKey(sysTabComPart->bo, sysTabComPart->obj);
            if (oracleAnalyzer->schema->sysTabComPartMapKey.find(sysTabComPartKey) == oracleAnalyzer->schema->sysTabComPartMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.TABCOMPART$: (BO#: " << sysTabComPart->bo << ", OBJ#: " << sysTabComPart->obj << ") for delete");
            }
            oracleAnalyzer->schema->sysTabComPartMapKey.erase(sysTabComPartKey);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateNumber32u(sysTabComPart->obj, i, pos, object, rowId);
                    sysTabComPartKey.obj = sysTabComPart->obj;
                } else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTabComPart->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("BO#") == 0) {
                    updateNumber32u(sysTabComPart->bo, i, pos, object, rowId);
                    sysTabComPartKey.bo = sysTabComPart->bo;
                }
            }

            if (oracleAnalyzer->schema->sysTabComPartMapKey.find(sysTabComPartKey) != oracleAnalyzer->schema->sysTabComPartMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABCOMPART$: (BO#: " << dec << sysTabComPart->bo << ", OBJ#: " << sysTabComPart->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysTabComPartMapKey[sysTabComPartKey] = sysTabComPart;

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            auto sysTabPartIt = oracleAnalyzer->schema->sysTabPartMapRowId.find(rowId);
            if (sysTabPartIt == oracleAnalyzer->schema->sysTabPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabPart *sysTabPart = sysTabPartIt->second;
            SysTabPartKey sysTabPartKey(sysTabPart->bo, sysTabPart->obj);
            if (oracleAnalyzer->schema->sysTabPartMapKey.find(sysTabPartKey) == oracleAnalyzer->schema->sysTabPartMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.TABPART$: (BO#: " << sysTabPart->bo << ", OBJ#: " << sysTabPart->obj << ") for delete");
            }
            oracleAnalyzer->schema->sysTabPartMapKey.erase(sysTabPartKey);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateNumber32u(sysTabPart->obj, i, pos, object, rowId);
                    sysTabPartKey.obj = sysTabPart->obj;
                } else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTabPart->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("BO#") == 0) {
                    updateNumber32u(sysTabPart->bo, i, pos, object, rowId);
                    sysTabPartKey.bo = sysTabPart->bo;
                }
            }

            if (oracleAnalyzer->schema->sysTabPartMapKey.find(sysTabPartKey) != oracleAnalyzer->schema->sysTabPartMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABPART$: (BO#: " << dec << sysTabPart->bo << ", OBJ#: " << sysTabPart->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysTabPartMapKey[sysTabPartKey] = sysTabPart;

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            auto sysTabSubPartIt = oracleAnalyzer->schema->sysTabSubPartMapRowId.find(rowId);
            if (sysTabSubPartIt == oracleAnalyzer->schema->sysTabSubPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabSubPart *sysTabSubPart = sysTabSubPartIt->second;
            SysTabSubPartKey sysTabSubPartKey(sysTabSubPart->pObj, sysTabSubPart->obj);
            if (oracleAnalyzer->schema->sysTabSubPartMapKey.find(sysTabSubPartKey) == oracleAnalyzer->schema->sysTabSubPartMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.TABSUBPART$: (POBJ#: " << sysTabSubPart->pObj << ", OBJ#: " << sysTabSubPart->obj << ") for delete");
            }
            oracleAnalyzer->schema->sysTabSubPartMapKey.erase(sysTabSubPartKey);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateNumber32u(sysTabSubPart->obj, i, pos, object, rowId);
                    sysTabSubPartKey.obj = sysTabSubPart->obj;
                } else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTabSubPart->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("POBJ#") == 0) {
                    updateNumber32u(sysTabSubPart->pObj, i, pos, object, rowId);
                    sysTabSubPartKey.pObj = sysTabSubPart->pObj;
                }
            }

            if (oracleAnalyzer->schema->sysTabSubPartMapKey.find(sysTabSubPartKey) != oracleAnalyzer->schema->sysTabSubPartMapKey.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABSUBPART$: (POBJ#: " << dec << sysTabSubPart->pObj << ", OBJ#: " << sysTabSubPart->obj << ") for insert");
            }
            oracleAnalyzer->schema->sysTabSubPartMapKey[sysTabSubPartKey] = sysTabSubPart;

        } else if (object->systemTable == TABLE_SYS_USER) {
            auto sysUserIt = oracleAnalyzer->schema->sysUserMapRowId.find(rowId);
            if (sysUserIt == oracleAnalyzer->schema->sysUserMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysUser *sysUser = sysUserIt->second;
            if (oracleAnalyzer->schema->sysUserMapUser.find(sysUser->user) == oracleAnalyzer->schema->sysUserMapUser.end()) {
                RUNTIME_FAIL("DDL: missing SYS.USER$: (USER#: " << sysUser->user << ") for delete");
            }

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("USER#") == 0) {
                    updateNumber32u(sysUser->user, i, pos, object, rowId);
                } else if (object->columns[i]->name.compare("NAME") == 0)
                    updateString(sysUser->name, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SPARE1") == 0)
                    updateNumberXu(sysUser->spare1, i, pos, object, rowId);
            }

            if (oracleAnalyzer->schema->sysUserMapUser.find(sysUser->user) != oracleAnalyzer->schema->sysUserMapUser.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.USER$: (USER#: " << dec << sysUser->user << ") for insert");
            }
            oracleAnalyzer->schema->sysUserMapUser[sysUser->user] = sysUser;
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
            SysCColKey sysCColKey(sysCCol->obj, sysCCol->intCol, sysCCol->con);
            if (oracleAnalyzer->schema->sysCColMapKey.find(sysCColKey) == oracleAnalyzer->schema->sysCColMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.CCOL$: (OBJ#: " << sysCCol->obj << ", INTCOL#: " << sysCCol->intCol << ", CON#: " << sysCCol->con << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (CON#: " << dec << sysCCol->con << ", INTCOL#: " << sysCCol->intCol << ", OBJ#: " <<
                    sysCCol->obj << ", SPARE1: " << sysCCol->spare1 << ")");
            oracleAnalyzer->schema->sysCColMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysCColMapKey.erase(sysCColKey);
            delete sysCCol;

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            if (oracleAnalyzer->schema->sysCDefMapRowId.find(rowId) == oracleAnalyzer->schema->sysCDefMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCDef *sysCDef = oracleAnalyzer->schema->sysCDefMapRowId[rowId];
            SysCDefKey sysCDefKey(sysCDef->obj, sysCDef->con);
            if (oracleAnalyzer->schema->sysCDefMapKey.find(sysCDefKey) == oracleAnalyzer->schema->sysCDefMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.CDEF$: (OBJ#: " << sysCDef->obj << ", CON#: " << sysCDef->con << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (CON#: " << dec << sysCDef->con << ", OBJ#: " << sysCDef->obj << ", type: " << sysCDef->type << ")");
            oracleAnalyzer->schema->sysCDefMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysCDefMapKey.erase(sysCDefKey);
            delete sysCDef;

        } else if (object->systemTable == TABLE_SYS_COL) {
            if (oracleAnalyzer->schema->sysColMapRowId.find(rowId) == oracleAnalyzer->schema->sysColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCol *sysCol = oracleAnalyzer->schema->sysColMapRowId[rowId];
            SysColKey sysColKey(sysCol->obj, sysCol->intCol);
            if (oracleAnalyzer->schema->sysColMapKey.find(sysColKey) == oracleAnalyzer->schema->sysColMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.COL$: (OBJ#: " << sysCol->obj << ", INTCOL#: " << sysCol->intCol << ") for delete");
            }
            SysColSeg sysColSeg(sysCol->obj, sysCol->segCol);
            if (oracleAnalyzer->schema->sysColMapSeg.find(sysColSeg) == oracleAnalyzer->schema->sysColMapSeg.end()) {
                RUNTIME_FAIL("DDL: missing SYS.COL$: (OBJ#: " << sysCol->obj << ", SEGCOL#: " << sysCol->segCol << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysCol->obj << ", COL#: " << sysCol->col << ", SEGCOL#: " << sysCol->segCol <<
                    ", INTCOL#: " << sysCol->intCol << ", NAME: '" << sysCol->name << "', TYPE#: " << sysCol->type << ", LENGTH: " << sysCol->length <<
                    ", PRECISION#: " << sysCol->precision << ", SCALE: " << sysCol->scale << ", CHARSETFORM: " << sysCol->charsetForm <<
                    ", CHARSETID: " << sysCol->charsetId << ", NULL$: " << sysCol->null_ << ", PROPERTY: " << sysCol->property << ")");
            oracleAnalyzer->schema->sysColMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysColMapKey.erase(sysColKey);
            oracleAnalyzer->schema->sysColMapSeg.erase(sysColSeg);
            delete sysCol;

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            if (oracleAnalyzer->schema->sysDeferredStgMapRowId.find(rowId) == oracleAnalyzer->schema->sysDeferredStgMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysDeferredStg *sysDeferredStg = oracleAnalyzer->schema->sysDeferredStgMapRowId[rowId];
            if (oracleAnalyzer->schema->sysDeferredStgMapObj.find(sysDeferredStg->obj) == oracleAnalyzer->schema->sysDeferredStgMapObj.end()) {
                RUNTIME_FAIL("DDL: missing SYS.DEFERRED_STG$: (OBJ#: " << sysDeferredStg->obj << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysDeferredStg->obj << ", FLAGS_STG: " << sysDeferredStg->flagsStg << ")");
            oracleAnalyzer->schema->sysDeferredStgMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysDeferredStgMapObj.erase(sysDeferredStg->obj);
            delete sysDeferredStg;

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            if (oracleAnalyzer->schema->sysEColMapRowId.find(rowId) == oracleAnalyzer->schema->sysEColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysECol *sysECol = oracleAnalyzer->schema->sysEColMapRowId[rowId];
            SysEColKey sysEColKey(sysECol->tabObj, sysECol->colNum);
            if (oracleAnalyzer->schema->sysEColMapKey.find(sysEColKey) == oracleAnalyzer->schema->sysEColMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.ECOL$: (TABOBJ#: " << sysECol->tabObj << ", COLNUM#: " << sysECol->colNum << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (TABOBJ#: " << dec << sysECol->tabObj << ", COLNUM: " << sysECol->colNum << ", GUARD_ID: " <<
                    sysECol->guardId << ")");
            oracleAnalyzer->schema->sysEColMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysEColMapKey.erase(sysEColKey);
            delete sysECol;

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            if (oracleAnalyzer->schema->sysObjMapRowId.find(rowId) == oracleAnalyzer->schema->sysObjMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysObj *sysObj = oracleAnalyzer->schema->sysObjMapRowId[rowId];
            if (oracleAnalyzer->schema->sysObjMapObj.find(sysObj->obj) == oracleAnalyzer->schema->sysObjMapObj.end()) {
                RUNTIME_FAIL("DDL: missing SYS.OBJ$: (OBJ#: " << sysObj->obj << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OWNER#: " << dec << sysObj->owner << ", OBJ#: " << sysObj->obj << ", DATAOBJ#: " <<
                    sysObj->dataObj << ", TYPE#: " << sysObj->type << ", NAME: '" << sysObj->name << "', FLAGS: " << sysObj->flags << ")");
            oracleAnalyzer->schema->sysObjMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysObjMapObj.erase(sysObj->obj);
            delete sysObj;

        } else if (object->systemTable == TABLE_SYS_SEG) {
            if (oracleAnalyzer->schema->sysSegMapRowId.find(rowId) == oracleAnalyzer->schema->sysSegMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysSeg *sysSeg = oracleAnalyzer->schema->sysSegMapRowId[rowId];
            SysSegKey sysSegKey(sysSeg->file, sysSeg->block, sysSeg->ts);
            if (oracleAnalyzer->schema->sysSegMapKey.find(sysSegKey) == oracleAnalyzer->schema->sysSegMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.SEG$: (FILE#: " << sysSeg->file << ", BLOCK#: " << sysSeg->block << ", TS#: " << sysSeg->ts << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (FILE#: " << dec << sysSeg->file << ", BLOCK#: " << sysSeg->block << ", TS#: " <<
                    sysSeg->ts << ", SPARE1: " << sysSeg->spare1 << ")");
            oracleAnalyzer->schema->sysSegMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysSegMapKey.erase(sysSegKey);
            delete sysSeg;

        } else if (object->systemTable == TABLE_SYS_TAB) {
            if (oracleAnalyzer->schema->sysTabMapRowId.find(rowId) == oracleAnalyzer->schema->sysTabMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTab *sysTab = oracleAnalyzer->schema->sysTabMapRowId[rowId];
            if (oracleAnalyzer->schema->sysTabMapObj.find(sysTab->obj) == oracleAnalyzer->schema->sysTabMapObj.end()) {
                RUNTIME_FAIL("DDL: missing SYS.TAB$: (OBJ#: " << sysTab->obj << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTab->obj << ", DATAOBJ#: " << sysTab->dataObj << ", TS#: " <<
                    sysTab->ts << ", FILE#: " << sysTab->file << ", BLOCK#: " << sysTab->block << ", CLUCOLS: " << sysTab->cluCols << ", FLAGS: " <<
                    sysTab->flags << ", PROPERTY: " << sysTab->property << ")");
            oracleAnalyzer->schema->sysTabMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysTabMapObj.erase(sysTab->obj);
            delete sysTab;

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            if (oracleAnalyzer->schema->sysTabComPartMapRowId.find(rowId) == oracleAnalyzer->schema->sysTabComPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabComPart *sysTabComPart = oracleAnalyzer->schema->sysTabComPartMapRowId[rowId];
            SysTabComPartKey sysTabComPartKey(sysTabComPart->bo, sysTabComPart->obj);
            if (oracleAnalyzer->schema->sysTabComPartMapKey.find(sysTabComPartKey) == oracleAnalyzer->schema->sysTabComPartMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.TABCOMPART$: (BO#: " << sysTabComPart->bo << ", OBJ#: " << sysTabComPart->obj << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabComPart->obj << ", DATAOBJ#: " << sysTabComPart->dataObj << ", BO#: " <<
                    sysTabComPart->bo << ")");
            oracleAnalyzer->schema->sysTabComPartMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysTabComPartMapKey.erase(sysTabComPartKey);
            delete sysTabComPart;

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            if (oracleAnalyzer->schema->sysTabPartMapRowId.find(rowId) == oracleAnalyzer->schema->sysTabPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabPart *sysTabPart = oracleAnalyzer->schema->sysTabPartMapRowId[rowId];
            SysTabPartKey sysTabPartKey(sysTabPart->bo, sysTabPart->obj);
            if (oracleAnalyzer->schema->sysTabPartMapKey.find(sysTabPartKey) == oracleAnalyzer->schema->sysTabPartMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.TABPART$: (BO#: " << sysTabPart->bo << ", OBJ#: " << sysTabPart->obj << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabPart->obj << ", DATAOBJ#: " << sysTabPart->dataObj << ", BO#: " <<
                    sysTabPart->bo << ")");
            oracleAnalyzer->schema->sysTabPartMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysTabPartMapKey.erase(sysTabPartKey);
            delete sysTabPart;

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            if (oracleAnalyzer->schema->sysTabSubPartMapRowId.find(rowId) == oracleAnalyzer->schema->sysTabSubPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabSubPart *sysTabSubPart = oracleAnalyzer->schema->sysTabSubPartMapRowId[rowId];
            SysTabSubPartKey sysTabSubPartKey(sysTabSubPart->pObj, sysTabSubPart->obj);
            if (oracleAnalyzer->schema->sysTabSubPartMapKey.find(sysTabSubPartKey) == oracleAnalyzer->schema->sysTabSubPartMapKey.end()) {
                RUNTIME_FAIL("DDL: missing SYS.TABSUBPART$: (POBJ#: " << sysTabSubPart->pObj << ", OBJ#: " << sysTabSubPart->obj << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabSubPart->obj << ", DATAOBJ#: " << sysTabSubPart->dataObj << ", POBJ#: " <<
                    sysTabSubPart->pObj << ")");
            oracleAnalyzer->schema->sysTabSubPartMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysTabSubPartMapKey.erase(sysTabSubPartKey);
            delete sysTabSubPart;

        } else if (object->systemTable == TABLE_SYS_USER) {
            if (oracleAnalyzer->schema->sysUserMapRowId.find(rowId) == oracleAnalyzer->schema->sysUserMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysUser *sysUser = oracleAnalyzer->schema->sysUserMapRowId[rowId];
            if (oracleAnalyzer->schema->sysUserMapUser.find(sysUser->user) == oracleAnalyzer->schema->sysUserMapUser.end()) {
                RUNTIME_FAIL("DDL: missing SYS.USER$: (USER#: " << sysUser->user << ") for delete");
            }

            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (USER#: " << dec << sysUser->user << ", NAME: " << sysUser->name << ", SPARE1: " <<
                    sysUser->spare1 << ")");
            oracleAnalyzer->schema->sysUserMapRowId.erase(rowId);
            oracleAnalyzer->schema->sysUserMapUser.erase(sysUser->user);
            delete sysUser;
        }
    }

    void SystemTransaction::commit(void) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: commit");
    }
}
