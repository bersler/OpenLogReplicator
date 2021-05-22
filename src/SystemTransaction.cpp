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

    SystemTransaction::SystemTransaction(OracleAnalyzer *oracleAnalyzer, OutputBuffer *outputBuffer, Schema *schema) :
                oracleAnalyzer(oracleAnalyzer),
                outputBuffer(outputBuffer),
                schema(schema) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: begin");
    }

    SystemTransaction::~SystemTransaction() {
    }

    bool SystemTransaction::updateNumber16(int16_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr && outputBuffer->values[pos][VALUE_AFTER].length[0] > 0) {
            char *retPtr;
            if (object->columns[i]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " type found " << object->columns[i]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[pos][VALUE_AFTER].data[0], outputBuffer->values[pos][VALUE_AFTER].length[0]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            int16_t newVal = strtol(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> " << newVal << ")");
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> NULL)");
                val = 0;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber16u(uint16_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
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
            uint16_t newVal = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> " << newVal << ")");
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> NULL)");
                val = 0;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber32u(uint32_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
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
            uint32_t newVal = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> " << newVal << ")");
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> NULL)");
                val = 0;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateObj(typeOBJ &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
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
            typeOBJ newVal = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> " << newVal << ")");
                schema->touchObj(val);
                schema->touchObj(newVal);
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> NULL)");
                schema->touchObj(val);
                val = 0;
                return true;
            }
        }
        return false;
    }


    bool SystemTransaction::updateUser(typeUSER &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
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
            typeUSER newVal = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> " << newVal << ")");
                schema->touchUser(val);
                schema->touchUser(newVal);
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> NULL)");
                schema->touchUser(val);
                val = 0;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber64(int64_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
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
            int64_t newVal = strtol(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> " << newVal << ")");
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> NULL)");
                val = 0;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber64u(uint64_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
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
            uint64_t newVal = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> " << newVal << ")");
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> NULL)");
                val = 0;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumberXu(uintX_t &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId, uint64_t mask64) {
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
            uintX_t newVal(0);
            newVal.setStr(outputBuffer->valueBuffer, outputBuffer->valueLength);
            if ((newVal.get64() & mask64) != (val.get64() & mask64)) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> " << newVal << ")");
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            if (!val.isZero()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": " << dec << val << " -> NULL)");
                val.set(0, 0);
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateString(string &val, uint16_t i, uint16_t pos, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr && outputBuffer->values[pos][VALUE_AFTER].length[0] > 0) {
            char *retPtr;
            if (object->columns[i]->typeNo != 1 && object->columns[i]->typeNo != 96) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[i]->name << " type found " << object->columns[i]->typeNo);
            }
            outputBuffer->parseString(outputBuffer->values[pos][VALUE_AFTER].data[0], outputBuffer->values[pos][VALUE_AFTER].length[0], object->columns[i]->charsetId);
            string newVal(outputBuffer->valueBuffer, outputBuffer->valueLength);
            if (val.compare(newVal) != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": '" << val << "' -> '" << newVal << "')");
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[pos][VALUE_AFTER].data[0] != nullptr || outputBuffer->values[pos][VALUE_BEFORE].data[0] != nullptr) {
            if (val.length() > 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[i]->name << ": '" << val << "' -> NULL)");
                val.assign("");
                return true;
            }
        }
        return false;
    }

    void SystemTransaction::processInsert(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        RowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);

        TRACE(TRACE2_SYSTEM, "SYSTEM: insert table (name: " << object->owner << "." << object->name << ", rowid: " << rowId << ")");

        if (object->systemTable == TABLE_SYS_CCOL) {
            if (schema->sysCColMapRowId.find(rowId) != schema->sysCColMapRowId.end()) {
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
                    updateObj(sysCCol->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SPARE1") == 0)
                    updateNumberXu(sysCCol->spare1, i, pos, object, rowId, SYSCOL_SPARE1_MASK);
            }

            schema->sysCColMapRowId[rowId] = sysCCol;
            schema->sysCColKeyTouched = true;
            schema->touched = true;

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            if (schema->sysCDefMapRowId.find(rowId) != schema->sysCDefMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEF$: (rowid: " << rowId << ") for insert");
            }
            SysCDef *sysCDef = new SysCDef(rowId, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("CON#") == 0)
                    updateNumber32u(sysCDef->con, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateObj(sysCDef->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TYPE#") == 0)
                    updateNumber16u(sysCDef->type, i, pos, object, rowId);
            }

            schema->sysCDefMapRowId[rowId] = sysCDef;
            schema->sysCDefKeyTouched = true;
            schema->sysCDefConTouched = true;
            schema->touched = true;

        } else if (object->systemTable == TABLE_SYS_COL) {
            if (schema->sysColMapRowId.find(rowId) != schema->sysColMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.COL$: (rowid: " << rowId << ")for insert");
            }
            SysCol *sysCol = new SysCol(rowId, 0, 0, 0, 0, "", 0, 0, -1, -1, 0, 0, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateObj(sysCol->obj, i, pos, object, rowId);
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
                    updateNumberXu(sysCol->property, i, pos, object, rowId, SYSCOL_PROPERTY_MASK);
            }

            schema->sysColMapRowId[rowId] = sysCol;
            schema->sysColKeyTouched = true;
            schema->sysColSegTouched = true;
            schema->touched = true;

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            if (schema->sysDeferredStgMapRowId.find(rowId) != schema->sysDeferredStgMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEFERRED_STG$: (rowid: " << rowId << ") for insert");
            }
            SysDeferredStg *sysDeferredStg = new SysDeferredStg(rowId, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    updateObj(sysDeferredStg->obj, i, pos, object, rowId);
                } else if (object->columns[i]->name.compare("FLAGS_STG") == 0)
                    updateNumberXu(sysDeferredStg->flagsStg, i, pos, object, rowId, SYSDEFERREDSTG_FLAGSSTG_MASK);
            }

            schema->sysDeferredStgMapRowId[rowId] = sysDeferredStg;
            schema->sysDeferredStgObjTouched = true;
            schema->touched = true;
            schema->touchObj(sysDeferredStg->obj);

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            if (schema->sysEColMapRowId.find(rowId) != schema->sysEColMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.ECOL$: (rowid: " << rowId << ") for insert");
            }
            SysECol *sysECol = new SysECol(rowId, 0, 0, -1, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("TABOBJ#") == 0)
                    updateObj(sysECol->tabObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("COLNUM") == 0)
                    updateNumber16(sysECol->colNum, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("GUARD_ID") == 0)
                    updateNumber16(sysECol->guardId, i, pos, object, rowId);
            }

            schema->sysEColMapRowId[rowId] = sysECol;
            schema->sysEColKeyTouched = true;
            schema->touched = true;

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            SysObj *sysObj = schema->sysObjMapRowId[rowId];
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
                    updateObj(sysObj->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysObj->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("NAME") == 0)
                    updateString(sysObj->name, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("TYPE#") == 0)
                    updateNumber16u(sysObj->type, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("FLAGS") == 0)
                    updateNumberXu(sysObj->flags, i, pos, object, rowId, SYSOBJ_FLAGS_MASK);
            }

            schema->sysObjMapRowId[rowId] = sysObj;
            schema->sysObjObjTouched = true;
            schema->touched = true;

        } else if (object->systemTable == TABLE_SYS_SEG) {
            if (schema->sysSegMapRowId.find(rowId) != schema->sysSegMapRowId.end()) {
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
                    updateNumberXu(sysSeg->spare1, i, pos, object, rowId, SYSSEG_SPARE1_MASK);
            }

            schema->sysSegMapRowId[rowId] = sysSeg;
            schema->sysSegKeyTouched = true;
            schema->touched = true;

        } else if (object->systemTable == TABLE_SYS_TAB) {
            if (schema->sysTabMapRowId.find(rowId) != schema->sysTabMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TAB$: (rowid: " << rowId << ") for insert");
            }
            SysTab *sysTab = new SysTab(rowId, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateObj(sysTab->obj, i, pos, object, rowId);
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
                    updateNumberXu(sysTab->flags, i, pos, object, rowId, SYSTAB_FLAGS_MASK);
                else if (object->columns[i]->name.compare("PROPERTY") == 0)
                    updateNumberXu(sysTab->property, i, pos, object, rowId, SYSTAB_PROPERTY_MASK);
            }

            schema->sysTabMapRowId[rowId] = sysTab;
            schema->sysTabObjTouched = true;
            schema->sysTabKeyTouched = true;
            schema->touched = true;

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            if (schema->sysTabComPartMapRowId.find(rowId) != schema->sysTabComPartMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABCOMPART$: (rowid: " << rowId << ") for insert");
            }
            SysTabComPart *sysTabComPart = new SysTabComPart(rowId, 0, 0, 0, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0)
                    updateObj(sysTabComPart->obj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("DATAOBJ#") == 0)
                    updateNumber32u(sysTabComPart->dataObj, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("BO#") == 0)
                    updateNumber32u(sysTabComPart->bo, i, pos, object, rowId);
            }

            schema->sysTabComPartMapRowId[rowId] = sysTabComPart;
            schema->sysTabComPartKeyTouched = true;
            schema->touched = true;

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            if (schema->sysTabPartMapRowId.find(rowId) != schema->sysTabPartMapRowId.end()) {
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
                    updateObj(sysTabPart->bo, i, pos, object, rowId);
            }

            schema->sysTabPartMapRowId[rowId] = sysTabPart;
            schema->sysTabPartKeyTouched = true;
            schema->touched = true;

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            if (schema->sysTabSubPartMapRowId.find(rowId) != schema->sysTabSubPartMapRowId.end()) {
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
                    updateObj(sysTabSubPart->pObj, i, pos, object, rowId);
            }

            schema->sysTabSubPartMapRowId[rowId] = sysTabSubPart;
            schema->sysTabSubPartKeyTouched = true;
            schema->touched = true;

        } else if (object->systemTable == TABLE_SYS_USER) {
            if (schema->sysUserMapRowId.find(rowId) != schema->sysUserMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.USER$: (rowid: " << rowId << ") for insert");
            }
            SysUser *sysUser = new SysUser(rowId, 0, "", 0, 0, false, true);

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("USER#") == 0)
                    updateUser(sysUser->user, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("NAME") == 0)
                    updateString(sysUser->name, i, pos, object, rowId);
                else if (object->columns[i]->name.compare("SPARE1") == 0)
                    updateNumberXu(sysUser->spare1, i, pos, object, rowId, SYSUSER_SPARE1_MASK);
            }

            schema->sysUserMapRowId[rowId] = sysUser;
            schema->sysUserUserTouched = true;
            schema->touched = true;
            schema->touchUser(sysUser->user);
        }
    }

    void SystemTransaction::processUpdate(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        RowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        TRACE(TRACE2_SYSTEM, "SYSTEM: update table (name: " << object->owner << "." << object->name << ", rowid: " << rowId << ")");

        if (object->systemTable == TABLE_SYS_CCOL) {
            auto sysCColIt = schema->sysCColMapRowId.find(rowId);
            if (sysCColIt == schema->sysCColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCCol *sysCCol = sysCColIt->second;
            sysCCol->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("CON#") == 0) {
                    if (updateNumber32u(sysCCol->con, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysCColKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("INTCOL#") == 0) {
                    if (updateNumber16(sysCCol->intCol, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysCColKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("OBJ#") == 0) {
                    if (updateObj(sysCCol->obj, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysCColKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("SPARE1") == 0) {
                    if (updateNumberXu(sysCCol->spare1, i, pos, object, rowId, SYSCOL_SPARE1_MASK))
                        schema->touched = true;
                }
            }

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            auto sysCDefIt = schema->sysCDefMapRowId.find(rowId);
            if (sysCDefIt == schema->sysCDefMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCDef *sysCDef = sysCDefIt->second;
            sysCDef->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("CON#") == 0) {
                    if (updateNumber32u(sysCDef->con, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysCDefKeyTouched = true;
                        schema->sysCDefConTouched = true;
                    }
                } else if (object->columns[i]->name.compare("OBJ#") == 0) {
                    if (updateObj(sysCDef->obj, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysCDefKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("TYPE#") == 0) {
                    if (updateNumber16u(sysCDef->type, i, pos, object, rowId))
                        schema->touched = true;
                }
            }

        } else if (object->systemTable == TABLE_SYS_COL) {
            auto sysColIt = schema->sysColMapRowId.find(rowId);
            if (sysColIt == schema->sysColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCol *sysCol = sysColIt->second;
            sysCol->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    if (updateObj(sysCol->obj, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysColKeyTouched = true;
                        schema->sysColSegTouched = true;
                    }
                } else if (object->columns[i]->name.compare("COL#") == 0) {
                    if (updateNumber16(sysCol->col, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("SEGCOL#") == 0) {
                    if (updateNumber16(sysCol->segCol, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysColSegTouched = true;
                    }
                } else if (object->columns[i]->name.compare("INTCOL#") == 0) {
                    if (updateNumber16(sysCol->intCol, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysColKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("NAME") == 0) {
                    if (updateString(sysCol->name, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("TYPE#") == 0) {
                    if (updateNumber16u(sysCol->type, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("LENGTH") == 0) {
                    if (updateNumber64u(sysCol->length, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("PRECISION#") == 0) {
                    if (updateNumber64(sysCol->precision, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("SCALE") == 0) {
                    if (updateNumber64(sysCol->scale, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("CHARSETFORM") == 0) {
                    if (updateNumber64u(sysCol->charsetForm, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("CHARSETID") == 0) {
                    if (updateNumber64u(sysCol->charsetId, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("NULL$") == 0) {
                    if (updateNumber64(sysCol->null_, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("PROPERTY") == 0) {
                    if (updateNumberXu(sysCol->property, i, pos, object, rowId, SYSCOL_PROPERTY_MASK))
                        schema->touched = true;
                }
            }

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            auto sysDeferredStgIt = schema->sysDeferredStgMapRowId.find(rowId);
            if (sysDeferredStgIt == schema->sysDeferredStgMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysDeferredStg *sysDeferredStg = sysDeferredStgIt->second;
            sysDeferredStg->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    if (updateObj(sysDeferredStg->obj, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysDeferredStgObjTouched = true;
                    }
                } else if (object->columns[i]->name.compare("FLAGS_STG") == 0) {
                    if (updateNumberXu(sysDeferredStg->flagsStg, i, pos, object, rowId, SYSDEFERREDSTG_FLAGSSTG_MASK))
                        schema->touched = true;
                }
            }

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            auto sysEColIt = schema->sysEColMapRowId.find(rowId);
            if (sysEColIt == schema->sysEColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysECol *sysECol = sysEColIt->second;
            sysECol->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("TABOBJ#") == 0) {
                    if (updateObj(sysECol->tabObj, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysEColKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("COLNUM") == 0) {
                    if (updateNumber16(sysECol->colNum, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysEColKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("GUARD_ID") == 0) {
                    if (updateNumber16(sysECol->guardId, i, pos, object, rowId))
                        schema->touched = true;
                }
            }

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            auto sysObjIt = schema->sysObjMapRowId.find(rowId);
            if (sysObjIt == schema->sysObjMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysObj *sysObj = sysObjIt->second;
            sysObj->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OWNER#") == 0) {
                    if (updateNumber32u(sysObj->owner, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("OBJ#") == 0) {
                    if (updateObj(sysObj->obj, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysObjObjTouched = true;
                    }
                } else if (object->columns[i]->name.compare("DATAOBJ#") == 0) {
                    if (updateNumber32u(sysObj->dataObj, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("NAME") == 0) {
                    if (updateString(sysObj->name, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("TYPE#") == 0) {
                    if (updateNumber16u(sysObj->type, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("FLAGS") == 0) {
                    if (updateNumberXu(sysObj->flags, i, pos, object, rowId, SYSOBJ_FLAGS_MASK))
                        schema->touched = true;
                }
            }

        } else if (object->systemTable == TABLE_SYS_SEG) {
            auto sysSegIt = schema->sysSegMapRowId.find(rowId);
            if (sysSegIt == schema->sysSegMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysSeg *sysSeg = sysSegIt->second;
            sysSeg->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("FILE#") == 0) {
                    if (updateNumber32u(sysSeg->file, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysSegKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("BLOCK#") == 0) {
                    if (updateNumber32u(sysSeg->block, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysSegKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("TS#") == 0) {
                    if (updateNumber32u(sysSeg->ts, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysSegKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("SPARE1") == 0) {
                    if (updateNumberXu(sysSeg->spare1, i, pos, object, rowId, SYSSEG_SPARE1_MASK))
                        schema->touched = true;
                }
            }

        } else if (object->systemTable == TABLE_SYS_TAB) {
            auto sysTabIt = schema->sysTabMapRowId.find(rowId);
            if (sysTabIt == schema->sysTabMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTab *sysTab = sysTabIt->second;
            sysTab->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    if (updateObj(sysTab->obj, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysTabObjTouched = true;
                    }
                } else if (object->columns[i]->name.compare("DATAOBJ#") == 0) {
                    if (updateNumber32u(sysTab->dataObj, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("TS#") == 0) {
                    if (updateNumber32u(sysTab->ts, i, pos, object, rowId)) {
                        schema->sysTabKeyTouched = true;
                        schema->touched = true;
                    }
                } else if (object->columns[i]->name.compare("FILE#") == 0) {
                    if (updateNumber32u(sysTab->file, i, pos, object, rowId)) {
                        schema->sysTabKeyTouched = true;
                        schema->touched = true;
                    }
                } else if (object->columns[i]->name.compare("BLOCK#") == 0) {
                    if (updateNumber32u(sysTab->block, i, pos, object, rowId)) {
                        schema->sysTabKeyTouched = true;
                        schema->touched = true;
                    }
                } else if (object->columns[i]->name.compare("CLUCOLS") == 0) {
                    if (updateNumber16(sysTab->cluCols, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("FLAGS") == 0) {
                    if (updateNumberXu(sysTab->flags, i, pos, object, rowId, SYSTAB_FLAGS_MASK))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("PROPERTY") == 0) {
                    if (updateNumberXu(sysTab->property, i, pos, object, rowId, SYSTAB_PROPERTY_MASK))
                        schema->touched = true;
                }
            }

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            auto sysTabComPartIt = schema->sysTabComPartMapRowId.find(rowId);
            if (sysTabComPartIt == schema->sysTabComPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabComPart *sysTabComPart = sysTabComPartIt->second;
            sysTabComPart->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    if (updateNumber32u(sysTabComPart->obj, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysTabComPartKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("DATAOBJ#") == 0) {
                    if (updateNumber32u(sysTabComPart->dataObj, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("BO#") == 0) {
                    if (updateObj(sysTabComPart->bo, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysTabComPartKeyTouched = true;
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            auto sysTabPartIt = schema->sysTabPartMapRowId.find(rowId);
            if (sysTabPartIt == schema->sysTabPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabPart *sysTabPart = sysTabPartIt->second;
            sysTabPart->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    if (updateNumber32u(sysTabPart->obj, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysTabPartKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("DATAOBJ#") == 0) {
                    if (updateNumber32u(sysTabPart->dataObj, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("BO#") == 0) {
                    if (updateObj(sysTabPart->bo, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysTabPartKeyTouched = true;
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            auto sysTabSubPartIt = schema->sysTabSubPartMapRowId.find(rowId);
            if (sysTabSubPartIt == schema->sysTabSubPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabSubPart *sysTabSubPart = sysTabSubPartIt->second;
            sysTabSubPart->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("OBJ#") == 0) {
                    if (updateNumber32u(sysTabSubPart->obj, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysTabSubPartKeyTouched = true;
                    }
                } else if (object->columns[i]->name.compare("DATAOBJ#") == 0) {
                    if (updateNumber32u(sysTabSubPart->dataObj, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("POBJ#") == 0) {
                    if (updateObj(sysTabSubPart->pObj, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysTabSubPartKeyTouched = true;
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_USER) {
            auto sysUserIt = schema->sysUserMapRowId.find(rowId);
            if (sysUserIt == schema->sysUserMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysUser *sysUser = sysUserIt->second;
            sysUser->touched = true;

            for (auto it = outputBuffer->valuesMap.cbegin(); it != outputBuffer->valuesMap.cend(); ++it) {
                uint16_t i = (*it).first;
                uint16_t pos = (*it).second;

                if (object->columns[i]->name.compare("USER#") == 0) {
                    if (updateUser(sysUser->user, i, pos, object, rowId)) {
                        schema->touched = true;
                        schema->sysUserUserTouched = true;
                    }
                } else if (object->columns[i]->name.compare("NAME") == 0) {
                    if (updateString(sysUser->name, i, pos, object, rowId))
                        schema->touched = true;
                } else if (object->columns[i]->name.compare("SPARE1") == 0) {
                    if (updateNumberXu(sysUser->spare1, i, pos, object, rowId, SYSUSER_SPARE1_MASK))
                        schema->touched = true;
                }
            }
        }
    }

    void SystemTransaction::processDelete(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        RowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete table (name: " << object->owner << "." << object->name << ", rowid: " << rowId << ")");

        if (object->systemTable == TABLE_SYS_CCOL) {
            if (schema->sysCColMapRowId.find(rowId) == schema->sysCColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCCol *sysCCol = schema->sysCColMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (CON#: " << dec << sysCCol->con << ", INTCOL#: " << sysCCol->intCol << ", OBJ#: " <<
                    sysCCol->obj << ", SPARE1: " << sysCCol->spare1 << ")");
            schema->sysCColMapRowId.erase(rowId);
            schema->sysCColKeyTouched = true;
            schema->touched = true;
            schema->touchObj(sysCCol->obj);
            delete sysCCol;

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            if (schema->sysCDefMapRowId.find(rowId) == schema->sysCDefMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCDef *sysCDef = schema->sysCDefMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (CON#: " << dec << sysCDef->con << ", OBJ#: " << sysCDef->obj << ", type: " << sysCDef->type << ")");
            schema->sysCDefMapRowId.erase(rowId);
            schema->sysCDefKeyTouched = true;
            schema->sysCDefConTouched = true;
            schema->touched = true;
            schema->touchObj(sysCDef->obj);
            delete sysCDef;

        } else if (object->systemTable == TABLE_SYS_COL) {
            if (schema->sysColMapRowId.find(rowId) == schema->sysColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCol *sysCol = schema->sysColMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysCol->obj << ", COL#: " << sysCol->col << ", SEGCOL#: " << sysCol->segCol <<
                    ", INTCOL#: " << sysCol->intCol << ", NAME: '" << sysCol->name << "', TYPE#: " << sysCol->type << ", LENGTH: " << sysCol->length <<
                    ", PRECISION#: " << sysCol->precision << ", SCALE: " << sysCol->scale << ", CHARSETFORM: " << sysCol->charsetForm <<
                    ", CHARSETID: " << sysCol->charsetId << ", NULL$: " << sysCol->null_ << ", PROPERTY: " << sysCol->property << ")");
            schema->sysColMapRowId.erase(rowId);
            schema->sysColKeyTouched = true;
            schema->sysColSegTouched = true;
            schema->touched = true;
            schema->touchObj(sysCol->obj);
            delete sysCol;

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            if (schema->sysDeferredStgMapRowId.find(rowId) == schema->sysDeferredStgMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysDeferredStg *sysDeferredStg = schema->sysDeferredStgMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysDeferredStg->obj << ", FLAGS_STG: " << sysDeferredStg->flagsStg << ")");
            schema->sysDeferredStgMapRowId.erase(rowId);
            schema->sysDeferredStgObjTouched = true;
            schema->touched = true;
            schema->touchObj(sysDeferredStg->obj);
            delete sysDeferredStg;

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            if (schema->sysEColMapRowId.find(rowId) == schema->sysEColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysECol *sysECol = schema->sysEColMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (TABOBJ#: " << dec << sysECol->tabObj << ", COLNUM: " << sysECol->colNum << ", GUARD_ID: " <<
                    sysECol->guardId << ")");
            schema->sysEColMapRowId.erase(rowId);
            schema->sysEColKeyTouched = true;
            schema->touched = true;
            schema->touchObj(sysECol->tabObj);
            delete sysECol;

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            if (schema->sysObjMapRowId.find(rowId) == schema->sysObjMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysObj *sysObj = schema->sysObjMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OWNER#: " << dec << sysObj->owner << ", OBJ#: " << sysObj->obj << ", DATAOBJ#: " <<
                    sysObj->dataObj << ", TYPE#: " << sysObj->type << ", NAME: '" << sysObj->name << "', FLAGS: " << sysObj->flags << ")");
            schema->sysObjMapRowId.erase(rowId);
            schema->sysObjObjTouched = true;
            schema->touched = true;
            schema->touchObj(sysObj->obj);
            delete sysObj;

        } else if (object->systemTable == TABLE_SYS_SEG) {
            if (schema->sysSegMapRowId.find(rowId) == schema->sysSegMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysSeg *sysSeg = schema->sysSegMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (FILE#: " << dec << sysSeg->file << ", BLOCK#: " << sysSeg->block << ", TS#: " <<
                    sysSeg->ts << ", SPARE1: " << sysSeg->spare1 << ")");
            schema->sysSegMapRowId.erase(rowId);
            schema->sysSegKeyTouched = true;
            schema->touched = true;
            //FIXME: schema->touchObj(sysCCol->obj);
            delete sysSeg;

        } else if (object->systemTable == TABLE_SYS_TAB) {
            if (schema->sysTabMapRowId.find(rowId) == schema->sysTabMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTab *sysTab = schema->sysTabMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTab->obj << ", DATAOBJ#: " << sysTab->dataObj << ", TS#: " <<
                    sysTab->ts << ", FILE#: " << sysTab->file << ", BLOCK#: " << sysTab->block << ", CLUCOLS: " << sysTab->cluCols << ", FLAGS: " <<
                    sysTab->flags << ", PROPERTY: " << sysTab->property << ")");
            schema->sysTabMapRowId.erase(rowId);
            schema->sysTabObjTouched = true;
            schema->sysTabKeyTouched = true;
            schema->touched = true;
            schema->touchObj(sysTab->obj);
            delete sysTab;

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            if (schema->sysTabComPartMapRowId.find(rowId) == schema->sysTabComPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabComPart *sysTabComPart = schema->sysTabComPartMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabComPart->obj << ", DATAOBJ#: " << sysTabComPart->dataObj << ", BO#: " <<
                    sysTabComPart->bo << ")");
            schema->sysTabComPartMapRowId.erase(rowId);
            schema->sysTabComPartKeyTouched = true;
            schema->touched = true;
            schema->touchObj(sysTabComPart->bo);
            delete sysTabComPart;

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            if (schema->sysTabPartMapRowId.find(rowId) == schema->sysTabPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabPart *sysTabPart = schema->sysTabPartMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabPart->obj << ", DATAOBJ#: " << sysTabPart->dataObj << ", BO#: " <<
                    sysTabPart->bo << ")");
            schema->sysTabPartMapRowId.erase(rowId);
            schema->sysTabPartKeyTouched = true;
            schema->touched = true;
            schema->touchObj(sysTabPart->bo);
            delete sysTabPart;

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            if (schema->sysTabSubPartMapRowId.find(rowId) == schema->sysTabSubPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabSubPart *sysTabSubPart = schema->sysTabSubPartMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabSubPart->obj << ", DATAOBJ#: " << sysTabSubPart->dataObj << ", POBJ#: " <<
                    sysTabSubPart->pObj << ")");
            schema->sysTabSubPartMapRowId.erase(rowId);
            schema->sysTabSubPartKeyTouched = true;
            schema->touched = true;
            schema->touchObj(sysTabSubPart->pObj);
            delete sysTabSubPart;

        } else if (object->systemTable == TABLE_SYS_USER) {
            if (schema->sysUserMapRowId.find(rowId) == schema->sysUserMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysUser *sysUser = schema->sysUserMapRowId[rowId];
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (USER#: " << dec << sysUser->user << ", NAME: " << sysUser->name << ", SPARE1: " <<
                    sysUser->spare1 << ")");
            schema->sysUserMapRowId.erase(rowId);
            schema->sysUserUserTouched = true;
            schema->touched = true;
            schema->touchUser(sysUser->user);
            delete sysUser;
        }
    }

    void SystemTransaction::commit(void) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: commit");

        if (!schema->touched)
            return;

        schema->refreshIndexes();
        schema->rebuildMaps(oracleAnalyzer);
    }
}
