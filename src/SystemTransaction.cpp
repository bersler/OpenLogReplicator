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

    bool SystemTransaction::updateNumber16(int16_t &val, int16_t defVal, typeCOL column, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr && outputBuffer->lengths[column][VALUE_AFTER] > 0) {
            char *retPtr;
            if (object->columns[column]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " type found " << object->columns[column]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[column][VALUE_AFTER], outputBuffer->lengths[column][VALUE_AFTER]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            int16_t newVal = strtol(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> " << newVal << ")");
                schema->touched = true;
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr || outputBuffer->values[column][VALUE_BEFORE] != nullptr) {
            if (val != defVal) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> NULL)");
                schema->touched = true;
                val = defVal;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber16u(uint16_t &val, uint16_t defVal, typeCOL column, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr && outputBuffer->lengths[column][VALUE_AFTER] > 0) {
            char *retPtr;
            if (object->columns[column]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " type found " << object->columns[column]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[column][VALUE_AFTER], outputBuffer->lengths[column][VALUE_AFTER]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0 || (outputBuffer->valueLength > 0 && outputBuffer->valueBuffer[0] == '-')) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " value found " << outputBuffer->valueBuffer);
            }
            uint16_t newVal = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> " << newVal << ")");
                schema->touched = true;
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr || outputBuffer->values[column][VALUE_BEFORE] != nullptr) {
            if (val != defVal) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> NULL)");
                schema->touched = true;
                val = defVal;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber32u(uint32_t &val, uint32_t defVal, typeCOL column, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr && outputBuffer->lengths[column][VALUE_AFTER] > 0) {
            char *retPtr;
            if (object->columns[column]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " type found " << object->columns[column]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[column][VALUE_AFTER], outputBuffer->lengths[column][VALUE_AFTER]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0 || (outputBuffer->valueLength > 0 && outputBuffer->valueBuffer[0] == '-')) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " value found " << outputBuffer->valueBuffer);
            }
            uint32_t newVal = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> " << newVal << ")");
                schema->touched = true;
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr || outputBuffer->values[column][VALUE_BEFORE] != nullptr) {
            if (val != defVal) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> NULL)");
                schema->touched = true;
                val = defVal;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateObj(typeOBJ &val, typeCOL column, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr && outputBuffer->lengths[column][VALUE_AFTER] > 0) {
            char *retPtr;
            if (object->columns[column]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " type found " << object->columns[column]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[column][VALUE_AFTER], outputBuffer->lengths[column][VALUE_AFTER]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0 || (outputBuffer->valueLength > 0 && outputBuffer->valueBuffer[0] == '-')) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " value found " << outputBuffer->valueBuffer);
            }
            typeOBJ newVal = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> " << newVal << ")");
                schema->touched = true;
                schema->touchObj(val);
                schema->touchObj(newVal);
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr || outputBuffer->values[column][VALUE_BEFORE] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> NULL)");
                schema->touched = true;
                schema->touchObj(val);
                val = 0;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateUser(typeUSER &val, typeCOL column, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr && outputBuffer->lengths[column][VALUE_AFTER] > 0) {
            char *retPtr;
            if (object->columns[column]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " type found " << object->columns[column]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[column][VALUE_AFTER], outputBuffer->lengths[column][VALUE_AFTER]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0 || (outputBuffer->valueLength > 0 && outputBuffer->valueBuffer[0] == '-')) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " value found " << outputBuffer->valueBuffer);
            }
            typeUSER newVal = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> " << newVal << ")");
                schema->touched = true;
                schema->touchUser(val);
                schema->touchUser(newVal);
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr || outputBuffer->values[column][VALUE_BEFORE] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> NULL)");
                schema->touched = true;
                schema->touchUser(val);
                val = 0;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber64(int64_t &val, int64_t defVal, typeCOL column, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr && outputBuffer->lengths[column][VALUE_AFTER] > 0) {
            char *retPtr;
            if (object->columns[column]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " type found " << object->columns[column]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[column][VALUE_AFTER], outputBuffer->lengths[column][VALUE_AFTER]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " value found " << outputBuffer->valueBuffer);
            }
            int64_t newVal = strtol(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> " << newVal << ")");
                schema->touched = true;
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr || outputBuffer->values[column][VALUE_BEFORE] != nullptr) {
            if (val != defVal) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> NULL)");
                schema->touched = true;
                val = defVal;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber64u(uint64_t &val, uint64_t defVal, typeCOL column, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr && outputBuffer->lengths[column][VALUE_AFTER] > 0) {
            char *retPtr;
            if (object->columns[column]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " type found " << object->columns[column]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[column][VALUE_AFTER], outputBuffer->lengths[column][VALUE_AFTER]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0 || (outputBuffer->valueLength > 0 && outputBuffer->valueBuffer[0] == '-')) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " value found " << outputBuffer->valueBuffer);
            }
            uint64_t newVal = strtoul(outputBuffer->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> " << newVal << ")");
                schema->touched = true;
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr || outputBuffer->values[column][VALUE_BEFORE] != nullptr) {
            if (val != defVal) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> NULL)");
                schema->touched = true;
                val = defVal;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumberXu(uintX_t &val, typeCOL column, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr && outputBuffer->lengths[column][VALUE_AFTER] > 0) {
            char *retPtr;
            if (object->columns[column]->typeNo != 2) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " type found " << object->columns[column]->typeNo);
            }
            outputBuffer->parseNumber(outputBuffer->values[column][VALUE_AFTER], outputBuffer->lengths[column][VALUE_AFTER]);
            outputBuffer->valueBuffer[outputBuffer->valueLength] = 0;
            if (outputBuffer->valueLength == 0 || (outputBuffer->valueLength > 0 && outputBuffer->valueBuffer[0] == '-')) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " value found " << outputBuffer->valueBuffer);
            }
            uintX_t newVal(0);
            newVal.setStr(outputBuffer->valueBuffer, outputBuffer->valueLength);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> " << newVal << ")");
                schema->touched = true;
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr || outputBuffer->values[column][VALUE_BEFORE] != nullptr) {
            if (!val.isZero()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << dec << val << " -> NULL)");
                schema->touched = true;
                val.set(0, 0);
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateString(string &val, typeCOL column, OracleObject *object, RowId &rowId) {
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr && outputBuffer->lengths[column][VALUE_AFTER] > 0) {
            char *retPtr;
            if (object->columns[column]->typeNo != 1 && object->columns[column]->typeNo != 96) {
                RUNTIME_FAIL("ddl: column type mismatch for " << object->owner << "." << object->name << ": column " << object->columns[column]->name << " type found " << object->columns[column]->typeNo);
            }
            outputBuffer->parseString(outputBuffer->values[column][VALUE_AFTER], outputBuffer->lengths[column][VALUE_AFTER], object->columns[column]->charsetId);
            string newVal(outputBuffer->valueBuffer, outputBuffer->valueLength);
            if (val.compare(newVal) != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": '" << val << "' -> '" << newVal << "')");
                schema->touched = true;
                val = newVal;
                return true;
            }
        } else
        if (outputBuffer->values[column][VALUE_AFTER] != nullptr || outputBuffer->values[column][VALUE_BEFORE] != nullptr) {
            if (val.length() > 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": '" << val << "' -> NULL)");
                schema->touched = true;
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
            if (sysCCol == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysCCol) << " bytes memory (for: SysCCol)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("CON#") == 0)
                        updateNumber32u(sysCCol->con, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("INTCOL#") == 0)
                        updateNumber16(sysCCol->intCol, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("OBJ#") == 0)
                        updateObj(sysCCol->obj, column, object, rowId);
                    else if (object->columns[column]->name.compare("SPARE1") == 0)
                        updateNumberXu(sysCCol->spare1, column, object, rowId);
                }
            }

            schema->sysCColMapRowId[rowId] = sysCCol;
            schema->sysCColTouched = true;

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            if (schema->sysCDefMapRowId.find(rowId) != schema->sysCDefMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEF$: (rowid: " << rowId << ") for insert");
            }
            SysCDef *sysCDef = new SysCDef(rowId, 0, 0, 0, true);
            if (sysCDef == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysCDef) << " bytes memory (for: SysCDef)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("CON#") == 0)
                        updateNumber32u(sysCDef->con, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("OBJ#") == 0)
                        updateObj(sysCDef->obj, column, object, rowId);
                    else if (object->columns[column]->name.compare("TYPE#") == 0)
                        updateNumber16u(sysCDef->type, 0, column, object, rowId);
                }
            }

            schema->sysCDefMapRowId[rowId] = sysCDef;
            schema->sysCDefTouched = true;

        } else if (object->systemTable == TABLE_SYS_COL) {
            if (schema->sysColMapRowId.find(rowId) != schema->sysColMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.COL$: (rowid: " << rowId << ")for insert");
            }
            SysCol *sysCol = new SysCol(rowId, 0, 0, 0, 0, "", 0, 0, -1, -1, 0, 0, 0, 0, 0, true);
            if (sysCol == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysCol) << " bytes memory (for: SysCol)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0)
                        updateObj(sysCol->obj, column, object, rowId);
                    else if (object->columns[column]->name.compare("COL#") == 0)
                        updateNumber16(sysCol->col, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("SEGCOL#") == 0)
                        updateNumber16(sysCol->segCol, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("INTCOL#") == 0)
                        updateNumber16(sysCol->intCol, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("NAME") == 0)
                        updateString(sysCol->name, column, object, rowId);
                    else if (object->columns[column]->name.compare("TYPE#") == 0)
                        updateNumber16u(sysCol->type, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("LENGTH") == 0)
                        updateNumber64u(sysCol->length, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("PRECISION#") == 0)
                        updateNumber64(sysCol->precision, -1, column, object, rowId);
                    else if (object->columns[column]->name.compare("SCALE") == 0)
                        updateNumber64(sysCol->scale, -1, column, object, rowId);
                    else if (object->columns[column]->name.compare("CHARSETFORM") == 0)
                        updateNumber64u(sysCol->charsetForm, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("CHARSETID") == 0)
                        updateNumber64u(sysCol->charsetId, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("NULL$") == 0)
                        updateNumber64(sysCol->null_, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("PROPERTY") == 0)
                        updateNumberXu(sysCol->property, column, object, rowId);
                }
            }

            schema->sysColMapRowId[rowId] = sysCol;
            schema->sysColTouched = true;

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            if (schema->sysDeferredStgMapRowId.find(rowId) != schema->sysDeferredStgMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.DEFERRED_STG$: (rowid: " << rowId << ") for insert");
            }
            SysDeferredStg *sysDeferredStg = new SysDeferredStg(rowId, 0, 0, 0, true);
            if (sysDeferredStg == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysDeferredStg) << " bytes memory (for: SysDeferredStg)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0)
                        updateObj(sysDeferredStg->obj, column, object, rowId);
                    else if (object->columns[column]->name.compare("FLAGS_STG") == 0)
                        updateNumberXu(sysDeferredStg->flagsStg, column, object, rowId);
                }
            }

            schema->sysDeferredStgMapRowId[rowId] = sysDeferredStg;
            schema->sysDeferredStgTouched = true;
            schema->touchObj(sysDeferredStg->obj);

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            if (schema->sysEColMapRowId.find(rowId) != schema->sysEColMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.ECOL$: (rowid: " << rowId << ") for insert");
            }
            SysECol *sysECol = new SysECol(rowId, 0, 0, -1, true);
            if (sysECol == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysECol) << " bytes memory (for: SysECol)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("TABOBJ#") == 0)
                        updateObj(sysECol->tabObj, column, object, rowId);
                    else if (object->columns[column]->name.compare("COLNUM") == 0)
                        updateNumber16(sysECol->colNum, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("GUARD_ID") == 0)
                        updateNumber16(sysECol->guardId, -1, column, object, rowId);
                }
            }

            schema->sysEColMapRowId[rowId] = sysECol;
            schema->sysEColTouched = true;

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            SysObj *sysObj = schema->sysObjMapRowId[rowId];
            if (sysObj != nullptr) {
                RUNTIME_FAIL("DDL: duplicate SYS.OBJ$: (rowid: " << rowId << ") for insert");
            }
            sysObj = new SysObj(rowId, 0, 0, 0, 0, "", 0, 0, false, true);
            if (sysObj == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysObj) << " bytes memory (for: SysObj)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OWNER#") == 0)
                        updateNumber32u(sysObj->owner, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("OBJ#") == 0)
                        updateObj(sysObj->obj, column, object, rowId);
                    else if (object->columns[column]->name.compare("DATAOBJ#") == 0)
                        updateNumber32u(sysObj->dataObj, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("NAME") == 0)
                        updateString(sysObj->name, column, object, rowId);
                    else if (object->columns[column]->name.compare("TYPE#") == 0)
                        updateNumber16u(sysObj->type, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("FLAGS") == 0)
                        updateNumberXu(sysObj->flags, column, object, rowId);
                }
            }

            schema->sysObjMapRowId[rowId] = sysObj;
            schema->sysObjTouched = true;

        } else if (object->systemTable == TABLE_SYS_SEG) {
            if (schema->sysSegMapRowId.find(rowId) != schema->sysSegMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.SEG$: (rowid: " << rowId << ") for insert");
            }
            SysSeg *sysSeg = new SysSeg(rowId, 0, 0, 0, 0, 0, true);
            if (sysSeg == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysSeg) << " bytes memory (for: SysSeg)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("FILE#") == 0)
                        updateNumber32u(sysSeg->file, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("BLOCK#") == 0)
                        updateNumber32u(sysSeg->block, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("TS#") == 0)
                        updateNumber32u(sysSeg->ts, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("SPARE1") == 0)
                        updateNumberXu(sysSeg->spare1, column, object, rowId);
                }
            }

            schema->sysSegMapRowId[rowId] = sysSeg;
            schema->sysSegTouched = true;

        } else if (object->systemTable == TABLE_SYS_TAB) {
            if (schema->sysTabMapRowId.find(rowId) != schema->sysTabMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TAB$: (rowid: " << rowId << ") for insert");
            }
            SysTab *sysTab = new SysTab(rowId, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, true);
            if (sysTab == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysTab) << " bytes memory (for: SysTab)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0)
                        updateObj(sysTab->obj, column, object, rowId);
                    else if (object->columns[column]->name.compare("DATAOBJ#") == 0)
                        updateNumber32u(sysTab->dataObj, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("TS#") == 0)
                        updateNumber32u(sysTab->ts, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("FILE#") == 0)
                        updateNumber32u(sysTab->file, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("BLOCK#") == 0)
                        updateNumber32u(sysTab->block, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("CLUCOLS") == 0)
                        updateNumber16(sysTab->cluCols, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("FLAGS") == 0)
                        updateNumberXu(sysTab->flags, column, object, rowId);
                    else if (object->columns[column]->name.compare("PROPERTY") == 0)
                        updateNumberXu(sysTab->property, column, object, rowId);
                }
            }

            schema->sysTabMapRowId[rowId] = sysTab;
            schema->sysTabTouched = true;

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            if (schema->sysTabComPartMapRowId.find(rowId) != schema->sysTabComPartMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABCOMPART$: (rowid: " << rowId << ") for insert");
            }
            SysTabComPart *sysTabComPart = new SysTabComPart(rowId, 0, 0, 0, true);
            if (sysTabComPart == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysTabComPart) << " bytes memory (for: SysTabComPart)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0)
                        updateObj(sysTabComPart->obj, column, object, rowId);
                    else if (object->columns[column]->name.compare("DATAOBJ#") == 0)
                        updateNumber32u(sysTabComPart->dataObj, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("BO#") == 0)
                        updateNumber32u(sysTabComPart->bo, 0, column, object, rowId);
                }
            }

            schema->sysTabComPartMapRowId[rowId] = sysTabComPart;
            schema->sysTabComPartTouched = true;

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            if (schema->sysTabPartMapRowId.find(rowId) != schema->sysTabPartMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABPART$: (rowid: " << rowId << ") for insert");
            }
            SysTabPart *sysTabPart = new SysTabPart(rowId, 0, 0, 0, true);
            if (sysTabPart == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysTabPart) << " bytes memory (for: SysTabPart)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0)
                        updateNumber32u(sysTabPart->obj, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("DATAOBJ#") == 0)
                        updateNumber32u(sysTabPart->dataObj, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("BO#") == 0)
                        updateObj(sysTabPart->bo, column, object, rowId);
                }
            }

            schema->sysTabPartMapRowId[rowId] = sysTabPart;
            schema->sysTabPartTouched = true;

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            if (schema->sysTabSubPartMapRowId.find(rowId) != schema->sysTabSubPartMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.TABSUBPART$: (rowid: " << rowId << ") for insert");
            }
            SysTabSubPart *sysTabSubPart = new SysTabSubPart(rowId, 0, 0, 0, true);
            if (sysTabSubPart == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysTabSubPart) << " bytes memory (for: SysTabSubPart)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0)
                        updateNumber32u(sysTabSubPart->obj, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("DATAOBJ#") == 0)
                        updateNumber32u(sysTabSubPart->dataObj, 0, column, object, rowId);
                    else if (object->columns[column]->name.compare("POBJ#") == 0)
                        updateObj(sysTabSubPart->pObj, column, object, rowId);
                }
            }

            schema->sysTabSubPartMapRowId[rowId] = sysTabSubPart;
            schema->sysTabSubPartTouched = true;

        } else if (object->systemTable == TABLE_SYS_USER) {
            if (schema->sysUserMapRowId.find(rowId) != schema->sysUserMapRowId.end()) {
                RUNTIME_FAIL("DDL: duplicate SYS.USER$: (rowid: " << rowId << ") for insert");
            }
            SysUser *sysUser = new SysUser(rowId, 0, "", 0, 0, false, true);
            if (sysUser == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysUser) << " bytes memory (for: SysUser)");
            }

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("USER#") == 0)
                        updateUser(sysUser->user, column, object, rowId);
                    else if (object->columns[column]->name.compare("NAME") == 0)
                        updateString(sysUser->name, column, object, rowId);
                    else if (object->columns[column]->name.compare("SPARE1") == 0)
                        updateNumberXu(sysUser->spare1, column, object, rowId);
                }
            }

            schema->sysUserMapRowId[rowId] = sysUser;
            schema->sysUserTouched = true;
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

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("CON#") == 0) {
                        if (updateNumber32u(sysCCol->con, 0, column, object, rowId)) {
                            sysCCol->touched = true;
                            schema->sysCColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("INTCOL#") == 0) {
                        if (updateNumber16(sysCCol->intCol, 0, column, object, rowId)) {
                            sysCCol->touched = true;
                            schema->sysCColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("OBJ#") == 0) {
                        if (updateObj(sysCCol->obj, column, object, rowId)) {
                            sysCCol->touched = true;
                            schema->sysCColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("SPARE1") == 0) {
                        if (updateNumberXu(sysCCol->spare1, column, object, rowId)) {
                            sysCCol->touched = true;
                            schema->sysCColTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            auto sysCDefIt = schema->sysCDefMapRowId.find(rowId);
            if (sysCDefIt == schema->sysCDefMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCDef *sysCDef = sysCDefIt->second;

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("CON#") == 0) {
                        if (updateNumber32u(sysCDef->con, 0, column, object, rowId)) {
                            sysCDef->touched = true;
                            schema->sysCDefTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("OBJ#") == 0) {
                        if (updateObj(sysCDef->obj, column, object, rowId)) {
                            sysCDef->touched = true;
                            schema->sysCDefTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("TYPE#") == 0) {
                        if (updateNumber16u(sysCDef->type, 0, column, object, rowId)) {
                            sysCDef->touched = true;
                            schema->sysCDefTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_COL) {
            auto sysColIt = schema->sysColMapRowId.find(rowId);
            if (sysColIt == schema->sysColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCol *sysCol = sysColIt->second;

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0) {
                        if (updateObj(sysCol->obj, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("COL#") == 0) {
                        if (updateNumber16(sysCol->col, 0, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("SEGCOL#") == 0) {
                        if (updateNumber16(sysCol->segCol, 0, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("INTCOL#") == 0) {
                        if (updateNumber16(sysCol->intCol, 0, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("NAME") == 0) {
                        if (updateString(sysCol->name, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("TYPE#") == 0) {
                        if (updateNumber16u(sysCol->type, 0, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched= true;
                        }
                    } else if (object->columns[column]->name.compare("LENGTH") == 0) {
                        if (updateNumber64u(sysCol->length, 0, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("PRECISION#") == 0) {
                        if (updateNumber64(sysCol->precision, -1, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("SCALE") == 0) {
                        if (updateNumber64(sysCol->scale, -1, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("CHARSETFORM") == 0) {
                        if (updateNumber64u(sysCol->charsetForm, 0, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("CHARSETID") == 0) {
                        if (updateNumber64u(sysCol->charsetId, 0, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("NULL$") == 0) {
                        if (updateNumber64(sysCol->null_, 0, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("PROPERTY") == 0) {
                        if (updateNumberXu(sysCol->property, column, object, rowId)) {
                            sysCol->touched = true;
                            schema->sysColTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            auto sysDeferredStgIt = schema->sysDeferredStgMapRowId.find(rowId);
            if (sysDeferredStgIt == schema->sysDeferredStgMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysDeferredStg *sysDeferredStg = sysDeferredStgIt->second;

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0) {
                        if (updateObj(sysDeferredStg->obj, column, object, rowId)) {
                            sysDeferredStg->touched = true;
                            schema->sysDeferredStgTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("FLAGS_STG") == 0) {
                        if (updateNumberXu(sysDeferredStg->flagsStg, column, object, rowId)) {
                            sysDeferredStg->touched = true;
                            schema->sysDeferredStgTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            auto sysEColIt = schema->sysEColMapRowId.find(rowId);
            if (sysEColIt == schema->sysEColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysECol *sysECol = sysEColIt->second;

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("TABOBJ#") == 0) {
                        if (updateObj(sysECol->tabObj, column, object, rowId)) {
                            sysECol->touched = true;
                            schema->sysEColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("COLNUM") == 0) {
                        if (updateNumber16(sysECol->colNum, 0, column, object, rowId)) {
                            sysECol->touched = true;
                            schema->sysEColTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("GUARD_ID") == 0) {
                        if (updateNumber16(sysECol->guardId, -1, column, object, rowId)) {
                            sysECol->touched = true;
                            schema->sysEColTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            auto sysObjIt = schema->sysObjMapRowId.find(rowId);
            if (sysObjIt == schema->sysObjMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysObj *sysObj = sysObjIt->second;

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OWNER#") == 0) {
                        if (updateNumber32u(sysObj->owner, 0, column, object, rowId)) {
                            sysObj->touched = true;
                            schema->sysObjTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("OBJ#") == 0) {
                        if (updateObj(sysObj->obj, column, object, rowId)) {
                            sysObj->touched = true;
                            schema->sysObjTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("DATAOBJ#") == 0) {
                        if (updateNumber32u(sysObj->dataObj, 0, column, object, rowId)) {
                            sysObj->touched = true;
                            schema->sysObjTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("NAME") == 0) {
                        if (updateString(sysObj->name, column, object, rowId)) {
                            sysObj->touched = true;
                            schema->sysObjTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("TYPE#") == 0) {
                        if (updateNumber16u(sysObj->type, 0, column, object, rowId)) {
                            sysObj->touched = true;
                            schema->sysObjTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("FLAGS") == 0) {
                        if (updateNumberXu(sysObj->flags, column, object, rowId)) {
                            sysObj->touched = true;
                            schema->sysObjTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_SEG) {
            auto sysSegIt = schema->sysSegMapRowId.find(rowId);
            if (sysSegIt == schema->sysSegMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysSeg *sysSeg = sysSegIt->second;

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("FILE#") == 0) {
                        if (updateNumber32u(sysSeg->file, 0, column, object, rowId)) {
                            sysSeg->touched = true;
                            schema->sysSegTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("BLOCK#") == 0) {
                        if (updateNumber32u(sysSeg->block, 0, column, object, rowId)) {
                            sysSeg->touched = true;
                            schema->sysSegTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("TS#") == 0) {
                        if (updateNumber32u(sysSeg->ts, 0, column, object, rowId)) {
                            sysSeg->touched = true;
                            schema->sysSegTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("SPARE1") == 0) {
                        if (updateNumberXu(sysSeg->spare1, column, object, rowId)) {
                            sysSeg->touched = true;
                            schema->sysSegTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_TAB) {
            auto sysTabIt = schema->sysTabMapRowId.find(rowId);
            if (sysTabIt == schema->sysTabMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTab *sysTab = sysTabIt->second;

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0) {
                        if (updateObj(sysTab->obj, column, object, rowId)) {
                            sysTab->touched = true;
                            schema->sysTabTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("DATAOBJ#") == 0) {
                        if (updateNumber32u(sysTab->dataObj, 0, column, object, rowId)) {
                            sysTab->touched = true;
                            schema->sysTabTouched = true;
                            schema->touchObj(sysTab->obj);
                        }
                    } else if (object->columns[column]->name.compare("TS#") == 0) {
                        if (updateNumber32u(sysTab->ts, 0, column, object, rowId)) {
                            sysTab->touched = true;
                            schema->sysTabTouched = true;
                            schema->touchObj(sysTab->obj);
                        }
                    } else if (object->columns[column]->name.compare("FILE#") == 0) {
                        if (updateNumber32u(sysTab->file, 0, column, object, rowId)) {
                            sysTab->touched = true;
                            schema->sysTabTouched = true;
                            schema->touchObj(sysTab->obj);
                        }
                    } else if (object->columns[column]->name.compare("BLOCK#") == 0) {
                        if (updateNumber32u(sysTab->block, 0, column, object, rowId)) {
                            sysTab->touched = true;
                            schema->sysTabTouched = true;
                            schema->touchObj(sysTab->obj);
                        }
                    } else if (object->columns[column]->name.compare("CLUCOLS") == 0) {
                        if (updateNumber16(sysTab->cluCols, 0, column, object, rowId)) {
                            sysTab->touched = true;
                            schema->sysTabTouched = true;
                            schema->touchObj(sysTab->obj);
                        }
                    } else if (object->columns[column]->name.compare("FLAGS") == 0) {
                        if (updateNumberXu(sysTab->flags, column, object, rowId)) {
                            sysTab->touched = true;
                            schema->sysTabTouched = true;
                            schema->touchObj(sysTab->obj);
                        }
                    } else if (object->columns[column]->name.compare("PROPERTY") == 0) {
                        if (updateNumberXu(sysTab->property, column, object, rowId)) {
                            sysTab->touched = true;
                            schema->sysTabTouched = true;
                            schema->touchObj(sysTab->obj);
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            auto sysTabComPartIt = schema->sysTabComPartMapRowId.find(rowId);
            if (sysTabComPartIt == schema->sysTabComPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabComPart *sysTabComPart = sysTabComPartIt->second;

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0) {
                        if (updateNumber32u(sysTabComPart->obj, 0, column, object, rowId)) {
                            sysTabComPart->touched = true;
                            schema->sysTabComPartTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("DATAOBJ#") == 0) {
                        if (updateNumber32u(sysTabComPart->dataObj, 0, column, object, rowId)) {
                            sysTabComPart->touched = true;
                            schema->sysTabComPartTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("BO#") == 0) {
                        if (updateObj(sysTabComPart->bo, column, object, rowId)) {
                            sysTabComPart->touched = true;
                            schema->sysTabComPartTouched = true;
                        }
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

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0) {
                        if (updateNumber32u(sysTabPart->obj, 0, column, object, rowId)) {
                            sysTabPart->touched = true;
                            schema->sysTabPartTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("DATAOBJ#") == 0) {
                        if (updateNumber32u(sysTabPart->dataObj, 0, column, object, rowId)) {
                            sysTabPart->touched = true;
                            schema->sysTabPartTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("BO#") == 0) {
                        if (updateObj(sysTabPart->bo, column, object, rowId)) {
                            sysTabPart->touched = true;
                            schema->sysTabPartTouched = true;
                        }
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

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("OBJ#") == 0) {
                        if (updateNumber32u(sysTabSubPart->obj, 0, column, object, rowId)) {
                            sysTabSubPart->touched = true;
                            schema->sysTabSubPartTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("DATAOBJ#") == 0) {
                        if (updateNumber32u(sysTabSubPart->dataObj, 0, column, object, rowId)) {
                            sysTabSubPart->touched = true;
                            schema->sysTabSubPartTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("POBJ#") == 0) {
                        if (updateObj(sysTabSubPart->pObj, column, object, rowId)) {
                            sysTabSubPart->touched = true;
                            schema->sysTabSubPartTouched = true;
                        }
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

            typeCOL column;
            uint64_t baseMax = outputBuffer->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (outputBuffer->valuesSet[base] < mask)
                        break;
                    if ((outputBuffer->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name.compare("USER#") == 0) {
                        if (updateUser(sysUser->user, column, object, rowId)) {
                            sysUser->touched = true;
                            schema->sysUserTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("NAME") == 0) {
                        if (updateString(sysUser->name, column, object, rowId)) {
                            sysUser->touched = true;
                            schema->sysUserTouched = true;
                        }
                    } else if (object->columns[column]->name.compare("SPARE1") == 0) {
                        if (updateNumberXu(sysUser->spare1, column, object, rowId)) {
                            sysUser->touched = true;
                            schema->sysUserTouched = true;
                        }
                    }
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
            auto sysCColIt = schema->sysCColMapRowId.find(rowId);
            if (sysCColIt == schema->sysCColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCCol *sysCCol = sysCColIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (CON#: " << dec << sysCCol->con << ", INTCOL#: " << sysCCol->intCol << ", OBJ#: " <<
                    sysCCol->obj << ", SPARE1: " << sysCCol->spare1 << ")");
            schema->touched = true;
            schema->sysCColMapRowId.erase(rowId);
            schema->sysCColTouched = true;
            schema->touchObj(sysCCol->obj);
            if (sysCCol->saved)
                schema->savedDeleted = true;
            delete sysCCol;

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            auto sysCDefIt = schema->sysCDefMapRowId.find(rowId);
            if (sysCDefIt == schema->sysCDefMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCDef *sysCDef = sysCDefIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (CON#: " << dec << sysCDef->con << ", OBJ#: " << sysCDef->obj << ", type: " << sysCDef->type << ")");
            schema->touched = true;
            schema->sysCDefMapRowId.erase(rowId);
            schema->sysCDefTouched = true;
            schema->touchObj(sysCDef->obj);
            if (sysCDef->saved)
                schema->savedDeleted = true;
            delete sysCDef;

        } else if (object->systemTable == TABLE_SYS_COL) {
            auto sysColIt = schema->sysColMapRowId.find(rowId);
            if (sysColIt == schema->sysColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysCol *sysCol = sysColIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysCol->obj << ", COL#: " << sysCol->col << ", SEGCOL#: " << sysCol->segCol <<
                    ", INTCOL#: " << sysCol->intCol << ", NAME: '" << sysCol->name << "', TYPE#: " << sysCol->type << ", LENGTH: " << sysCol->length <<
                    ", PRECISION#: " << sysCol->precision << ", SCALE: " << sysCol->scale << ", CHARSETFORM: " << sysCol->charsetForm <<
                    ", CHARSETID: " << sysCol->charsetId << ", NULL$: " << sysCol->null_ << ", PROPERTY: " << sysCol->property << ")");
            schema->touched = true;
            schema->sysColMapRowId.erase(rowId);
            schema->sysColTouched = true;
            schema->touchObj(sysCol->obj);
            if (sysCol->saved)
                schema->savedDeleted = true;
            delete sysCol;

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            auto sysDeferredStgIt = schema->sysDeferredStgMapRowId.find(rowId);
            if (sysDeferredStgIt == schema->sysDeferredStgMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysDeferredStg *sysDeferredStg = sysDeferredStgIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysDeferredStg->obj << ", FLAGS_STG: " << sysDeferredStg->flagsStg << ")");
            schema->touched = true;
            schema->sysDeferredStgMapRowId.erase(rowId);
            schema->sysDeferredStgTouched = true;
            schema->touchObj(sysDeferredStg->obj);
            if (sysDeferredStg->saved)
                schema->savedDeleted = true;
            delete sysDeferredStg;

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            auto sysEColIt = schema->sysEColMapRowId.find(rowId);
            if (sysEColIt == schema->sysEColMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysECol *sysECol = sysEColIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (TABOBJ#: " << dec << sysECol->tabObj << ", COLNUM: " << sysECol->colNum << ", GUARD_ID: " <<
                    sysECol->guardId << ")");
            schema->touched = true;
            schema->sysEColMapRowId.erase(rowId);
            schema->sysEColTouched = true;
            schema->touchObj(sysECol->tabObj);
            if (sysECol->saved)
                schema->savedDeleted = true;
            delete sysECol;

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            auto sysObjIt = schema->sysObjMapRowId.find(rowId);
            if (sysObjIt == schema->sysObjMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysObj *sysObj = sysObjIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OWNER#: " << dec << sysObj->owner << ", OBJ#: " << sysObj->obj << ", DATAOBJ#: " <<
                    sysObj->dataObj << ", TYPE#: " << sysObj->type << ", NAME: '" << sysObj->name << "', FLAGS: " << sysObj->flags << ")");
            schema->touched = true;
            schema->sysObjMapRowId.erase(rowId);
            schema->sysObjTouched = true;
            schema->touchObj(sysObj->obj);
            if (sysObj->saved)
                schema->savedDeleted = true;
            delete sysObj;

        } else if (object->systemTable == TABLE_SYS_SEG) {
            auto sysSegIt = schema->sysSegMapRowId.find(rowId);
            if (sysSegIt == schema->sysSegMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysSeg *sysSeg = sysSegIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (FILE#: " << dec << sysSeg->file << ", BLOCK#: " << sysSeg->block << ", TS#: " <<
                    sysSeg->ts << ", SPARE1: " << sysSeg->spare1 << ")");
            schema->touched = true;
            schema->sysSegMapRowId.erase(rowId);
            schema->sysSegTouched = true;
            if (sysSeg->file != 0 || sysSeg->block != 0) {
                SysTabKey sysTabKey(sysSeg->file, sysSeg->block, sysSeg->ts);
                auto sysTabMapKeyIt = schema->sysTabMapKey.find(sysTabKey);
                if (sysTabMapKeyIt != schema->sysTabMapKey.end()) {
                    SysTab* sysTab = sysTabMapKeyIt->second;
                    schema->touchObj(sysTab->obj);
                }
            }
            if (sysSeg->saved)
                schema->savedDeleted = true;
            delete sysSeg;

        } else if (object->systemTable == TABLE_SYS_TAB) {
            auto sysTabIt = schema->sysTabMapRowId.find(rowId);
            if (sysTabIt == schema->sysTabMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTab *sysTab = sysTabIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTab->obj << ", DATAOBJ#: " << sysTab->dataObj << ", TS#: " <<
                    sysTab->ts << ", FILE#: " << sysTab->file << ", BLOCK#: " << sysTab->block << ", CLUCOLS: " << sysTab->cluCols << ", FLAGS: " <<
                    sysTab->flags << ", PROPERTY: " << sysTab->property << ")");
            schema->touched = true;
            schema->sysTabMapRowId.erase(rowId);
            schema->sysTabTouched = true;
            schema->touchObj(sysTab->obj);
            if (sysTab->saved)
                schema->savedDeleted = true;
            delete sysTab;

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            auto sysTabComPartIt = schema->sysTabComPartMapRowId.find(rowId);
            if (sysTabComPartIt == schema->sysTabComPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabComPart *sysTabComPart = sysTabComPartIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabComPart->obj << ", DATAOBJ#: " << sysTabComPart->dataObj << ", BO#: " <<
                    sysTabComPart->bo << ")");
            schema->touched = true;
            schema->sysTabComPartMapRowId.erase(rowId);
            schema->sysTabComPartTouched = true;
            schema->touchObj(sysTabComPart->bo);
            if (sysTabComPart->saved)
                schema->savedDeleted = true;
            delete sysTabComPart;

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            auto sysTabPartIt = schema->sysTabPartMapRowId.find(rowId);
            if (sysTabPartIt == schema->sysTabPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabPart *sysTabPart = sysTabPartIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabPart->obj << ", DATAOBJ#: " << sysTabPart->dataObj << ", BO#: " <<
                    sysTabPart->bo << ")");
            schema->touched = true;
            schema->sysTabPartMapRowId.erase(rowId);
            schema->sysTabPartTouched = true;
            schema->touchObj(sysTabPart->bo);
            if (sysTabPart->saved)
                schema->savedDeleted = true;
            delete sysTabPart;

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            auto sysTabSubPartIt = schema->sysTabSubPartMapRowId.find(rowId);
            if (sysTabSubPartIt == schema->sysTabSubPartMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysTabSubPart *sysTabSubPart = sysTabSubPartIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (OBJ#: " << dec << sysTabSubPart->obj << ", DATAOBJ#: " << sysTabSubPart->dataObj << ", POBJ#: " <<
                    sysTabSubPart->pObj << ")");
            schema->touched = true;
            schema->sysTabSubPartMapRowId.erase(rowId);
            schema->sysTabSubPartTouched = true;
            schema->touchObj(sysTabSubPart->pObj);
            if (sysTabSubPart->saved)
                schema->savedDeleted = true;
            delete sysTabSubPart;

        } else if (object->systemTable == TABLE_SYS_USER) {
            auto sysUserIt = schema->sysUserMapRowId.find(rowId);
            if (sysUserIt == schema->sysUserMapRowId.end()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")");
                return;
            }
            SysUser *sysUser = sysUserIt->second;
            TRACE(TRACE2_SYSTEM, "SYSTEM: delete (USER#: " << dec << sysUser->user << ", NAME: " << sysUser->name << ", SPARE1: " <<
                    sysUser->spare1 << ")");
            schema->touched = true;
            schema->sysUserMapRowId.erase(rowId);
            schema->sysUserTouched = true;
            schema->touchUser(sysUser->user);
            if (sysUser->saved)
                schema->savedDeleted = true;
            delete sysUser;
        }
    }

    void SystemTransaction::commit(typeSCN scn) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: commit");

        if (!schema->touched)
            return;

        if (schema->refreshIndexes()) {
            oracleAnalyzer->schemaScn = scn;
            schema->writeSchema();
        }
        schema->rebuildMaps();
    }
}
