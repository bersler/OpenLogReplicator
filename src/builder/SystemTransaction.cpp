/* System transaction to change metadata
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

#include "../common/OracleColumn.h"
#include "../common/OracleObject.h"
#include "../common/RuntimeException.h"
#include "../common/SysCCol.h"
#include "../common/SysCDef.h"
#include "../common/SysCol.h"
#include "../common/SysDeferredStg.h"
#include "../common/SysECol.h"
#include "../common/SysLob.h"
#include "../common/SysObj.h"
#include "../common/SysTab.h"
#include "../common/SysTabComPart.h"
#include "../common/SysTabPart.h"
#include "../common/SysTabSubPart.h"
#include "../common/SysUser.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "../metadata/SchemaElement.h"
#include "Builder.h"
#include "SystemTransaction.h"

namespace OpenLogReplicator {

    SystemTransaction::SystemTransaction(Builder* newBuilder, Metadata* newMetadata) :
                ctx(newMetadata->ctx),
                builder(newBuilder),
                metadata(newMetadata),
                sysCCol(nullptr),
                sysCDef(nullptr),
                sysCol(nullptr),
                sysDeferredStg(nullptr),
                sysECol(nullptr),
                sysLob(nullptr),
                sysObj(nullptr),
                sysTab(nullptr),
                sysTabComPart(nullptr),
                sysTabPart(nullptr),
                sysTabSubPart(nullptr),
                sysUser(nullptr) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: begin")
    }

    SystemTransaction::~SystemTransaction() {
        if (sysCCol != nullptr) {
            delete sysCCol;
            sysCCol = nullptr;
        }

        if (sysCCol != nullptr) {
            delete sysCCol;
            sysCCol = nullptr;
        }

        if (sysCDef != nullptr) {
            delete sysCDef;
            sysCDef = nullptr;
        }

        if (sysCol != nullptr) {
            delete sysCol;
            sysCol = nullptr;
        }

        if (sysDeferredStg != nullptr) {
            delete sysDeferredStg;
            sysDeferredStg = nullptr;
        }

        if (sysECol != nullptr) {
            delete sysECol;
            sysECol = nullptr;
        }

        if (sysLob != nullptr) {
            delete sysLob;
            sysLob = nullptr;
        }

        if (sysObj != nullptr) {
            delete sysObj;
            sysObj = nullptr;
        }

        if (sysTab != nullptr) {
            delete sysTab;
            sysTab = nullptr;
        }

        if (sysTabComPart != nullptr) {
            delete sysTabComPart;
            sysTabComPart = nullptr;
        }

        if (sysTabPart != nullptr) {
            delete sysTabPart;
            sysTabPart = nullptr;
        }

        if (sysTabSubPart != nullptr) {
            delete sysTabSubPart;
            sysTabSubPart = nullptr;
        }

        if (sysUser != nullptr) {
            delete sysUser;
            sysUser = nullptr;
        }
    }

    bool SystemTransaction::updateNumber16(int16_t& val, int16_t defVal, typeCol column, OracleObject* object, typeRowId& rowId __attribute__((unused))) {
        if (builder->values[column][VALUE_AFTER] != nullptr && builder->lengths[column][VALUE_AFTER] > 0) {
            char* retPtr;
            if (object->columns[column]->type != 2)
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " type found " + std::to_string(object->columns[column]->type));

            builder->parseNumber(builder->values[column][VALUE_AFTER], builder->lengths[column][VALUE_AFTER]);
            builder->valueBuffer[builder->valueLength] = 0;
            auto newVal = (int16_t)strtol(builder->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> " << newVal << ")")
                metadata->schema->touched = true;
                val = newVal;
                return true;
            }
        } else if (builder->values[column][VALUE_AFTER] != nullptr || builder->values[column][VALUE_BEFORE] != nullptr) {
            if (val != defVal) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> NULL)")
                metadata->schema->touched = true;
                val = defVal;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber16u(uint16_t& val, uint16_t defVal, typeCol column, OracleObject* object, typeRowId& rowId __attribute__((unused))) {
        if (builder->values[column][VALUE_AFTER] != nullptr && builder->lengths[column][VALUE_AFTER] > 0) {
            char* retPtr;
            if (object->columns[column]->type != 2)
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " type found " + std::to_string(object->columns[column]->type));

            builder->parseNumber(builder->values[column][VALUE_AFTER], builder->lengths[column][VALUE_AFTER]);
            builder->valueBuffer[builder->valueLength] = 0;
            if (builder->valueLength == 0 || (builder->valueLength > 0 && builder->valueBuffer[0] == '-'))
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " value found " + builder->valueBuffer);

            uint16_t newVal = strtoul(builder->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> " << newVal << ")")
                metadata->schema->touched = true;
                val = newVal;
                return true;
            }
        } else if (builder->values[column][VALUE_AFTER] != nullptr || builder->values[column][VALUE_BEFORE] != nullptr) {
            if (val != defVal) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> NULL)")
                metadata->schema->touched = true;
                val = defVal;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber32u(uint32_t& val, uint32_t defVal, typeCol column, OracleObject* object, typeRowId& rowId __attribute__((unused))) {
        if (builder->values[column][VALUE_AFTER] != nullptr && builder->lengths[column][VALUE_AFTER] > 0) {
            char* retPtr;
            if (object->columns[column]->type != 2)
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " type found " + std::to_string(object->columns[column]->type));

            builder->parseNumber(builder->values[column][VALUE_AFTER], builder->lengths[column][VALUE_AFTER]);
            builder->valueBuffer[builder->valueLength] = 0;
            if (builder->valueLength == 0 || (builder->valueLength > 0 && builder->valueBuffer[0] == '-'))
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " value found " + builder->valueBuffer);

            uint32_t newVal = strtoul(builder->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> " << newVal << ")")
                metadata->schema->touched = true;
                val = newVal;
                return true;
            }
        } else if (builder->values[column][VALUE_AFTER] != nullptr || builder->values[column][VALUE_BEFORE] != nullptr) {
            if (val != defVal) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> NULL)")
                metadata->schema->touched = true;
                val = defVal;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateObj(typeObj& val, typeCol column, OracleObject* object, typeRowId& rowId __attribute__((unused))) {
        if (builder->values[column][VALUE_AFTER] != nullptr && builder->lengths[column][VALUE_AFTER] > 0) {
            char* retPtr;
            if (object->columns[column]->type != 2)
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " type found " + std::to_string(object->columns[column]->type));

            builder->parseNumber(builder->values[column][VALUE_AFTER], builder->lengths[column][VALUE_AFTER]);
            builder->valueBuffer[builder->valueLength] = 0;
            if (builder->valueLength == 0 || (builder->valueLength > 0 && builder->valueBuffer[0] == '-'))
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " value found " + builder->valueBuffer);

            typeObj newVal = strtoul(builder->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> " << newVal << ")")
                metadata->schema->touched = true;
                metadata->schema->touchObj(val);
                metadata->schema->touchObj(newVal);
                val = newVal;
                return true;
            }
        } else if (builder->values[column][VALUE_AFTER] != nullptr || builder->values[column][VALUE_BEFORE] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> NULL)")
                metadata->schema->touched = true;
                metadata->schema->touchObj(val);
                val = 0;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updatePart(typeObj& val, typeCol column, OracleObject* object, typeRowId& rowId __attribute__((unused))) {
        if (builder->values[column][VALUE_AFTER] != nullptr && builder->lengths[column][VALUE_AFTER] > 0) {
            char* retPtr;
            if (object->columns[column]->type != 2)
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " type found " + std::to_string(object->columns[column]->type));

            builder->parseNumber(builder->values[column][VALUE_AFTER], builder->lengths[column][VALUE_AFTER]);
            builder->valueBuffer[builder->valueLength] = 0;
            if (builder->valueLength == 0 || (builder->valueLength > 0 && builder->valueBuffer[0] == '-'))
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " value found " + builder->valueBuffer);

            typeObj newVal = strtoul(builder->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> " << newVal << ")")
                metadata->schema->touched = true;
                metadata->schema->touchPart(val);
                metadata->schema->touchPart(newVal);
                val = newVal;
                return true;
            }
        } else if (builder->values[column][VALUE_AFTER] != nullptr || builder->values[column][VALUE_BEFORE] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> NULL)")
                metadata->schema->touched = true;
                metadata->schema->touchPart(val);
                val = 0;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateUser(typeUser& val, typeCol column, OracleObject* object, typeRowId& rowId __attribute__((unused))) {
        if (builder->values[column][VALUE_AFTER] != nullptr && builder->lengths[column][VALUE_AFTER] > 0) {
            char* retPtr;
            if (object->columns[column]->type != 2)
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " type found " + std::to_string(object->columns[column]->type));

            builder->parseNumber(builder->values[column][VALUE_AFTER], builder->lengths[column][VALUE_AFTER]);
            builder->valueBuffer[builder->valueLength] = 0;
            if (builder->valueLength == 0 || (builder->valueLength > 0 && builder->valueBuffer[0] == '-'))
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " value found " + builder->valueBuffer);

            typeUser newVal = strtoul(builder->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> " << newVal << ")")
                metadata->schema->touched = true;
                metadata->schema->touchUser(val);
                metadata->schema->touchUser(newVal);
                val = newVal;
                return true;
            }
        } else if (builder->values[column][VALUE_AFTER] != nullptr || builder->values[column][VALUE_BEFORE] != nullptr) {
            if (val != 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> NULL)")
                metadata->schema->touched = true;
                metadata->schema->touchUser(val);
                val = 0;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber64(int64_t& val, int64_t defVal, typeCol column, OracleObject* object, typeRowId& rowId __attribute__((unused))) {
        if (builder->values[column][VALUE_AFTER] != nullptr && builder->lengths[column][VALUE_AFTER] > 0) {
            char* retPtr;
            if (object->columns[column]->type != 2)
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " type found " + std::to_string(object->columns[column]->type));

            builder->parseNumber(builder->values[column][VALUE_AFTER], builder->lengths[column][VALUE_AFTER]);
            builder->valueBuffer[builder->valueLength] = 0;
            if (builder->valueLength == 0)
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " value found " + builder->valueBuffer);

            int64_t newVal = strtol(builder->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> " << newVal << ")")
                metadata->schema->touched = true;
                val = newVal;
                return true;
            }
        } else if (builder->values[column][VALUE_AFTER] != nullptr || builder->values[column][VALUE_BEFORE] != nullptr) {
            if (val != defVal) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> NULL)")
                metadata->schema->touched = true;
                val = defVal;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumber64u(uint64_t& val, uint64_t defVal, typeCol column, OracleObject* object, typeRowId& rowId __attribute__((unused))) {
        if (builder->values[column][VALUE_AFTER] != nullptr && builder->lengths[column][VALUE_AFTER] > 0) {
            char* retPtr;
            if (object->columns[column]->type != 2)
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " type found " + std::to_string(object->columns[column]->type));

            builder->parseNumber(builder->values[column][VALUE_AFTER], builder->lengths[column][VALUE_AFTER]);
            builder->valueBuffer[builder->valueLength] = 0;
            if (builder->valueLength == 0 || (builder->valueLength > 0 && builder->valueBuffer[0] == '-'))
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                       object->columns[column]->name + " value found " + builder->valueBuffer);

            uint64_t newVal = strtoul(builder->valueBuffer, &retPtr, 10);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> " << newVal << ")")
                metadata->schema->touched = true;
                val = newVal;
                return true;
            }
        } else if (builder->values[column][VALUE_AFTER] != nullptr || builder->values[column][VALUE_BEFORE] != nullptr) {
            if (val != defVal) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> NULL)")
                metadata->schema->touched = true;
                val = defVal;
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateNumberXu(typeINTX& val, typeCol column, OracleObject* object, typeRowId& rowId __attribute__((unused))) {
        if (builder->values[column][VALUE_AFTER] != nullptr && builder->lengths[column][VALUE_AFTER] > 0) {
            if (object->columns[column]->type != 2)
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " type found " + std::to_string(object->columns[column]->type));

            builder->parseNumber(builder->values[column][VALUE_AFTER], builder->lengths[column][VALUE_AFTER]);
            builder->valueBuffer[builder->valueLength] = 0;
            if (builder->valueLength == 0 || (builder->valueLength > 0 && builder->valueBuffer[0] == '-'))
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " value found " + builder->valueBuffer);

            typeINTX newVal(0);
            newVal.setStr(builder->valueBuffer, builder->valueLength);
            if (newVal != val) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> " << newVal << ")")
                metadata->schema->touched = true;
                val = newVal;
                return true;
            }
        } else if (builder->values[column][VALUE_AFTER] != nullptr || builder->values[column][VALUE_BEFORE] != nullptr) {
            if (!val.isZero()) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": " << std::dec << val << " -> NULL)")
                metadata->schema->touched = true;
                val.set(0, 0);
                return true;
            }
        }
        return false;
    }

    bool SystemTransaction::updateString(std::string& val, uint64_t maxLength, typeCol column, OracleObject* object, typeRowId& rowId __attribute__((unused))) {
        if (builder->values[column][VALUE_AFTER] != nullptr && builder->lengths[column][VALUE_AFTER] > 0) {
            if (object->columns[column]->type != SYS_COL_TYPE_VARCHAR && object->columns[column]->type != SYS_COL_TYPE_CHAR)
                throw RuntimeException("ddl: column type mismatch for " + object->owner + "." + object->name + ": column " +
                        object->columns[column]->name + " type found " + std::to_string(object->columns[column]->type));

            builder->parseString(builder->values[column][VALUE_AFTER], builder->lengths[column][VALUE_AFTER], object->columns[column]->charsetId);
            std::string newVal(builder->valueBuffer, builder->valueLength);
            if (builder->valueLength > maxLength)
                throw RuntimeException("ddl: value too long for " + object->owner + "." + object->name + ": column " + object->columns[column]->name +
                        ", length " + std::to_string(builder->valueLength));

            if (val != newVal) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": '" << val << "' -> '" << newVal << "')")
                metadata->schema->touched = true;
                val = newVal;
                return true;
            }
        } else if (builder->values[column][VALUE_AFTER] != nullptr || builder->values[column][VALUE_BEFORE] != nullptr) {
            if (val.length() > 0) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: set (" << object->columns[column]->name << ": '" << val << "' -> NULL)")
                metadata->schema->touched = true;
                val.assign("");
                return true;
            }
        }
        return false;
    }

    void SystemTransaction::processInsert(OracleObject* object, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid __attribute__((unused))) {
        typeRowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);

        TRACE(TRACE2_SYSTEM, "SYSTEM: insert table (name: " << object->owner << "." << object->name << ", rowid: " << rowId << ")")

        if (object->systemTable == TABLE_SYS_CCOL) {
            if (metadata->schema->dictSysCColFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.CCOL$: (rowid: ") + str + ") for insert");
            sysCCol = new SysCCol(rowId, 0, 0, 0, 0, 0, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "CON#")
                        updateNumber32u(sysCCol->con, 0, column, object, rowId);
                    else if (object->columns[column]->name == "INTCOL#")
                        updateNumber16(sysCCol->intCol, 0, column, object, rowId);
                    else if (object->columns[column]->name == "OBJ#")
                        updateObj(sysCCol->obj, column, object, rowId);
                    else if (object->columns[column]->name == "SPARE1")
                        updateNumberXu(sysCCol->spare1, column, object, rowId);
                }
            }

            metadata->schema->sysCColMapRowId[rowId] = sysCCol;
            metadata->schema->sysCColTouched = true;
            sysCCol = nullptr;

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            if (metadata->schema->dictSysCDefFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.CDEF$: (rowid: ") + str + ") for insert");
            sysCDef = new SysCDef(rowId, 0, 0, 0, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "CON#")
                        updateNumber32u(sysCDef->con, 0, column, object, rowId);
                    else if (object->columns[column]->name == "OBJ#")
                        updateObj(sysCDef->obj, column, object, rowId);
                    else if (object->columns[column]->name == "TYPE#")
                        updateNumber16u(sysCDef->type, 0, column, object, rowId);
                }
            }

            metadata->schema->sysCDefMapRowId[rowId] = sysCDef;
            metadata->schema->sysCDefTouched = true;
            sysCDef = nullptr;

        } else if (object->systemTable == TABLE_SYS_COL) {
            if (metadata->schema->dictSysColFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.COL$: (rowid: ") + str + ") for insert");
            sysCol = new SysCol(rowId, 0, 0, 0, 0, "", 0, 0, -1, -1, 0, 0, 0, 0, 0, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#")
                        updateObj(sysCol->obj, column, object, rowId);
                    else if (object->columns[column]->name == "COL#")
                        updateNumber16(sysCol->col, 0, column, object, rowId);
                    else if (object->columns[column]->name == "SEGCOL#")
                        updateNumber16(sysCol->segCol, 0, column, object, rowId);
                    else if (object->columns[column]->name == "INTCOL#")
                        updateNumber16(sysCol->intCol, 0, column, object, rowId);
                    else if (object->columns[column]->name == "NAME")
                        updateString(sysCol->name, SYS_COL_NAME_LENGTH, column, object, rowId);
                    else if (object->columns[column]->name == "TYPE#")
                        updateNumber16u(sysCol->type, 0, column, object, rowId);
                    else if (object->columns[column]->name == "LENGTH")
                        updateNumber64u(sysCol->length, 0, column, object, rowId);
                    else if (object->columns[column]->name == "PRECISION#")
                        updateNumber64(sysCol->precision, -1, column, object, rowId);
                    else if (object->columns[column]->name == "SCALE")
                        updateNumber64(sysCol->scale, -1, column, object, rowId);
                    else if (object->columns[column]->name == "CHARSETFORM")
                        updateNumber64u(sysCol->charsetForm, 0, column, object, rowId);
                    else if (object->columns[column]->name == "CHARSETID")
                        updateNumber64u(sysCol->charsetId, 0, column, object, rowId);
                    else if (object->columns[column]->name == "NULL$")
                        updateNumber64(sysCol->null_, 0, column, object, rowId);
                    else if (object->columns[column]->name == "PROPERTY")
                        updateNumberXu(sysCol->property, column, object, rowId);
                }
            }

            metadata->schema->sysColMapRowId[rowId] = sysCol;
            metadata->schema->sysColTouched = true;
            sysCol = nullptr;

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            if (metadata->schema->dictSysDeferredStgFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.DEFERRED_STG$: (rowid: ") + str + ") for insert");
            sysDeferredStg = new SysDeferredStg(rowId, 0, 0, 0, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#")
                        updateObj(sysDeferredStg->obj, column, object, rowId);
                    else if (object->columns[column]->name == "FLAGS_STG")
                        updateNumberXu(sysDeferredStg->flagsStg, column, object, rowId);
                }
            }

            metadata->schema->sysDeferredStgMapRowId[rowId] = sysDeferredStg;
            metadata->schema->sysDeferredStgTouched = true;
            metadata->schema->touchObj(sysDeferredStg->obj);
            sysDeferredStg = nullptr;

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            if (metadata->schema->dictSysEColFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.ECOL$: (rowid: ") + str + ") for insert");
            sysECol = new SysECol(rowId, 0, 0, -1, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "TABOBJ#")
                        updateObj(sysECol->tabObj, column, object, rowId);
                    else if (object->columns[column]->name == "COLNUM")
                        updateNumber16(sysECol->colNum, 0, column, object, rowId);
                    else if (object->columns[column]->name == "GUARD_ID")
                        updateNumber16(sysECol->guardId, -1, column, object, rowId);
                }
            }

            metadata->schema->sysEColMapRowId[rowId] = sysECol;
            metadata->schema->sysEColTouched = true;
            sysECol = nullptr;

        } else if (object->systemTable == TABLE_SYS_LOB) {
            if (metadata->schema->dictSysLobFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.LOB$: (rowid: ") + str + ") for insert");
            sysLob = new SysLob(rowId, 0, 0, 0, 0, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#")
                        updateObj(sysLob->obj, column, object, rowId);
                    else if (object->columns[column]->name == "COL#")
                        updateNumber16(sysLob->col, 0, column, object, rowId);
                    else if (object->columns[column]->name == "INTCOL#")
                        updateNumber16(sysLob->intCol, 0, column, object, rowId);
                    else if (object->columns[column]->name == "LOBJ#")
                        updateObj(sysLob->lObj, column, object, rowId);
                }
            }

            metadata->schema->sysLobMapRowId[rowId] = sysLob;
            metadata->schema->sysLobTouched = true;
            sysLob = nullptr;

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            if (metadata->schema->dictSysObjFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.OBJ$: (rowid: ") + str + ") for insert");
            sysObj = new SysObj(rowId, 0, 0, 0, 0, "", 0, 0, false, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OWNER#")
                        updateNumber32u(sysObj->owner, 0, column, object, rowId);
                    else if (object->columns[column]->name == "OBJ#")
                        updateObj(sysObj->obj, column, object, rowId);
                    else if (object->columns[column]->name == "DATAOBJ#")
                        updateNumber32u(sysObj->dataObj, 0, column, object, rowId);
                    else if (object->columns[column]->name == "NAME")
                        updateString(sysObj->name, SYS_OBJ_NAME_LENGTH, column, object, rowId);
                    else if (object->columns[column]->name == "TYPE#")
                        updateNumber16u(sysObj->type, 0, column, object, rowId);
                    else if (object->columns[column]->name == "FLAGS")
                        updateNumberXu(sysObj->flags, column, object, rowId);
                }
            }

            metadata->schema->sysObjMapRowId[rowId] = sysObj;
            metadata->schema->sysObjTouched = true;
            sysObj = nullptr;

        } else if (object->systemTable == TABLE_SYS_TAB) {
            if (metadata->schema->dictSysTabFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.TAB$: (rowid: ") + str + ") for insert");
            sysTab = new SysTab(rowId, 0, 0, 0, 0, 0, 0, 0, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#")
                        updateObj(sysTab->obj, column, object, rowId);
                    else if (object->columns[column]->name == "DATAOBJ#")
                        updateNumber32u(sysTab->dataObj, 0, column, object, rowId);
                    else if (object->columns[column]->name == "CLUCOLS")
                        updateNumber16(sysTab->cluCols, 0, column, object, rowId);
                    else if (object->columns[column]->name == "FLAGS")
                        updateNumberXu(sysTab->flags, column, object, rowId);
                    else if (object->columns[column]->name == "PROPERTY")
                        updateNumberXu(sysTab->property, column, object, rowId);
                }
            }

            metadata->schema->sysTabMapRowId[rowId] = sysTab;
            metadata->schema->sysTabTouched = true;
            sysTab = nullptr;

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            if (metadata->schema->dictSysTabComPartFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.TABCOMPART$: (rowid: ") + str + ") for insert");
            sysTabComPart = new SysTabComPart(rowId, 0, 0, 0, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#")
                        updateObj(sysTabComPart->obj, column, object, rowId);
                    else if (object->columns[column]->name == "DATAOBJ#")
                        updateNumber32u(sysTabComPart->dataObj, 0, column, object, rowId);
                    else if (object->columns[column]->name == "BO#")
                        updateNumber32u(sysTabComPart->bo, 0, column, object, rowId);
                }
            }

            metadata->schema->sysTabComPartMapRowId[rowId] = sysTabComPart;
            metadata->schema->sysTabComPartTouched = true;
            sysTabComPart = nullptr;

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            if (metadata->schema->dictSysTabPartFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.TABPART$: (rowid: ") + str + ") for insert");
            sysTabPart = new SysTabPart(rowId, 0, 0, 0, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#")
                        updateNumber32u(sysTabPart->obj, 0, column, object, rowId);
                    else if (object->columns[column]->name == "DATAOBJ#")
                        updateNumber32u(sysTabPart->dataObj, 0, column, object, rowId);
                    else if (object->columns[column]->name == "BO#")
                        updateObj(sysTabPart->bo, column, object, rowId);
                }
            }

            metadata->schema->sysTabPartMapRowId[rowId] = sysTabPart;
            metadata->schema->sysTabPartTouched = true;
            sysTabPart = nullptr;

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            if (metadata->schema->dictSysTabSubPartFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.TABSUBPART$: (rowid: ") + str + ") for insert");
            sysTabSubPart = new SysTabSubPart(rowId, 0, 0, 0, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#")
                        updateNumber32u(sysTabSubPart->obj, 0, column, object, rowId);
                    else if (object->columns[column]->name == "DATAOBJ#")
                        updateNumber32u(sysTabSubPart->dataObj, 0, column, object, rowId);
                    else if (object->columns[column]->name == "POBJ#")
                        updatePart(sysTabSubPart->pObj, column, object, rowId);
                }
            }

            metadata->schema->sysTabSubPartMapRowId[rowId] = sysTabSubPart;
            metadata->schema->sysTabSubPartTouched = true;
            sysTabSubPart = nullptr;

        } else if (object->systemTable == TABLE_SYS_USER) {
            if (metadata->schema->dictSysUserFind(rowId))
                throw RuntimeException(std::string("DDL: duplicate SYS.USER$: (rowid: ") + str + ") for insert");
            sysUser = new SysUser(rowId, 0, "", 0, 0, false, true);

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "USER#")
                        updateUser(sysUser->user, column, object, rowId);
                    else if (object->columns[column]->name == "NAME")
                        updateString(sysUser->name, SYS_USER_NAME_LENGTH, column, object, rowId);
                    else if (object->columns[column]->name == "SPARE1")
                        updateNumberXu(sysUser->spare1, column, object, rowId);
                }
            }

            metadata->schema->sysUserMapRowId[rowId] = sysUser;
            metadata->schema->sysUserTouched = true;
            metadata->schema->touchUser(sysUser->user);
            sysUser = nullptr;
        }
    }

    void SystemTransaction::processUpdate(OracleObject* object, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid __attribute__((unused))) {
        typeRowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        TRACE(TRACE2_SYSTEM, "SYSTEM: update table (name: " << object->owner << "." << object->name << ", rowid: " << rowId << ")")

        if (object->systemTable == TABLE_SYS_CCOL) {
            SysCCol* sysCCol2 = metadata->schema->dictSysCColFind(rowId);
            if (sysCCol2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "CON#") {
                        if (updateNumber32u(sysCCol2->con, 0, column, object, rowId)) {
                            sysCCol2->touched = true;
                            metadata->schema->sysCColTouched = true;
                        }
                    } else if (object->columns[column]->name == "INTCOL#") {
                        if (updateNumber16(sysCCol2->intCol, 0, column, object, rowId)) {
                            sysCCol2->touched = true;
                            metadata->schema->sysCColTouched = true;
                        }
                    } else if (object->columns[column]->name == "OBJ#") {
                        if (updateObj(sysCCol2->obj, column, object, rowId)) {
                            sysCCol2->touched = true;
                            metadata->schema->sysCColTouched = true;
                        }
                    } else if (object->columns[column]->name == "SPARE1") {
                        if (updateNumberXu(sysCCol2->spare1, column, object, rowId)) {
                            sysCCol2->touched = true;
                            metadata->schema->sysCColTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_CDEF) {
            SysCDef* sysCDef2 = metadata->schema->dictSysCDefFind(rowId);
            if (sysCDef2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "CON#") {
                        if (updateNumber32u(sysCDef2->con, 0, column, object, rowId)) {
                            sysCDef2->touched = true;
                            metadata->schema->sysCDefTouched = true;
                        }
                    } else if (object->columns[column]->name == "OBJ#") {
                        if (updateObj(sysCDef2->obj, column, object, rowId)) {
                            sysCDef2->touched = true;
                            metadata->schema->sysCDefTouched = true;
                        }
                    } else if (object->columns[column]->name == "TYPE#") {
                        if (updateNumber16u(sysCDef2->type, 0, column, object, rowId)) {
                            sysCDef2->touched = true;
                            metadata->schema->sysCDefTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_COL) {
            SysCol* sysCol2 = metadata->schema->dictSysColFind(rowId);
            if (sysCol2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#") {
                        if (updateObj(sysCol2->obj, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name == "COL#") {
                        if (updateNumber16(sysCol2->col, 0, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name == "SEGCOL#") {
                        if (updateNumber16(sysCol2->segCol, 0, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name == "INTCOL#") {
                        if (updateNumber16(sysCol2->intCol, 0, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name == "NAME") {
                        if (updateString(sysCol2->name, SYS_COL_NAME_LENGTH, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name == "TYPE#") {
                        if (updateNumber16u(sysCol2->type, 0, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched= true;
                        }
                    } else if (object->columns[column]->name == "LENGTH") {
                        if (updateNumber64u(sysCol2->length, 0, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name == "PRECISION#") {
                        if (updateNumber64(sysCol2->precision, -1, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name == "SCALE") {
                        if (updateNumber64(sysCol2->scale, -1, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name == "CHARSETFORM") {
                        if (updateNumber64u(sysCol2->charsetForm, 0, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name == "CHARSETID") {
                        if (updateNumber64u(sysCol2->charsetId, 0, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name == "NULL$") {
                        if (updateNumber64(sysCol2->null_, 0, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    } else if (object->columns[column]->name == "PROPERTY") {
                        if (updateNumberXu(sysCol2->property, column, object, rowId)) {
                            sysCol2->touched = true;
                            metadata->schema->sysColTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_DEFERRED_STG) {
            SysDeferredStg* sysDeferredStg2 = metadata->schema->dictSysDeferredStgFind(rowId);
            if (sysDeferredStg2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#") {
                        if (updateObj(sysDeferredStg2->obj, column, object, rowId)) {
                            sysDeferredStg2->touched = true;
                            metadata->schema->sysDeferredStgTouched = true;
                        }
                    } else if (object->columns[column]->name == "FLAGS_STG") {
                        if (updateNumberXu(sysDeferredStg2->flagsStg, column, object, rowId)) {
                            sysDeferredStg2->touched = true;
                            metadata->schema->sysDeferredStgTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_ECOL) {
            SysECol* sysECol2 = metadata->schema->dictSysEColFind(rowId);
            if (sysECol2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "TABOBJ#") {
                        if (updateObj(sysECol2->tabObj, column, object, rowId)) {
                            sysECol2->touched = true;
                            metadata->schema->sysEColTouched = true;
                        }
                    } else if (object->columns[column]->name == "COLNUM") {
                        if (updateNumber16(sysECol2->colNum, 0, column, object, rowId)) {
                            sysECol2->touched = true;
                            metadata->schema->sysEColTouched = true;
                        }
                    } else if (object->columns[column]->name == "GUARD_ID") {
                        if (updateNumber16(sysECol2->guardId, -1, column, object, rowId)) {
                            sysECol2->touched = true;
                            metadata->schema->sysEColTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_LOB) {
            SysLob* sysLob2 = metadata->schema->dictSysLobFind(rowId);
            if (sysLob2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#") {
                        if (updateObj(sysLob2->obj, column, object, rowId)) {
                            sysLob2->touched = true;
                            metadata->schema->sysLobTouched = true;
                        }
                    } else if (object->columns[column]->name == "COL#") {
                        if (updateNumber16(sysLob2->col, 0, column, object, rowId)) {
                            sysLob2->touched = true;
                            metadata->schema->sysTabTouched = true;
                            metadata->schema->touchObj(sysLob2->obj);
                        }
                    } else if (object->columns[column]->name == "INTCOL#") {
                        if (updateNumber16(sysLob2->intCol, 0, column, object, rowId)) {
                            sysLob2->touched = true;
                            metadata->schema->sysLobTouched = true;
                            metadata->schema->touchObj(sysLob2->obj);
                        }
                    } else if (object->columns[column]->name == "LOBJ#") {
                        if (updateObj(sysLob2->lObj, column, object, rowId)) {
                            sysLob2->touched = true;
                            metadata->schema->sysLobTouched = true;
                            metadata->schema->touchObj(sysLob2->obj);
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_OBJ) {
            SysObj* sysObj2 = metadata->schema->dictSysObjFind(rowId);
            if (sysObj2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OWNER#") {
                        if (updateNumber32u(sysObj2->owner, 0, column, object, rowId)) {
                            sysObj2->touched = true;
                            metadata->schema->sysObjTouched = true;
                        }
                    } else if (object->columns[column]->name == "OBJ#") {
                        if (updateObj(sysObj2->obj, column, object, rowId)) {
                            sysObj2->touched = true;
                            metadata->schema->sysObjTouched = true;
                        }
                    } else if (object->columns[column]->name == "DATAOBJ#") {
                        if (updateNumber32u(sysObj2->dataObj, 0, column, object, rowId)) {
                            sysObj2->touched = true;
                            metadata->schema->sysObjTouched = true;
                        }
                    } else if (object->columns[column]->name == "NAME") {
                        if (updateString(sysObj2->name, SYS_OBJ_NAME_LENGTH, column, object, rowId)) {
                            sysObj2->touched = true;
                            metadata->schema->sysObjTouched = true;
                        }
                    } else if (object->columns[column]->name == "TYPE#") {
                        if (updateNumber16u(sysObj2->type, 0, column, object, rowId)) {
                            sysObj2->touched = true;
                            metadata->schema->sysObjTouched = true;
                        }
                    } else if (object->columns[column]->name == "FLAGS") {
                        if (updateNumberXu(sysObj2->flags, column, object, rowId)) {
                            sysObj2->touched = true;
                            metadata->schema->sysObjTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_TAB) {
            SysTab* sysTab2 = metadata->schema->dictSysTabFind(rowId);
            if (sysTab2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#") {
                        if (updateObj(sysTab2->obj, column, object, rowId)) {
                            sysTab2->touched = true;
                            metadata->schema->sysTabTouched = true;
                        }
                    } else if (object->columns[column]->name == "DATAOBJ#") {
                        if (updateNumber32u(sysTab2->dataObj, 0, column, object, rowId)) {
                            sysTab2->touched = true;
                            metadata->schema->sysTabTouched = true;
                            metadata->schema->touchObj(sysTab2->obj);
                        }
                    } else if (object->columns[column]->name == "CLUCOLS") {
                        if (updateNumber16(sysTab2->cluCols, 0, column, object, rowId)) {
                            sysTab2->touched = true;
                            metadata->schema->sysTabTouched = true;
                            metadata->schema->touchObj(sysTab2->obj);
                        }
                    } else if (object->columns[column]->name == "FLAGS") {
                        if (updateNumberXu(sysTab2->flags, column, object, rowId)) {
                            sysTab2->touched = true;
                            metadata->schema->sysTabTouched = true;
                            metadata->schema->touchObj(sysTab2->obj);
                        }
                    } else if (object->columns[column]->name == "PROPERTY") {
                        if (updateNumberXu(sysTab2->property, column, object, rowId)) {
                            sysTab2->touched = true;
                            metadata->schema->sysTabTouched = true;
                            metadata->schema->touchObj(sysTab2->obj);
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_TABCOMPART) {
            SysTabComPart* sysTabComPart2 = metadata->schema->dictSysTabComPartFind(rowId);
            if (sysTabComPart2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#") {
                        if (updateNumber32u(sysTabComPart2->obj, 0, column, object, rowId)) {
                            sysTabComPart2->touched = true;
                            metadata->schema->sysTabComPartTouched = true;
                        }
                    } else if (object->columns[column]->name == "DATAOBJ#") {
                        if (updateNumber32u(sysTabComPart2->dataObj, 0, column, object, rowId)) {
                            sysTabComPart2->touched = true;
                            metadata->schema->sysTabComPartTouched = true;
                        }
                    } else if (object->columns[column]->name == "BO#") {
                        if (updateObj(sysTabComPart2->bo, column, object, rowId)) {
                            sysTabComPart2->touched = true;
                            metadata->schema->sysTabComPartTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_TABPART) {
            SysTabPart* sysTabPart2 = metadata->schema->dictSysTabPartFind(rowId);
            if (sysTabPart2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#") {
                        if (updateNumber32u(sysTabPart2->obj, 0, column, object, rowId)) {
                            sysTabPart2->touched = true;
                            metadata->schema->sysTabPartTouched = true;
                        }
                    } else if (object->columns[column]->name == "DATAOBJ#") {
                        if (updateNumber32u(sysTabPart2->dataObj, 0, column, object, rowId)) {
                            sysTabPart2->touched = true;
                            metadata->schema->sysTabPartTouched = true;
                        }
                    } else if (object->columns[column]->name == "BO#") {
                        if (updateObj(sysTabPart2->bo, column, object, rowId)) {
                            sysTabPart2->touched = true;
                            metadata->schema->sysTabPartTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_TABSUBPART) {
            SysTabSubPart* sysTabSubPart2 = metadata->schema->dictSysTabSubPartFind(rowId);
            if (sysTabSubPart2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "OBJ#") {
                        if (updateNumber32u(sysTabSubPart2->obj, 0, column, object, rowId)) {
                            sysTabSubPart2->touched = true;
                            metadata->schema->sysTabSubPartTouched = true;
                        }
                    } else if (object->columns[column]->name == "DATAOBJ#") {
                        if (updateNumber32u(sysTabSubPart2->dataObj, 0, column, object, rowId)) {
                            sysTabSubPart2->touched = true;
                            metadata->schema->sysTabSubPartTouched = true;
                        }
                    } else if (object->columns[column]->name == "POBJ#") {
                        if (updatePart(sysTabSubPart2->pObj, column, object, rowId)) {
                            sysTabSubPart2->touched = true;
                            metadata->schema->sysTabSubPartTouched = true;
                        }
                    }
                }
            }

        } else if (object->systemTable == TABLE_SYS_USER) {
            SysUser* sysUser2 = metadata->schema->dictSysUserFind(rowId);
            if (sysUser2 == nullptr) {
                TRACE(TRACE2_SYSTEM, "SYSTEM: missing row (rowid: " << rowId << ")")
                return;
            }

            typeCol column;
            uint64_t baseMax = builder->valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                column = (typeCol)(base << 6);
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (builder->valuesSet[base] < mask)
                        break;
                    if ((builder->valuesSet[base] & mask) == 0)
                        continue;

                    if (object->columns[column]->name == "USER#") {
                        if (updateUser(sysUser2->user, column, object, rowId)) {
                            sysUser2->touched = true;
                            metadata->schema->sysUserTouched = true;
                        }
                    } else if (object->columns[column]->name == "NAME") {
                        if (updateString(sysUser2->name, SYS_USER_NAME_LENGTH, column, object, rowId)) {
                            sysUser2->touched = true;
                            metadata->schema->sysUserTouched = true;
                        }
                    } else if (object->columns[column]->name == "SPARE1") {
                        if (updateNumberXu(sysUser2->spare1, column, object, rowId)) {
                            sysUser2->touched = true;
                            metadata->schema->sysUserTouched = true;
                        }
                    }
                }
            }
        }
    }

    void SystemTransaction::processDelete(OracleObject* object, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid __attribute__((unused))) {
        typeRowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        TRACE(TRACE2_SYSTEM, "SYSTEM: delete table (name: " << object->owner << "." << object->name << ", rowid: " << rowId << ")")

        switch (object->systemTable) {
            case TABLE_SYS_CCOL:
                metadata->schema->dictSysCColDrop(rowId);
                break;
            case TABLE_SYS_CDEF:
                metadata->schema->dictSysCDefDrop(rowId);
                break;
            case TABLE_SYS_COL:
                metadata->schema->dictSysColDrop(rowId);
                break;
            case TABLE_SYS_DEFERRED_STG:
                metadata->schema->dictSysDeferredStgDrop(rowId);
                break;
            case TABLE_SYS_ECOL:
                metadata->schema->dictSysEColDrop(rowId);
                break;
            case TABLE_SYS_LOB:
                metadata->schema->dictSysLobDrop(rowId);
                break;
            case TABLE_SYS_OBJ:
                metadata->schema->dictSysObjDrop(rowId);
                break;
            case TABLE_SYS_TAB:
                metadata->schema->dictSysTabDrop(rowId);
                break;
            case TABLE_SYS_TABCOMPART:
                metadata->schema->dictSysTabComPartDrop(rowId);
                break;
            case TABLE_SYS_TABPART:
                metadata->schema->dictSysTabPartDrop(rowId);
                break;
            case TABLE_SYS_TABSUBPART:
                metadata->schema->dictSysTabSubPartDrop(rowId);
                break;
            case TABLE_SYS_USER:
                metadata->schema->dictSysUserDrop(rowId);
                break;
        }
    }

    void SystemTransaction::commit(typeScn scn) {
        TRACE(TRACE2_SYSTEM, "SYSTEM: commit")

        if (!metadata->schema->touched)
            return;

        metadata->schema->scn = scn;
        metadata->schema->refreshIndexes(metadata->users);
        std::set<std::string> msgs;
        metadata->schema->rebuildMaps(msgs);
        for (auto msg : msgs) {
            INFO("dropped metadata: " << msg);
        }
        msgs.clear();

        for (SchemaElement* element : metadata->schemaElements)
            metadata->schema->buildMaps(element->owner, element->table, element->keys, element->keysStr, element->options, msgs,
                                        metadata->suppLogDbPrimary,
                                        metadata->suppLogDbAll, metadata->defaultCharacterMapId,
                                        metadata->defaultCharacterNcharMapId);
        for (auto msg: msgs) {
            INFO("updated metadata: " << msg)
        }
    }
}
