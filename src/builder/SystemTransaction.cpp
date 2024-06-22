/* System transaction to change metadata
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "../common/OracleTable.h"
#include "../common/exception/RuntimeException.h"
#include "../common/table/SysCCol.h"
#include "../common/table/SysCDef.h"
#include "../common/table/SysCol.h"
#include "../common/table/SysDeferredStg.h"
#include "../common/table/SysECol.h"
#include "../common/table/SysLob.h"
#include "../common/table/SysLobCompPart.h"
#include "../common/table/SysLobFrag.h"
#include "../common/table/SysObj.h"
#include "../common/table/SysTab.h"
#include "../common/table/SysTabComPart.h"
#include "../common/table/SysTabPart.h"
#include "../common/table/SysTabSubPart.h"
#include "../common/table/SysTs.h"
#include "../common/table/SysUser.h"
#include "../common/table/XdbTtSet.h"
#include "../common/table/XdbXNm.h"
#include "../common/table/XdbXPt.h"
#include "../common/table/XdbXQn.h"
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
            sysCColTmp(nullptr),
            sysCDefTmp(nullptr),
            sysColTmp(nullptr),
            sysDeferredStgTmp(nullptr),
            sysEColTmp(nullptr),
            sysLobTmp(nullptr),
            sysLobCompPartTmp(nullptr),
            sysLobFragTmp(nullptr),
            sysObjTmp(nullptr),
            sysTabTmp(nullptr),
            sysTabComPartTmp(nullptr),
            sysTabPartTmp(nullptr),
            sysTabSubPartTmp(nullptr),
            sysTsTmp(nullptr),
            sysUserTmp(nullptr),
            xdbTtSetTmp(nullptr),
            xdbXNmTmp(nullptr),
            xdbXPtTmp(nullptr),
            xdbXQnTmp(nullptr) {
        ctx->logTrace(Ctx::TRACE_SYSTEM, "begin");
    }

    SystemTransaction::~SystemTransaction() {
        if (sysCColTmp != nullptr) {
            delete sysCColTmp;
            sysCColTmp = nullptr;
        }

        if (sysCDefTmp != nullptr) {
            delete sysCDefTmp;
            sysCDefTmp = nullptr;
        }

        if (sysColTmp != nullptr) {
            delete sysColTmp;
            sysColTmp = nullptr;
        }

        if (sysDeferredStgTmp != nullptr) {
            delete sysDeferredStgTmp;
            sysDeferredStgTmp = nullptr;
        }

        if (sysEColTmp != nullptr) {
            delete sysEColTmp;
            sysEColTmp = nullptr;
        }

        if (sysLobTmp != nullptr) {
            delete sysLobTmp;
            sysLobTmp = nullptr;
        }

        if (sysLobCompPartTmp != nullptr) {
            delete sysLobCompPartTmp;
            sysLobCompPartTmp = nullptr;
        }

        if (sysLobFragTmp != nullptr) {
            delete sysLobFragTmp;
            sysLobFragTmp = nullptr;
        }

        if (sysObjTmp != nullptr) {
            delete sysObjTmp;
            sysObjTmp = nullptr;
        }

        if (sysTabTmp != nullptr) {
            delete sysTabTmp;
            sysTabTmp = nullptr;
        }

        if (sysTabComPartTmp != nullptr) {
            delete sysTabComPartTmp;
            sysTabComPartTmp = nullptr;
        }

        if (sysTabPartTmp != nullptr) {
            delete sysTabPartTmp;
            sysTabPartTmp = nullptr;
        }

        if (sysTabSubPartTmp != nullptr) {
            delete sysTabSubPartTmp;
            sysTabSubPartTmp = nullptr;
        }

        if (sysTsTmp != nullptr) {
            delete sysTsTmp;
            sysTsTmp = nullptr;
        }

        if (sysUserTmp != nullptr) {
            delete sysUserTmp;
            sysUserTmp = nullptr;
        }

        if (xdbTtSetTmp != nullptr) {
            delete xdbTtSetTmp;
            xdbTtSetTmp = nullptr;
        }

        if (xdbXNmTmp != nullptr) {
            delete xdbXNmTmp;
            xdbXNmTmp = nullptr;
        }

        if (xdbXPtTmp != nullptr) {
            delete xdbXPtTmp;
            xdbXPtTmp = nullptr;
        }

        if (xdbXQnTmp != nullptr) {
            delete xdbXQnTmp;
            xdbXQnTmp = nullptr;
        }
    }

    void SystemTransaction::updateNumber16(int16_t& val, int16_t defVal, typeCol column, const OracleTable* table, uint64_t offset) {
        if (builder->values[column][Builder::Builder::VALUE_AFTER] != nullptr && builder->sizes[column][Builder::Builder::VALUE_AFTER] > 0) {
            char* retPtr;
            if (unlikely(table->columns[column]->type != 2))
                throw RuntimeException(50019, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " type found " + std::to_string(table->columns[column]->type) + " offset: " +
                                              std::to_string(offset));

            builder->parseNumber(builder->values[column][Builder::Builder::VALUE_AFTER], builder->sizes[column][Builder::Builder::VALUE_AFTER], offset);
            builder->valueBuffer[builder->valueSize] = 0;
            auto newVal = static_cast<int16_t>(strtol(builder->valueBuffer, &retPtr, 10));
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> " +
                                                 std::to_string(newVal) + ")");
            val = newVal;
        } else if (builder->values[column][Builder::Builder::VALUE_AFTER] != nullptr || builder->values[column][Builder::Builder::VALUE_BEFORE] != nullptr) {
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> NULL)");
            val = defVal;
        }
    }

    void SystemTransaction::updateNumber16u(uint16_t& val, uint16_t defVal, typeCol column, const OracleTable* table, uint64_t offset) {
        if (builder->values[column][Builder::Builder::VALUE_AFTER] != nullptr && builder->sizes[column][Builder::Builder::VALUE_AFTER] > 0) {
            char* retPtr;
            if (unlikely(table->columns[column]->type != 2))
                throw RuntimeException(50019, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " type found " + std::to_string(table->columns[column]->type) + " offset: " +
                                              std::to_string(offset));

            builder->parseNumber(builder->values[column][Builder::Builder::Builder::VALUE_AFTER],
                                 builder->sizes[column][Builder::Builder::Builder::VALUE_AFTER], offset);
            builder->valueBuffer[builder->valueSize] = 0;
            if (unlikely(builder->valueSize == 0 || builder->valueBuffer[0] == '-'))
                throw RuntimeException(50020, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " value found " + builder->valueBuffer + " offset: " + std::to_string(offset));

            uint16_t newVal = strtoul(builder->valueBuffer, &retPtr, 10);
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> " +
                                            std::to_string(newVal) + ")");
            val = newVal;
        } else if (builder->values[column][Builder::Builder::VALUE_AFTER] != nullptr || builder->values[column][Builder::VALUE_BEFORE] != nullptr) {
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> NULL)");
            val = defVal;
        }
    }

    void SystemTransaction::updateNumber32u(uint32_t& val, uint32_t defVal, typeCol column, const OracleTable* table, uint64_t offset) {
        if (builder->values[column][Builder::Builder::VALUE_AFTER] != nullptr && builder->sizes[column][Builder::Builder::VALUE_AFTER] > 0) {
            char* retPtr;
            if (unlikely(table->columns[column]->type != 2))
                throw RuntimeException(50019, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " type found " + std::to_string(table->columns[column]->type) + " offset: " +
                                              std::to_string(offset));

            builder->parseNumber(builder->values[column][Builder::Builder::VALUE_AFTER], builder->sizes[column][Builder::VALUE_AFTER], offset);
            builder->valueBuffer[builder->valueSize] = 0;
            if (unlikely(builder->valueSize == 0 || builder->valueBuffer[0] == '-'))
                throw RuntimeException(50020, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " value found " + builder->valueBuffer + " offset: " + std::to_string(offset));

            uint32_t newVal = strtoul(builder->valueBuffer, &retPtr, 10);
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> " +
                                                 std::to_string(newVal) + ")");
            val = newVal;
        } else if (builder->values[column][Builder::VALUE_AFTER] != nullptr || builder->values[column][Builder::VALUE_BEFORE] != nullptr) {
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> NULL)");
            val = defVal;
        }
    }

    void SystemTransaction::updateNumber64(int64_t& val, int64_t defVal, typeCol column, const OracleTable* table, uint64_t offset) {
        if (builder->values[column][Builder::VALUE_AFTER] != nullptr && builder->sizes[column][Builder::VALUE_AFTER] > 0) {
            char* retPtr;
            if (unlikely(table->columns[column]->type != 2))
                throw RuntimeException(50019, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " type found " + std::to_string(table->columns[column]->type) + " offset: " +
                                              std::to_string(offset));

            builder->parseNumber(builder->values[column][Builder::VALUE_AFTER], builder->sizes[column][Builder::VALUE_AFTER], offset);
            builder->valueBuffer[builder->valueSize] = 0;
            if (unlikely(builder->valueSize == 0))
                throw RuntimeException(50020, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " value found " + builder->valueBuffer + " offset: " + std::to_string(offset));

            int64_t newVal = strtol(builder->valueBuffer, &retPtr, 10);
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> " +
                                                 std::to_string(newVal) + ")");
            val = newVal;
        } else if (builder->values[column][Builder::VALUE_AFTER] != nullptr || builder->values[column][Builder::VALUE_BEFORE] != nullptr) {
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> NULL)");
            val = defVal;
        }
    }

    void SystemTransaction::updateNumber64u(uint64_t& val, uint64_t defVal, typeCol column, const OracleTable* table, uint64_t offset) {
        if (builder->values[column][Builder::VALUE_AFTER] != nullptr && builder->sizes[column][Builder::VALUE_AFTER] > 0) {
            char* retPtr;
            if (unlikely(table->columns[column]->type != 2))
                throw RuntimeException(50019, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " type found " + std::to_string(table->columns[column]->type) + " offset: " +
                                              std::to_string(offset));

            builder->parseNumber(builder->values[column][Builder::VALUE_AFTER], builder->sizes[column][Builder::VALUE_AFTER], offset);
            builder->valueBuffer[builder->valueSize] = 0;
            if (unlikely(builder->valueSize == 0 || builder->valueBuffer[0] == '-'))
                throw RuntimeException(50020, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " value found " + builder->valueBuffer + " offset: " + std::to_string(offset));

            uint64_t newVal = strtoul(builder->valueBuffer, &retPtr, 10);
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> " +
                                                 std::to_string(newVal) + ")");
            val = newVal;
        } else if (builder->values[column][Builder::VALUE_AFTER] != nullptr || builder->values[column][Builder::VALUE_BEFORE] != nullptr) {
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> NULL)");
            val = defVal;
        }
    }

    void SystemTransaction::updateNumberXu(typeIntX& val, typeCol column, const OracleTable* table, uint64_t offset) {
        if (builder->values[column][Builder::VALUE_AFTER] != nullptr && builder->sizes[column][Builder::VALUE_AFTER] > 0) {
            if (unlikely(table->columns[column]->type != 2))
                throw RuntimeException(50019, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " type found " + std::to_string(table->columns[column]->type) + " offset: " +
                                              std::to_string(offset));

            builder->parseNumber(builder->values[column][Builder::VALUE_AFTER], builder->sizes[column][Builder::VALUE_AFTER], offset);
            builder->valueBuffer[builder->valueSize] = 0;
            if (unlikely(builder->valueSize == 0 || builder->valueBuffer[0] == '-'))
                throw RuntimeException(50020, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " value found " + builder->valueBuffer + " offset: " + std::to_string(offset));

            typeIntX newVal(0);
            std::string err;
            newVal.setStr(builder->valueBuffer, builder->valueSize, err);
            if (err != "")
                ctx->error(50021, err);

            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + val.toString() + " -> " + newVal.toString() + ")");
            val = newVal;
        } else if (builder->values[column][Builder::VALUE_AFTER] != nullptr || builder->values[column][Builder::VALUE_BEFORE] != nullptr) {
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": " + val.toString() + " -> NULL)");
            val.set(0, 0);
        }
    }

    void SystemTransaction::updateRaw(std::string& val, uint64_t maxLength, typeCol column, const OracleTable* table, uint64_t offset) {
        if (builder->values[column][Builder::VALUE_AFTER] != nullptr && builder->sizes[column][Builder::VALUE_AFTER] > 0) {
            if (unlikely(table->columns[column]->type != SysCol::TYPE_RAW))
                throw RuntimeException(50019, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " type found " + std::to_string(table->columns[column]->type) + " offset: " +
                                              std::to_string(offset));

            builder->parseRaw(builder->values[column][Builder::VALUE_AFTER], builder->sizes[column][Builder::VALUE_AFTER], offset);
            std::string newVal(builder->valueBuffer, builder->valueSize);
            if (unlikely(builder->valueSize > maxLength))
                throw RuntimeException(50020, "ddl: value too long for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + ", length " + std::to_string(builder->valueSize) + " offset: " +
                                              std::to_string(offset));

            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": '" + val + "' -> '" + newVal + "')");
            val = newVal;
        } else if (builder->values[column][Builder::VALUE_AFTER] != nullptr || builder->values[column][Builder::VALUE_BEFORE] != nullptr) {
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": '" + val + "' -> NULL)");
            val.assign("");
        }
    }

    void SystemTransaction::updateString(std::string& val, uint64_t maxLength, typeCol column, const OracleTable* table, uint64_t offset) {
        if (builder->values[column][Builder::VALUE_AFTER] != nullptr && builder->sizes[column][Builder::VALUE_AFTER] > 0) {
            if (unlikely(table->columns[column]->type != SysCol::TYPE_VARCHAR && table->columns[column]->type != SysCol::TYPE_CHAR))
                throw RuntimeException(50019, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + " type found " + std::to_string(table->columns[column]->type) + " offset: " +
                                              std::to_string(offset));

            builder->parseString(builder->values[column][Builder::VALUE_AFTER], builder->sizes[column][Builder::VALUE_AFTER],
                                 table->columns[column]->charsetId, offset, false, false, false, true);
            std::string newVal(builder->valueBuffer, builder->valueSize);
            if (unlikely(builder->valueSize > maxLength))
                throw RuntimeException(50020, "ddl: value too long for " + table->owner + "." + table->name + ": column " +
                                              table->columns[column]->name + ", length " + std::to_string(builder->valueSize) + " offset: " +
                                              std::to_string(offset));

            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": '" + val + "' -> '" + newVal + "')");
            val = newVal;
        } else if (builder->values[column][Builder::VALUE_AFTER] != nullptr || builder->values[column][Builder::VALUE_BEFORE] != nullptr) {
            if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                ctx->logTrace(Ctx::TRACE_SYSTEM, "set (" + table->columns[column]->name + ": '" + val + "' -> NULL)");
            val.assign("");
        }
    }

    void SystemTransaction::processInsertSysCCol(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysCCol* sysCCol = metadata->schema->dictSysCColFind(rowId);
        if (sysCCol != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.CCOL$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysCColDrop(sysCCol);
            delete sysCCol;
        }
        sysCColTmp = new SysCCol(rowId, 0, 0, 0, 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "CON#") {
                    updateNumber32u(sysCColTmp->con, 0, column, table, offset);
                } else if (table->columns[column]->name == "INTCOL#") {
                    updateNumber16(sysCColTmp->intCol, 0, column, table, offset);
                } else if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysCColTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "SPARE1") {
                    updateNumberXu(sysCColTmp->spare1, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysCColAdd(sysCColTmp);
        sysCColTmp = nullptr;
    }

    void SystemTransaction::processInsertSysCDef(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysCDef* sysCDef = metadata->schema->dictSysCDefFind(rowId);
        if (sysCDef != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.CDEF$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysCDefDrop(sysCDef);
            delete sysCDef;
        }
        sysCDefTmp = new SysCDef(rowId, 0, 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "CON#") {
                    updateNumber32u(sysCDefTmp->con, 0, column, table, offset);
                } else if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysCDefTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "TYPE#") {
                    updateNumber16u(sysCDefTmp->type, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysCDefAdd(sysCDefTmp);
        sysCDefTmp = nullptr;
    }

    void SystemTransaction::processInsertSysCol(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysCol* sysCol = metadata->schema->dictSysColFind(rowId);
        if (sysCol != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.COL$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysColDrop(sysCol);
            delete sysCol;
        }
        sysColTmp = new SysCol(rowId, 0, 0, 0, 0, "", 0, 0, -1, -1,
                               0, 0, 0, 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysColTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "COL#") {
                    updateNumber16(sysColTmp->col, 0, column, table, offset);
                } else if (table->columns[column]->name == "SEGCOL#") {
                    updateNumber16(sysColTmp->segCol, 0, column, table, offset);
                } else if (table->columns[column]->name == "INTCOL#") {
                    updateNumber16(sysColTmp->intCol, 0, column, table, offset);
                } else if (table->columns[column]->name == "NAME") {
                    updateString(sysColTmp->name, SysCol::NAME_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "TYPE#") {
                    updateNumber16u(sysColTmp->type, 0, column, table, offset);
                } else if (table->columns[column]->name == "SIZE") {
                    updateNumber64u(sysColTmp->length, 0, column, table, offset);
                } else if (table->columns[column]->name == "PRECISION#") {
                    updateNumber64(sysColTmp->precision, -1, column, table, offset);
                } else if (table->columns[column]->name == "SCALE") {
                    updateNumber64(sysColTmp->scale, -1, column, table, offset);
                } else if (table->columns[column]->name == "CHARSETFORM") {
                    updateNumber64u(sysColTmp->charsetForm, 0, column, table, offset);
                } else if (table->columns[column]->name == "CHARSETID") {
                    updateNumber64u(sysColTmp->charsetId, 0, column, table, offset);
                } else if (table->columns[column]->name == "NULL$") {
                    updateNumber64(sysColTmp->null_, 0, column, table, offset);
                } else if (table->columns[column]->name == "PROPERTY") {
                    updateNumberXu(sysColTmp->property, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysColAdd(sysColTmp);
        sysColTmp = nullptr;
    }

    void SystemTransaction::processInsertSysDeferredStg(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysDeferredStg* sysDeferredStg = metadata->schema->dictSysDeferredStgFind(rowId);
        if (sysDeferredStg != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.DEFERRED_STG$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysDeferredStgDrop(sysDeferredStg);
            delete sysDeferredStg;
        }
        sysDeferredStgTmp = new SysDeferredStg(rowId, 0, 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysDeferredStgTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "FLAGS_STG") {
                    updateNumberXu(sysDeferredStgTmp->flagsStg, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysDeferredStgAdd(sysDeferredStgTmp);
        sysDeferredStgTmp = nullptr;
    }

    void SystemTransaction::processInsertSysECol(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysECol* sysECol = metadata->schema->dictSysEColFind(rowId);
        if (sysECol != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.ECOL$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysEColDrop(sysECol);
            delete sysECol;
        }
        sysEColTmp = new SysECol(rowId, 0, 0, -1);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "TABOBJ#") {
                    updateNumber32u(sysEColTmp->tabObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "COLNUM") {
                    updateNumber16(sysEColTmp->colNum, 0, column, table, offset);
                } else if (table->columns[column]->name == "GUARD_ID") {
                    updateNumber16(sysEColTmp->guardId, -1, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysEColAdd(sysEColTmp);
        sysEColTmp = nullptr;
    }

    void SystemTransaction::processInsertSysLob(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysLob* sysLob = metadata->schema->dictSysLobFind(rowId);
        if (sysLob != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.LOB$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysLobDrop(sysLob);
            delete sysLob;
        }
        sysLobTmp = new SysLob(rowId, 0, 0, 0, 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysLobTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "COL#") {
                    updateNumber16(sysLobTmp->col, 0, column, table, offset);
                } else if (table->columns[column]->name == "INTCOL#") {
                    updateNumber16(sysLobTmp->intCol, 0, column, table, offset);
                } else if (table->columns[column]->name == "LOBJ#") {
                    updateNumber32u(sysLobTmp->lObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "TS#") {
                    updateNumber32u(sysLobTmp->ts, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysLobAdd(sysLobTmp);
        sysLobTmp = nullptr;
    }

    void SystemTransaction::processInsertSysLobCompPart(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysLobCompPart* sysLobCompPart = metadata->schema->dictSysLobCompPartFind(rowId);
        if (sysLobCompPart != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.LOBCOMPPART$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysLobCompPartDrop(sysLobCompPart);
            delete sysLobCompPart;
        }
        sysLobCompPartTmp = new SysLobCompPart(rowId, 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "PARTOBJ#") {
                    updateNumber32u(sysLobCompPartTmp->partObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "LOBJ#") {
                    updateNumber32u(sysLobCompPartTmp->lObj, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysLobCompPartAdd(sysLobCompPartTmp);
        sysLobCompPartTmp = nullptr;
    }

    void SystemTransaction::processInsertSysLobFrag(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysLobFrag* sysLobFrag = metadata->schema->dictSysLobFragFind(rowId);
        if (sysLobFrag != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.LOBFRAG$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysLobFragDrop(sysLobFrag);
            delete sysLobFrag;
        }
        sysLobFragTmp = new SysLobFrag(rowId, 0, 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "FRAGOBJ#") {
                    updateNumber32u(sysLobFragTmp->fragObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "PARENTOBJ#") {
                    updateNumber32u(sysLobFragTmp->parentObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "TS#") {
                    updateNumber32u(sysLobFragTmp->ts, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysLobFragAdd(sysLobFragTmp);
        sysLobFragTmp = nullptr;
    }

    void SystemTransaction::processInsertSysObj(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysObj* sysObj = metadata->schema->dictSysObjFind(rowId);
        if (sysObj != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.OBJ$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysObjDrop(sysObj);
            delete sysObj;
        }
        sysObjTmp = new SysObj(rowId, 0, 0, 0, 0, "", 0, 0, false);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OWNER#") {
                    updateNumber32u(sysObjTmp->owner, 0, column, table, offset);
                } else if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysObjTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "DATAOBJ#") {
                    updateNumber32u(sysObjTmp->dataObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "NAME") {
                    updateString(sysObjTmp->name, SysObj::NAME_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "TYPE#") {
                    updateNumber16u(sysObjTmp->type, 0, column, table, offset);
                } else if (table->columns[column]->name == "FLAGS") {
                    updateNumberXu(sysObjTmp->flags, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysObjAdd(sysObjTmp);
        sysObjTmp = nullptr;
    }

    void SystemTransaction::processInsertSysTab(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysTab* sysTab = metadata->schema->dictSysTabFind(rowId);
        if (sysTab != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.TAB$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysTabDrop(sysTab);
            delete sysTab;
        }
        sysTabTmp = new SysTab(rowId, 0, 0, 0, 0, 0, 0, 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysTabTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "DATAOBJ#") {
                    updateNumber32u(sysTabTmp->dataObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "TS#") {
                    updateNumber32u(sysTabTmp->ts, 0, column, table, offset);
                } else if (table->columns[column]->name == "CLUCOLS") {
                    updateNumber16(sysTabTmp->cluCols, 0, column, table, offset);
                } else if (table->columns[column]->name == "FLAGS") {
                    updateNumberXu(sysTabTmp->flags, column, table, offset);
                } else if (table->columns[column]->name == "PROPERTY") {
                    updateNumberXu(sysTabTmp->property, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysTabAdd(sysTabTmp);
        sysTabTmp = nullptr;
    }

    void SystemTransaction::processInsertSysTabComPart(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysTabComPart* sysTabComPart = metadata->schema->dictSysTabComPartFind(rowId);
        if (sysTabComPart != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.TABCOMPART$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysTabComPartDrop(sysTabComPart);
            delete sysTabComPart;
        }
        sysTabComPartTmp = new SysTabComPart(rowId, 0, 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysTabComPartTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "DATAOBJ#") {
                    updateNumber32u(sysTabComPartTmp->dataObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "BO#") {
                    updateNumber32u(sysTabComPartTmp->bo, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysTabComPartAdd(sysTabComPartTmp);
        sysTabComPartTmp = nullptr;
    }

    void SystemTransaction::processInsertSysTabPart(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysTabPart* sysTabPart = metadata->schema->dictSysTabPartFind(rowId);
        if (sysTabPart != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.TABPART$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysTabPartDrop(sysTabPart);
            delete sysTabPart;
        }
        sysTabPartTmp = new SysTabPart(rowId, 0, 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysTabPartTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "DATAOBJ#") {
                    updateNumber32u(sysTabPartTmp->dataObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "BO#") {
                    updateNumber32u(sysTabPartTmp->bo, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysTabPartAdd(sysTabPartTmp);
        sysTabPartTmp = nullptr;
    }

    void SystemTransaction::processInsertSysTabSubPart(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysTabSubPart* sysTabSubPart = metadata->schema->dictSysTabSubPartFind(rowId);
        if (sysTabSubPart != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.TABSUBPART$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysTabSubPartDrop(sysTabSubPart);
            delete sysTabSubPart;
        }
        sysTabSubPartTmp = new SysTabSubPart(rowId, 0, 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysTabSubPartTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "DATAOBJ#") {
                    updateNumber32u(sysTabSubPartTmp->dataObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "POBJ#") {
                    updateNumber32u(sysTabSubPartTmp->pObj, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysTabSubPartAdd(sysTabSubPartTmp);
        sysTabSubPartTmp = nullptr;
    }

    void SystemTransaction::processInsertSysTs(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysTs* sysTs = metadata->schema->dictSysTsFind(rowId);
        if (sysTs != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.TS$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysTsDrop(sysTs);
            delete sysTs;
        }
        sysTsTmp = new SysTs(rowId, 0, "", 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "TS#") {
                    updateNumber32u(sysTsTmp->ts, 0, column, table, offset);
                } else if (table->columns[column]->name == "NAME") {
                    updateString(sysTsTmp->name, SysTs::NAME_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "BLOCKSIZE") {
                    updateNumber32u(sysTsTmp->blockSize, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysTsAdd(sysTsTmp);
        sysTsTmp = nullptr;
    }

    void SystemTransaction::processInsertSysUser(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        SysUser* sysUser = metadata->schema->dictSysUserFind(rowId);
        if (sysUser != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate SYS.USER$: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictSysUserDrop(sysUser);
            delete sysUser;
        }
        sysUserTmp = new SysUser(rowId, 0, "", 0, 0, false);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "USER#") {
                    updateNumber32u(sysUserTmp->user, 0, column, table, offset);
                } else if (table->columns[column]->name == "NAME") {
                    updateString(sysUserTmp->name, SysUser::NAME_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "SPARE1") {
                    updateNumberXu(sysUserTmp->spare1, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysUserAdd(sysUserTmp);
        sysUserTmp = nullptr;
    }

    void SystemTransaction::processInsertXdbTtSet(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        XdbTtSet* xdbTtSet = metadata->schema->dictXdbTtSetFind(rowId);
        if (xdbTtSet != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate XDB.XDB$TTSET: (rowid: " + rowId.toString() + ") for insert at offset: " +
                                              std::to_string(offset));
            metadata->schema->dictXdbTtSetDrop(xdbTtSet);
            delete xdbTtSet;
        }
        xdbTtSetTmp = new XdbTtSet(rowId, "", "", 0, 0);

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "GUID") {
                    updateRaw(xdbTtSetTmp->guid, XdbTtSet::GUID_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "TOKSUF") {
                    updateString(xdbTtSetTmp->tokSuf, XdbTtSet::TOKSUF_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "FLAGS") {
                    updateNumber64u(xdbTtSetTmp->flags, 0, column, table, offset);
                } else if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(xdbTtSetTmp->obj, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictXdbTtSetAdd(xdbTtSetTmp);
        xdbTtSetTmp = nullptr;
    }

    void SystemTransaction::processInsertXdbXNm(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        XdbXNm* xdbXNm = metadata->schema->dictXdbXNmFind(table->tokSuf, rowId);
        if (xdbXNm != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate XDB.X$NM" + table->tokSuf + ": (rowid: " + rowId.toString() +
                                              ") for insert at offset: " + std::to_string(offset));
            metadata->schema->dictXdbXNmDrop(table->tokSuf, xdbXNm);
            delete xdbXNm;
        }
        xdbXNmTmp = new XdbXNm(rowId, "", "");

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "NMSPCURI") {
                    updateString(xdbXNmTmp->nmSpcUri, XdbXNm::NMSPCURI_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "ID") {
                    updateRaw(xdbXNmTmp->id, XdbXNm::ID_LENGTH, column, table, offset);
                }
            }
        }

        metadata->schema->dictXdbXNmAdd(table->tokSuf, xdbXNmTmp);
        xdbXNmTmp = nullptr;
    }

    void SystemTransaction::processInsertXdbXPt(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        XdbXPt* xdbXPt = metadata->schema->dictXdbXPtFind(table->tokSuf, rowId);
        if (xdbXPt != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate XDB.X$PT" + table->tokSuf + ": (rowid: " + rowId.toString() +
                                              ") for insert at offset: " + std::to_string(offset));
            metadata->schema->dictXdbXPtDrop(table->tokSuf, xdbXPt);
            delete xdbXPt;
        }
        xdbXPtTmp = new XdbXPt(rowId, "", "");

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "PATH") {
                    updateRaw(xdbXPtTmp->path, XdbXPt::PATH_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "ID") {
                    updateRaw(xdbXPtTmp->id, XdbXPt::ID_LENGTH, column, table, offset);
                }
            }
        }

        metadata->schema->dictXdbXPtAdd(table->tokSuf, xdbXPtTmp);
        xdbXPtTmp = nullptr;
    }

    void SystemTransaction::processInsertXdbXQn(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        XdbXQn* xdbXQn = metadata->schema->dictXdbXQnFind(table->tokSuf, rowId);
        if (xdbXQn != nullptr) {
            if (unlikely(!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)))
                throw RuntimeException(50022, "ddl: duplicate XDB.X$QN" + table->tokSuf + ": (rowid: " + rowId.toString() +
                                              ") for insert at offset: " + std::to_string(offset));
            metadata->schema->dictXdbXQnDrop(table->tokSuf, xdbXQn);
            delete xdbXQn;
        }
        xdbXQnTmp = new XdbXQn(rowId, "", "", "", "");

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;
                if (table->columns[column]->name == "NMSPCID") {
                    updateRaw(xdbXQnTmp->nmSpcId, XdbXQn::NMSPCID_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "LOCALNAME") {
                    updateString(xdbXQnTmp->localName, XdbXQn::LOCALNAME_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "FLAGS") {
                    updateRaw(xdbXQnTmp->flags, XdbXQn::FLAGS_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "ID") {
                    updateRaw(xdbXQnTmp->id, XdbXQn::ID_LENGTH, column, table, offset);
                }
            }
        }

        metadata->schema->dictXdbXQnAdd(table->tokSuf, xdbXQnTmp);
        xdbXQnTmp = nullptr;
    }

    void SystemTransaction::processInsert(const OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset) {
        typeRowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
            ctx->logTrace(Ctx::TRACE_SYSTEM, "insert table (name: " + table->owner + "." + table->name + ", rowid: " + rowId.toString() + ")");

        switch (table->systemTable) {
            case OracleTable::SYS_CCOL:
                processInsertSysCCol(table, rowId, offset);
                break;

            case OracleTable::SYS_CDEF:
                processInsertSysCDef(table, rowId, offset);
                break;

            case OracleTable::SYS_COL:
                processInsertSysCol(table, rowId, offset);
                break;

            case OracleTable::SYS_DEFERRED_STG:
                processInsertSysDeferredStg(table, rowId, offset);
                break;

            case OracleTable::SYS_ECOL:
                processInsertSysECol(table, rowId, offset);
                break;

            case OracleTable::SYS_LOB:
                processInsertSysLob(table, rowId, offset);
                break;

            case OracleTable::SYS_LOB_COMP_PART:
                processInsertSysLobCompPart(table, rowId, offset);
                break;

            case OracleTable::SYS_LOB_FRAG:
                processInsertSysLobFrag(table, rowId, offset);
                break;

            case OracleTable::SYS_OBJ:
                processInsertSysObj(table, rowId, offset);
                break;

            case OracleTable::SYS_TAB:
                processInsertSysTab(table, rowId, offset);
                break;

            case OracleTable::SYS_TABCOMPART:
                processInsertSysTabComPart(table, rowId, offset);
                break;

            case OracleTable::SYS_TABPART:
                processInsertSysTabPart(table, rowId, offset);
                break;

            case OracleTable::SYS_TABSUBPART:
                processInsertSysTabSubPart(table, rowId, offset);
                break;

            case OracleTable::SYS_TS:
                processInsertSysTs(table, rowId, offset);
                break;

            case OracleTable::SYS_USER:
                processInsertSysUser(table, rowId, offset);
                break;

            case OracleTable::XDB_TTSET:
                processInsertXdbTtSet(table, rowId, offset);
                break;

            case OracleTable::XDB_XNM:
                processInsertXdbXNm(table, rowId, offset);
                break;

            case OracleTable::XDB_XPT:
                processInsertXdbXPt(table, rowId, offset);
                break;

            case OracleTable::XDB_XQN:
                processInsertXdbXQn(table, rowId, offset);
                break;
        }
    }

    void SystemTransaction::processUpdateSysCCol(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysCColTmp = metadata->schema->dictSysCColFind(rowId);
        if (sysCColTmp != nullptr) {
            metadata->schema->dictSysCColDrop(sysCColTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.CCOL$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysCColTmp = new SysCCol(rowId, 0, 0, 0, 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "CON#") {
                    updateNumber32u(sysCColTmp->con, 0, column, table, offset);
                } else if (table->columns[column]->name == "INTCOL#") {
                    updateNumber16(sysCColTmp->intCol, 0, column, table, offset);
                } else if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysCColTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "SPARE1") {
                    updateNumberXu(sysCColTmp->spare1, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysCColAdd(sysCColTmp);
        sysCColTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysCDef(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysCDefTmp = metadata->schema->dictSysCDefFind(rowId);
        if (sysCDefTmp != nullptr) {
            metadata->schema->dictSysCDefDrop(sysCDefTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.CDEF$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysCDefTmp = new SysCDef(rowId, 0, 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "CON#") {
                    updateNumber32u(sysCDefTmp->con, 0, column, table, offset);
                } else if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysCDefTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "TYPE#") {
                    updateNumber16u(sysCDefTmp->type, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysCDefAdd(sysCDefTmp);
        sysCDefTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysCol(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysColTmp = metadata->schema->dictSysColFind(rowId);
        if (sysColTmp != nullptr) {
            metadata->schema->dictSysColDrop(sysColTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.COL$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysColTmp = new SysCol(rowId, 0, 0, 0, 0, "", 0, 0, -1, -1,
                                   0, 0, 0, 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysColTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "COL#") {
                    updateNumber16(sysColTmp->col, 0, column, table, offset);
                } else if (table->columns[column]->name == "SEGCOL#") {
                    updateNumber16(sysColTmp->segCol, 0, column, table, offset);
                } else if (table->columns[column]->name == "INTCOL#") {
                    updateNumber16(sysColTmp->intCol, 0, column, table, offset);
                } else if (table->columns[column]->name == "NAME") {
                    updateString(sysColTmp->name, SysCol::NAME_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "TYPE#") {
                    updateNumber16u(sysColTmp->type, 0, column, table, offset);
                } else if (table->columns[column]->name == "SIZE") {
                    updateNumber64u(sysColTmp->length, 0, column, table, offset);
                } else if (table->columns[column]->name == "PRECISION#") {
                    updateNumber64(sysColTmp->precision, -1, column, table, offset);
                } else if (table->columns[column]->name == "SCALE") {
                    updateNumber64(sysColTmp->scale, -1, column, table, offset);
                } else if (table->columns[column]->name == "CHARSETFORM") {
                    updateNumber64u(sysColTmp->charsetForm, 0, column, table, offset);
                } else if (table->columns[column]->name == "CHARSETID") {
                    updateNumber64u(sysColTmp->charsetId, 0, column, table, offset);
                } else if (table->columns[column]->name == "NULL$") {
                    updateNumber64(sysColTmp->null_, 0, column, table, offset);
                } else if (table->columns[column]->name == "PROPERTY") {
                    updateNumberXu(sysColTmp->property, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysColAdd(sysColTmp);
        sysColTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysDeferredStg(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysDeferredStgTmp = metadata->schema->dictSysDeferredStgFind(rowId);
        if (sysDeferredStgTmp != nullptr) {
            metadata->schema->dictSysDeferredStgDrop(sysDeferredStgTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.DEFERRED_STG$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysDeferredStgTmp = new SysDeferredStg(rowId, 0, 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysDeferredStgTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "FLAGS_STG") {
                    updateNumberXu(sysDeferredStgTmp->flagsStg, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysDeferredStgAdd(sysDeferredStgTmp);
        sysDeferredStgTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysECol(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysEColTmp = metadata->schema->dictSysEColFind(rowId);
        if (sysEColTmp != nullptr) {
            metadata->schema->dictSysEColDrop(sysEColTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.ECOL$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysEColTmp = new SysECol(rowId, 0, 0, -1);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "TABOBJ#") {
                    updateNumber32u(sysEColTmp->tabObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "COLNUM") {
                    updateNumber16(sysEColTmp->colNum, 0, column, table, offset);
                } else if (table->columns[column]->name == "GUARD_ID") {
                    updateNumber16(sysEColTmp->guardId, -1, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysEColAdd(sysEColTmp);
        sysEColTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysLob(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysLobTmp = metadata->schema->dictSysLobFind(rowId);
        if (sysLobTmp != nullptr) {
            metadata->schema->dictSysLobDrop(sysLobTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.LOB$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysLobTmp = new SysLob(rowId, 0, 0, 0, 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysLobTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "COL#") {
                    updateNumber16(sysLobTmp->col, 0, column, table, offset);
                } else if (table->columns[column]->name == "INTCOL#") {
                    updateNumber16(sysLobTmp->intCol, 0, column, table, offset);
                } else if (table->columns[column]->name == "LOBJ#") {
                    updateNumber32u(sysLobTmp->lObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "TS#") {
                    updateNumber32u(sysLobTmp->ts, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysLobAdd(sysLobTmp);
        sysLobTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysLobCompPart(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysLobCompPartTmp = metadata->schema->dictSysLobCompPartFind(rowId);
        if (sysLobCompPartTmp != nullptr) {
            metadata->schema->dictSysLobCompPartDrop(sysLobCompPartTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.LOBCOMPPART$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysLobCompPartTmp = new SysLobCompPart(rowId, 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "PARTOBJ#") {
                    updateNumber32u(sysLobCompPartTmp->partObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "LOBJ#") {
                    updateNumber32u(sysLobCompPartTmp->lObj, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysLobCompPartAdd(sysLobCompPartTmp);
        sysLobCompPartTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysLobFrag(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysLobFragTmp = metadata->schema->dictSysLobFragFind(rowId);
        if (sysLobFragTmp != nullptr) {
            metadata->schema->dictSysLobFragDrop(sysLobFragTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.LOBFRAG$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysLobFragTmp = new SysLobFrag(rowId, 0, 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "FRAGOBJ#") {
                    updateNumber32u(sysLobFragTmp->fragObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "PARENTOBJ#") {
                    updateNumber32u(sysLobFragTmp->parentObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "TS#") {
                    updateNumber32u(sysLobFragTmp->ts, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysLobFragAdd(sysLobFragTmp);
        sysLobFragTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysObj(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysObjTmp = metadata->schema->dictSysObjFind(rowId);
        if (sysObjTmp != nullptr) {
            metadata->schema->dictSysObjDrop(sysObjTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.OBJ$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysObjTmp = new SysObj(rowId, 0, 0, 0, 0, "", 0, 0, false);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OWNER#") {
                    updateNumber32u(sysObjTmp->owner, 0, column, table, offset);
                } else if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysObjTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "DATAOBJ#") {
                    updateNumber32u(sysObjTmp->dataObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "NAME") {
                    updateString(sysObjTmp->name, SysObj::NAME_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "TYPE#") {
                    updateNumber16u(sysObjTmp->type, 0, column, table, offset);
                } else if (table->columns[column]->name == "FLAGS") {
                    updateNumberXu(sysObjTmp->flags, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysObjAdd(sysObjTmp);
        sysObjTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysTab(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysTabTmp = metadata->schema->dictSysTabFind(rowId);
        if (sysTabTmp != nullptr) {
            metadata->schema->dictSysTabDrop(sysTabTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.TAB$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysTabTmp = new SysTab(rowId, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysTabTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "DATAOBJ#") {
                    updateNumber32u(sysTabTmp->dataObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "TS#") {
                    updateNumber32u(sysTabTmp->ts, 0, column, table, offset);
                } else if (table->columns[column]->name == "CLUCOLS") {
                    updateNumber16(sysTabTmp->cluCols, 0, column, table, offset);
                } else if (table->columns[column]->name == "FLAGS") {
                    updateNumberXu(sysTabTmp->flags, column, table, offset);
                } else if (table->columns[column]->name == "PROPERTY") {
                    updateNumberXu(sysTabTmp->property, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysTabAdd(sysTabTmp);
        sysTabTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysTabComPart(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysTabComPartTmp = metadata->schema->dictSysTabComPartFind(rowId);
        if (sysTabComPartTmp != nullptr) {
            metadata->schema->dictSysTabComPartDrop(sysTabComPartTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.TABCOMPART$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysTabComPartTmp = new SysTabComPart(rowId, 0, 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysTabComPartTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "DATAOBJ#") {
                    updateNumber32u(sysTabComPartTmp->dataObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "BO#") {
                    updateNumber32u(sysTabComPartTmp->bo, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysTabComPartAdd(sysTabComPartTmp);
        sysTabComPartTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysTabPart(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysTabPartTmp = metadata->schema->dictSysTabPartFind(rowId);
        if (sysTabPartTmp != nullptr) {
            metadata->schema->dictSysTabPartDrop(sysTabPartTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.TABPART$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysTabPartTmp = new SysTabPart(rowId, 0, 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysTabPartTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "DATAOBJ#") {
                    updateNumber32u(sysTabPartTmp->dataObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "BO#") {
                    updateNumber32u(sysTabPartTmp->bo, 0, column, table, offset);
                }
            }
        }
        metadata->schema->dictSysTabPartAdd(sysTabPartTmp);
        sysTabPartTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysTabSubPart(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysTabSubPartTmp = metadata->schema->dictSysTabSubPartFind(rowId);
        if (sysTabSubPartTmp != nullptr) {
            metadata->schema->dictSysTabSubPartDrop(sysTabSubPartTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.TABSUBPART$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysTabSubPartTmp = new SysTabSubPart(rowId, 0, 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "OBJ#") {
                    updateNumber32u(sysTabSubPartTmp->obj, 0, column, table, offset);
                } else if (table->columns[column]->name == "DATAOBJ#") {
                    updateNumber32u(sysTabSubPartTmp->dataObj, 0, column, table, offset);
                } else if (table->columns[column]->name == "POBJ#") {
                    updateNumber32u(sysTabSubPartTmp->pObj, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysTabSubPartAdd(sysTabSubPartTmp);
        sysTabSubPartTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysTs(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysTsTmp = metadata->schema->dictSysTsFind(rowId);
        if (sysTsTmp != nullptr) {
            metadata->schema->dictSysTsDrop(sysTsTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.TS$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysTsTmp = new SysTs(rowId, 0, "", 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "TS#") {
                    updateNumber32u(sysTsTmp->ts, 0, column, table, offset);
                } else if (table->columns[column]->name == "NAME") {
                    updateString(sysTsTmp->name, SysTs::NAME_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "BLOCKSIZE") {
                    updateNumber32u(sysTsTmp->blockSize, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysTsAdd(sysTsTmp);
        sysTsTmp = nullptr;
    }

    void SystemTransaction::processUpdateSysUser(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        sysUserTmp = metadata->schema->dictSysUserFind(rowId);
        if (sysUserTmp != nullptr) {
            metadata->schema->dictSysUserDrop(sysUserTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.USER$: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            sysUserTmp = new SysUser(rowId, 0, "", 0, 0, false);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "USER#") {
                    updateNumber32u(sysUserTmp->user, 0, column, table, offset);
                } else if (table->columns[column]->name == "NAME") {
                    updateString(sysUserTmp->name, SysUser::NAME_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "SPARE1") {
                    updateNumberXu(sysUserTmp->spare1, column, table, offset);
                }
            }
        }

        metadata->schema->dictSysUserAdd(sysUserTmp);
        sysUserTmp = nullptr;
    }

    void SystemTransaction::processUpdateXdbTtSet(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        xdbTtSetTmp = metadata->schema->dictXdbTtSetFind(rowId);
        if (xdbTtSetTmp != nullptr) {
            metadata->schema->dictXdbTtSetDrop(xdbTtSetTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing XDB.XDB$TTSET: (rowid: " + rowId.toString() + ") for update");
                return;
            }
            xdbTtSetTmp = new XdbTtSet(rowId, "", "", 0, 0);
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "GUID") {
                    updateRaw(xdbTtSetTmp->guid, XdbTtSet::GUID_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "TOKSUF") {
                    updateString(xdbTtSetTmp->tokSuf, XdbTtSet::TOKSUF_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "FLAGS") {
                    updateNumber64u(xdbTtSetTmp->flags, 0, column, table, offset);
                } else if (table->columns[column]->name == "OBJ") {
                    updateNumber32u(xdbTtSetTmp->obj, 0, column, table, offset);
                }
            }
        }

        metadata->schema->dictXdbTtSetAdd(xdbTtSetTmp);
        xdbTtSetTmp = nullptr;
    }

    void SystemTransaction::processUpdateXdbXNm(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        xdbXNmTmp = metadata->schema->dictXdbXNmFind(table->tokSuf, rowId);
        if (xdbXNmTmp != nullptr) {
            metadata->schema->dictXdbXNmDrop(table->tokSuf, xdbXNmTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing XDB.X$NM" + table->tokSuf + ": (rowid: " + rowId.toString() + ") for update");
                return;
            }
            xdbXNmTmp = new XdbXNm(rowId, "", "");
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "NMSPCURI") {
                    updateString(xdbXNmTmp->nmSpcUri, XdbXNm::NMSPCURI_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "ID") {
                    updateRaw(xdbXNmTmp->id, XdbXNm::ID_LENGTH, column, table, offset);
                }
            }
        }

        metadata->schema->dictXdbXNmAdd(table->tokSuf, xdbXNmTmp);
        xdbXNmTmp = nullptr;
    }

    void SystemTransaction::processUpdateXdbXPt(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        xdbXPtTmp = metadata->schema->dictXdbXPtFind(table->tokSuf, rowId);
        if (xdbXPtTmp != nullptr) {
            metadata->schema->dictXdbXPtDrop(table->tokSuf, xdbXPtTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing XDB.X$PT" + table->tokSuf + ": (rowid: " + rowId.toString() + ") for update");
                return;
            }
            xdbXPtTmp = new XdbXPt(rowId, "", "");
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "PATH") {
                    updateRaw(xdbXPtTmp->path, XdbXPt::PATH_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "ID") {
                    updateRaw(xdbXPtTmp->id, XdbXPt::ID_LENGTH, column, table, offset);
                }
            }
        }

        metadata->schema->dictXdbXPtAdd(table->tokSuf, xdbXPtTmp);
        xdbXPtTmp = nullptr;
    }

    void SystemTransaction::processUpdateXdbXQn(const OracleTable* table, typeRowId rowId, uint64_t offset) {
        xdbXQnTmp = metadata->schema->dictXdbXQnFind(table->tokSuf, rowId);
        if (xdbXQnTmp != nullptr) {
            metadata->schema->dictXdbXQnDrop(table->tokSuf, xdbXQnTmp);
        } else {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing XDB.X$QN" + table->tokSuf + ": (rowid: " + rowId.toString() + ") for update");
                return;
            }
            xdbXQnTmp = new XdbXQn(rowId, "", "", "", "");
        }

        uint64_t baseMax = builder->valuesMax >> 6;
        for (uint64_t base = 0; base <= baseMax; ++base) {
            auto column = static_cast<typeCol>(base << 6);
            for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                if (builder->valuesSet[base] < mask)
                    break;
                if ((builder->valuesSet[base] & mask) == 0)
                    continue;

                if (table->columns[column]->name == "NMSPCID") {
                    updateRaw(xdbXQnTmp->nmSpcId, XdbXQn::NMSPCID_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "LOCALNAME") {
                    updateString(xdbXQnTmp->localName, XdbXQn::LOCALNAME_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "FLAGS") {
                    updateRaw(xdbXQnTmp->flags, XdbXQn::FLAGS_LENGTH, column, table, offset);
                } else if (table->columns[column]->name == "ID") {
                    updateRaw(xdbXQnTmp->id, XdbXQn::ID_LENGTH, column, table, offset);
                }
            }
        }

        metadata->schema->dictXdbXQnAdd(table->tokSuf, xdbXQnTmp);
        xdbXQnTmp = nullptr;
    }

    void SystemTransaction::processUpdate(const OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset) {
        typeRowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
            ctx->logTrace(Ctx::TRACE_SYSTEM, "update table (name: " + table->owner + "." + table->name + ", rowid: " + rowId.toString() + ")");

        switch (table->systemTable) {
            case OracleTable::SYS_CCOL:
                processUpdateSysCCol(table, rowId, offset);
                break;

            case OracleTable::SYS_CDEF:
                processUpdateSysCDef(table, rowId, offset);
                break;

            case OracleTable::SYS_COL:
                processUpdateSysCol(table, rowId, offset);
                break;

            case OracleTable::SYS_DEFERRED_STG:
                processUpdateSysDeferredStg(table, rowId, offset);
                break;

            case OracleTable::SYS_ECOL:
                processUpdateSysECol(table, rowId, offset);
                break;

            case OracleTable::SYS_LOB:
                processUpdateSysLob(table, rowId, offset);
                break;

            case OracleTable::SYS_LOB_COMP_PART:
                processUpdateSysLobCompPart(table, rowId, offset);
                break;

            case OracleTable::SYS_LOB_FRAG:
                processUpdateSysLobFrag(table, rowId, offset);
                break;

            case OracleTable::SYS_OBJ:
                processUpdateSysObj(table, rowId, offset);
                break;

            case OracleTable::SYS_TAB:
                processUpdateSysTab(table, rowId, offset);
                break;

            case OracleTable::SYS_TABCOMPART:
                processUpdateSysTabComPart(table, rowId, offset);
                break;

            case OracleTable::SYS_TABPART:
                processUpdateSysTabPart(table, rowId, offset);
                break;

            case OracleTable::SYS_TABSUBPART:
                processUpdateSysTabSubPart(table, rowId, offset);
                break;

            case OracleTable::SYS_TS:
                processUpdateSysTs(table, rowId, offset);
                break;

            case OracleTable::SYS_USER:
                processUpdateSysUser(table, rowId, offset);
                break;

            case OracleTable::XDB_TTSET:
                processUpdateXdbTtSet(table, rowId, offset);
                break;

            case OracleTable::XDB_XNM:
                processUpdateXdbXNm(table, rowId, offset);
                break;

            case OracleTable::XDB_XPT:
                processUpdateXdbXPt(table, rowId, offset);
                break;

            case OracleTable::XDB_XQN:
                processUpdateXdbXQn(table, rowId, offset);
                break;
        }
    }

    void SystemTransaction::processDeleteSysCCol(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysCColTmp = metadata->schema->dictSysCColFind(rowId);
        if (sysCColTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.CCOL$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysCColDrop(sysCColTmp);
        metadata->schema->sysCColSetTouched.erase(sysCColTmp);
        delete sysCColTmp;
        sysCColTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysCDef(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysCDefTmp = metadata->schema->dictSysCDefFind(rowId);
        if (sysCDefTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.CDEF$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysCDefDrop(sysCDefTmp);
        metadata->schema->sysCDefSetTouched.erase(sysCDefTmp);
        delete sysCDefTmp;
        sysCDefTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysCol(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysColTmp = metadata->schema->dictSysColFind(rowId);
        if (sysColTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.COL$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysColDrop(sysColTmp);
        metadata->schema->sysColSetTouched.erase(sysColTmp);
        delete sysColTmp;
        sysColTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysDeferredStg(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysDeferredStgTmp = metadata->schema->dictSysDeferredStgFind(rowId);
        if (sysDeferredStgTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.DEFERRED_STG$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysDeferredStgDrop(sysDeferredStgTmp);
        metadata->schema->sysDeferredStgSetTouched.erase(sysDeferredStgTmp);
        delete sysDeferredStgTmp;
        sysDeferredStgTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysECol(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysEColTmp = metadata->schema->dictSysEColFind(rowId);
        if (sysEColTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.ECOL$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysEColDrop(sysEColTmp);
        metadata->schema->sysEColSetTouched.erase(sysEColTmp);
        delete sysEColTmp;
        sysEColTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysLob(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysLobTmp = metadata->schema->dictSysLobFind(rowId);
        if (sysLobTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.LOB$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysLobDrop(sysLobTmp);
        metadata->schema->sysLobSetTouched.erase(sysLobTmp);
        delete sysLobTmp;
        sysLobTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysLobCompPart(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysLobCompPartTmp = metadata->schema->dictSysLobCompPartFind(rowId);
        if (sysLobCompPartTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.LOBCOMPPART$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysLobCompPartDrop(sysLobCompPartTmp);
        metadata->schema->sysLobCompPartSetTouched.erase(sysLobCompPartTmp);
        delete sysLobCompPartTmp;
        sysLobCompPartTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysLobFrag(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysLobFragTmp = metadata->schema->dictSysLobFragFind(rowId);
        if (sysLobFragTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.LOBFRAG$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysLobFragDrop(sysLobFragTmp);
        metadata->schema->sysLobFragSetTouched.erase(sysLobFragTmp);
        delete sysLobFragTmp;
        sysLobFragTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysObj(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysObjTmp = metadata->schema->dictSysObjFind(rowId);
        if (sysObjTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.OBJ$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysObjDrop(sysObjTmp);
        metadata->schema->sysObjSetTouched.erase(sysObjTmp);
        delete sysObjTmp;
        sysObjTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysTab(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysTabTmp = metadata->schema->dictSysTabFind(rowId);
        if (sysTabTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.TAB$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysTabDrop(sysTabTmp);
        metadata->schema->sysTabSetTouched.erase(sysTabTmp);
        delete sysTabTmp;
        sysTabTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysTabComPart(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysTabComPartTmp = metadata->schema->dictSysTabComPartFind(rowId);
        if (sysTabComPartTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.TABCOMPART$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysTabComPartDrop(sysTabComPartTmp);
        metadata->schema->sysTabComPartSetTouched.erase(sysTabComPartTmp);
        delete sysTabComPartTmp;
        sysTabComPartTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysTabPart(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysTabPartTmp = metadata->schema->dictSysTabPartFind(rowId);
        if (sysTabPartTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.TABPART$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysTabPartDrop(sysTabPartTmp);
        metadata->schema->sysTabPartSetTouched.erase(sysTabPartTmp);
        delete sysTabPartTmp;
        sysTabPartTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysTabSubPart(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysTabSubPartTmp = metadata->schema->dictSysTabSubPartFind(rowId);
        if (sysTabSubPartTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.TABSUBPART$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysTabSubPartDrop(sysTabSubPartTmp);
        metadata->schema->sysTabSubPartSetTouched.erase(sysTabSubPartTmp);
        delete sysTabSubPartTmp;
        sysTabSubPartTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysTs(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysTsTmp = metadata->schema->dictSysTsFind(rowId);
        if (sysTsTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.TS$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysTsDrop(sysTsTmp);
        delete sysTsTmp;
        sysTsTmp = nullptr;
    }

    void SystemTransaction::processDeleteSysUser(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        sysUserTmp = metadata->schema->dictSysUserFind(rowId);
        if (sysUserTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing SYS.USER$: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictSysUserDrop(sysUserTmp);
        metadata->schema->sysUserSetTouched.erase(sysUserTmp);
        delete sysUserTmp;
        sysUserTmp = nullptr;
    }

    void SystemTransaction::processDeleteXdbTtSet(typeRowId rowId, uint64_t offset __attribute__((unused))) {
        xdbTtSetTmp = metadata->schema->dictXdbTtSetFind(rowId);
        if (xdbTtSetTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing XDB.XDB$TTSET: (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictXdbTtSetDrop(xdbTtSetTmp);
        delete xdbTtSetTmp;
        xdbTtSetTmp = nullptr;
    }

    void SystemTransaction::processDeleteXdbXNm(const OracleTable* table, typeRowId rowId, uint64_t offset __attribute__((unused))) {
        xdbXNmTmp = metadata->schema->dictXdbXNmFind(table->tokSuf, rowId);
        if (xdbXNmTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing XDB.X$NM" + table->tokSuf + ": (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictXdbXNmDrop(table->tokSuf, xdbXNmTmp);
        delete xdbXNmTmp;
        xdbXNmTmp = nullptr;
    }

    void SystemTransaction::processDeleteXdbXPt(const OracleTable* table, typeRowId rowId, uint64_t offset __attribute__((unused))) {
        xdbXPtTmp = metadata->schema->dictXdbXPtFind(table->tokSuf, rowId);
        if (xdbXPtTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing XDB.X$PT" + table->tokSuf + ": (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictXdbXPtDrop(table->tokSuf, xdbXPtTmp);
        delete xdbXPtTmp;
        xdbXPtTmp = nullptr;
    }

    void SystemTransaction::processDeleteXdbXQn(const OracleTable* table, typeRowId rowId, uint64_t offset __attribute__((unused))) {
        xdbXQnTmp = metadata->schema->dictXdbXQnFind(table->tokSuf, rowId);
        if (xdbXQnTmp == nullptr) {
            if (!ctx->flagsSet(Ctx::REDO_FLAGS_ADAPTIVE_SCHEMA)) {
                if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
                    ctx->logTrace(Ctx::TRACE_SYSTEM, "missing XDB.X$QN" + table->tokSuf + ": (rowid: " + rowId.toString() + ") for delete");
                return;
            }
        }

        metadata->schema->dictXdbXQnDrop(table->tokSuf, xdbXQnTmp);
        delete xdbXQnTmp;
        xdbXQnTmp = nullptr;
    }

    void SystemTransaction::processDelete(const OracleTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, uint64_t offset) {
        typeRowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
            ctx->logTrace(Ctx::TRACE_SYSTEM, "delete table (name: " + table->owner + "." + table->name + ", rowid: " + rowId.toString() + ")");

        switch (table->systemTable) {
            case OracleTable::SYS_CCOL:
                processDeleteSysCCol(rowId, offset);
                break;

            case OracleTable::SYS_CDEF:
                processDeleteSysCDef(rowId, offset);
                break;

            case OracleTable::SYS_COL:
                processDeleteSysCol(rowId, offset);
                break;

            case OracleTable::SYS_DEFERRED_STG:
                processDeleteSysDeferredStg(rowId, offset);
                break;

            case OracleTable::SYS_ECOL:
                processDeleteSysECol(rowId, offset);
                break;

            case OracleTable::SYS_LOB:
                processDeleteSysLob(rowId, offset);
                break;

            case OracleTable::SYS_LOB_COMP_PART:
                processDeleteSysLobCompPart(rowId, offset);
                break;

            case OracleTable::SYS_LOB_FRAG:
                processDeleteSysLobFrag(rowId, offset);
                break;

            case OracleTable::SYS_OBJ:
                processDeleteSysObj(rowId, offset);
                break;

            case OracleTable::SYS_TAB:
                processDeleteSysTab(rowId, offset);
                break;

            case OracleTable::SYS_TABCOMPART:
                processDeleteSysTabComPart(rowId, offset);
                break;

            case OracleTable::SYS_TABPART:
                processDeleteSysTabPart(rowId, offset);
                break;

            case OracleTable::SYS_TABSUBPART:
                processDeleteSysTabSubPart(rowId, offset);
                break;

            case OracleTable::SYS_TS:
                processDeleteSysTs(rowId, offset);
                break;

            case OracleTable::SYS_USER:
                processDeleteSysUser(rowId, offset);
                break;

            case OracleTable::XDB_TTSET:
                processDeleteXdbTtSet(rowId, offset);
                break;

            case OracleTable::XDB_XNM:
                processDeleteXdbXNm(table, rowId, offset);
                break;

            case OracleTable::XDB_XPT:
                processDeleteXdbXPt(table, rowId, offset);
                break;

            case OracleTable::XDB_XQN:
                processDeleteXdbXQn(table, rowId, offset);
                break;
        }
    }

    void SystemTransaction::commit(typeScn scn) {
        if (unlikely(ctx->trace & Ctx::TRACE_SYSTEM))
            ctx->logTrace(Ctx::TRACE_SYSTEM, "commit");

        if (!metadata->schema->touched)
            return;

        std::vector<std::string> msgsDropped;
        std::vector<std::string> msgsUpdated;
        metadata->schema->scn = scn;
        metadata->schema->dropUnusedMetadata(metadata->users, metadata->schemaElements, msgsDropped);

        for (const SchemaElement* element: metadata->schemaElements)
            metadata->schema->buildMaps(element->owner, element->table, element->keys, element->keysStr, element->conditionStr, element->options, msgsUpdated,
                                        metadata->suppLogDbPrimary, metadata->suppLogDbAll, metadata->defaultCharacterMapId,
                                        metadata->defaultCharacterNcharMapId);
        metadata->schema->resetTouched();

        for (const auto& msg: msgsDropped) {
            ctx->info(0, "dropped metadata: " + msg);
        }
        for (const auto& msg: msgsUpdated) {
            ctx->info(0, "updated metadata: " + msg);
        }

        metadata->schema->updateXmlCtx();
    }
}
