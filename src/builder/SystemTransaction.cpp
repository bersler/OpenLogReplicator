/* System transaction to change metadata
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../common/DbColumn.h"
#include "../common/DbTable.h"
#include "../common/XmlCtx.h"
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
#include "Builder.h"
#include "SystemTransaction.h"

namespace OpenLogReplicator {
    SystemTransaction::SystemTransaction(Builder* newBuilder, Metadata* newMetadata):
            ctx(newMetadata->ctx),
            builder(newBuilder),
            metadata(newMetadata) {
        ctx->logTrace(Ctx::TRACE::SYSTEM, "begin");
    }

    template<class VALUE, SysCol::COLTYPE COLTYPE>
    void SystemTransaction::updateValue(VALUE& val, typeCol column, const DbTable* table, FileOffset fileOffset, int defVal, uint maxLength) {
        if (builder->values[column][+Format::VALUE_TYPE::AFTER] != nullptr &&
            builder->sizes[column][+Format::VALUE_TYPE::AFTER] > 0) {
            char* retPtr;
            if (unlikely(table->columns[column]->type != COLTYPE))
                throw RuntimeException(50019, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                       table->columns[column]->name + " type found " + std::to_string(static_cast<uint>(table->columns[column]->type)) +
                                       " offset: " + fileOffset.toString());

            if constexpr (COLTYPE == SysCol::COLTYPE::NUMBER) {
                builder->parseNumber(builder->values[column][+Format::VALUE_TYPE::AFTER],
                                     builder->sizes[column][+Format::VALUE_TYPE::AFTER], fileOffset);
                builder->valueBuffer[builder->valueSize] = 0;
                if (unlikely(builder->valueSize == 0 || builder->valueBuffer[0] == '-'))
                    throw RuntimeException(50020, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                           table->columns[column]->name + " value found " + builder->valueBuffer + " offset: " + fileOffset.toString());

                VALUE newVal;
                if constexpr (std::is_same_v<VALUE, int> || std::is_same_v<VALUE, int16_t> || std::is_same_v<VALUE, int32_t> ||
                        std::is_same_v<VALUE, int64_t>) {
                    if (unlikely(builder->valueSize == 0))
                        throw RuntimeException(50020, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                               table->columns[column]->name + " value found " + builder->valueBuffer + " offset: " + fileOffset.toString());
                    newVal = static_cast<VALUE>(strtol(builder->valueBuffer, &retPtr, 10));
                } else if constexpr (std::is_same_v<VALUE, uint> || std::is_same_v<VALUE, uint16_t> || std::is_same_v<VALUE, uint32_t> ||
                        std::is_same_v<VALUE, uint64_t>) {
                    if (unlikely(builder->valueSize == 0 || builder->valueBuffer[0] == '-'))
                        throw RuntimeException(50020, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                               table->columns[column]->name + " value found " + builder->valueBuffer + " offset: " + fileOffset.toString());
                    newVal = strtoul(builder->valueBuffer, &retPtr, 10);
                } else if constexpr (std::is_same_v<VALUE, IntX>) {
                    if (unlikely(builder->valueSize == 0 || builder->valueBuffer[0] == '-'))
                        throw RuntimeException(50020, "ddl: column type mismatch for " + table->owner + "." + table->name + ": column " +
                                               table->columns[column]->name + " value found " + builder->valueBuffer + " offset: " + fileOffset.toString());

                    std::string err;
                    newVal.setStr(builder->valueBuffer, builder->valueSize, err);
                    if (!err.empty())
                        ctx->error(50021, err);
                }

                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM))) {
                    if constexpr (std::is_same_v<VALUE, IntX>)
                        ctx->logTrace(Ctx::TRACE::SYSTEM, "set (" + table->columns[column]->name + ": " + val.toString() + " -> " +
                                      newVal.toString() + ")");
                    else
                        ctx->logTrace(Ctx::TRACE::SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> " +
                                      std::to_string(newVal) + ")");
                }
                val = newVal;
            } else if constexpr (COLTYPE == SysCol::COLTYPE::RAW) {
                builder->parseRaw(builder->values[column][+Format::VALUE_TYPE::AFTER],
                                  builder->sizes[column][+Format::VALUE_TYPE::AFTER], fileOffset);
                VALUE newVal(builder->valueBuffer, builder->valueSize);
                if (unlikely(builder->valueSize > maxLength))
                    throw RuntimeException(50020, "ddl: value too long for " + table->owner + "." + table->name + ": column " +
                                           table->columns[column]->name + ", length " + std::to_string(builder->valueSize) + " offset: " +
                                           fileOffset.toString());

                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
                    ctx->logTrace(Ctx::TRACE::SYSTEM, "set (" + table->columns[column]->name + ": '" + val + "' -> '" + newVal + "')");
                val = newVal;
            } else if constexpr (COLTYPE == SysCol::COLTYPE::VARCHAR || COLTYPE == SysCol::COLTYPE::CHAR) {
                builder->parseString(builder->values[column][+Format::VALUE_TYPE::AFTER],
                                     builder->sizes[column][+Format::VALUE_TYPE::AFTER],
                                     table->columns[column]->charsetId, fileOffset, false, false, false, true);
                VALUE newVal(builder->valueBuffer, builder->valueSize);
                if (unlikely(builder->valueSize > maxLength))
                    throw RuntimeException(50020, "ddl: value too long for " + table->owner + "." + table->name + ": column " +
                                           table->columns[column]->name + ", length " + std::to_string(builder->valueSize) + " offset: " +
                                           fileOffset.toString());

                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
                    ctx->logTrace(Ctx::TRACE::SYSTEM, "set (" + table->columns[column]->name + ": '" + val + "' -> '" + newVal + "')");
                val = newVal;
            }
        } else if (builder->values[column][+Format::VALUE_TYPE::AFTER] != nullptr ||
                builder->values[column][+Format::VALUE_TYPE::BEFORE] != nullptr) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM))) {
                if constexpr (std::is_same_v<VALUE, IntX>)
                    ctx->logTrace(Ctx::TRACE::SYSTEM, "set (" + table->columns[column]->name + ": " + val.toString() + " -> NULL) defVal: " +
                                  std::to_string(defVal));
                else if constexpr (std::is_same_v<VALUE, std::string>)
                    ctx->logTrace(Ctx::TRACE::SYSTEM, "set (" + table->columns[column]->name + ": '" + val + "' -> NULL) defVal: " + std::to_string(defVal));
                else
                    ctx->logTrace(Ctx::TRACE::SYSTEM, "set (" + table->columns[column]->name + ": " + std::to_string(val) + " -> NULL) defVal: " +
                                  std::to_string(defVal));
            }
            if constexpr (std::is_same_v<VALUE, int> || std::is_same_v<VALUE, int16_t> || std::is_same_v<VALUE, int32_t> || std::is_same_v<VALUE, int64_t> ||
                    std::is_same_v<VALUE, uint> || std::is_same_v<VALUE, uint16_t> || std::is_same_v<VALUE, uint32_t> ||
                    std::is_same_v<VALUE, uint64_t> || std::is_same_v<VALUE, IntX>) {
                val = defVal;
            } else if constexpr (std::is_same_v<VALUE, std::string>) {
                val.assign("");
            }
        }
    }

    template<class TABLE>
    void SystemTransaction::updateValues(const DbTable* table, TABLE* row, typeCol column, FileOffset fileOffset) {
        if constexpr (std::is_same_v<TABLE, SysCCol>) {
            if (table->columns[column]->name == "CON#") {
                updateValue<typeCon, SysCol::COLTYPE::NUMBER>(row->con, column, table, fileOffset);
            } else if (table->columns[column]->name == "INTCOL#") {
                updateValue<typeCol, SysCol::COLTYPE::NUMBER>(row->intCol, column, table, fileOffset);
            } else if (table->columns[column]->name == "OBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->obj, column, table, fileOffset);
            } else if (table->columns[column]->name == "SPARE1") {
                updateValue<IntX, SysCol::COLTYPE::NUMBER>(row->spare1, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysCDef>) {
            if (table->columns[column]->name == "CON#") {
                updateValue<typeCon, SysCol::COLTYPE::NUMBER>(row->con, column, table, fileOffset);
            } else if (table->columns[column]->name == "OBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->obj, column, table, fileOffset);
            } else if (table->columns[column]->name == "TYPE#") {
                auto x = static_cast<uint16_t>(row->type);
                updateValue<uint16_t, SysCol::COLTYPE::NUMBER>(x, column, table, fileOffset);
                row->type = static_cast<SysCDef::CDEFTYPE>(x);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysCol>) {
            if (table->columns[column]->name == "OBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->obj, column, table, fileOffset);
            } else if (table->columns[column]->name == "COL#") {
                updateValue<typeCol, SysCol::COLTYPE::NUMBER>(row->col, column, table, fileOffset);
            } else if (table->columns[column]->name == "SEGCOL#") {
                updateValue<typeCol, SysCol::COLTYPE::NUMBER>(row->segCol, column, table, fileOffset);
            } else if (table->columns[column]->name == "INTCOL#") {
                updateValue<typeCol, SysCol::COLTYPE::NUMBER>(row->intCol, column, table, fileOffset);
            } else if (table->columns[column]->name == "NAME") {
                updateValue<std::string, SysCol::COLTYPE::VARCHAR>(row->name, column, table, fileOffset, 0, SysCol::NAME_LENGTH);
            } else if (table->columns[column]->name == "TYPE#") {
                auto x = static_cast<uint16_t>(row->type);
                updateValue<uint16_t, SysCol::COLTYPE::NUMBER>(x, column, table, fileOffset);
                row->type = static_cast<SysCol::COLTYPE>(x);
            } else if (table->columns[column]->name == "SIZE") {
                updateValue<uint, SysCol::COLTYPE::NUMBER>(row->length, column, table, fileOffset);
            } else if (table->columns[column]->name == "PRECISION#") {
                updateValue<int, SysCol::COLTYPE::NUMBER>(row->precision, column, table, fileOffset, -1);
            } else if (table->columns[column]->name == "SCALE") {
                updateValue<int, SysCol::COLTYPE::NUMBER>(row->scale, column, table, fileOffset, -1);
            } else if (table->columns[column]->name == "CHARSETFORM") {
                updateValue<uint, SysCol::COLTYPE::NUMBER>(row->charsetForm, column, table, fileOffset);
            } else if (table->columns[column]->name == "CHARSETID") {
                updateValue<uint, SysCol::COLTYPE::NUMBER>(row->charsetId, column, table, fileOffset);
            } else if (table->columns[column]->name == "NULL$") {
                updateValue<int, SysCol::COLTYPE::NUMBER>(row->null_, column, table, fileOffset);
            } else if (table->columns[column]->name == "PROPERTY") {
                updateValue<IntX, SysCol::COLTYPE::NUMBER>(row->property, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysDeferredStg>) {
            if (table->columns[column]->name == "OBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->obj, column, table, fileOffset);
            } else if (table->columns[column]->name == "FLAGS_STG") {
                updateValue<IntX, SysCol::COLTYPE::NUMBER>(row->flagsStg, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysECol>) {
            if (table->columns[column]->name == "TABOBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->tabObj, column, table, fileOffset);
            } else if (table->columns[column]->name == "COLNUM") {
                updateValue<typeCol, SysCol::COLTYPE::NUMBER>(row->colNum, column, table, fileOffset);
            } else if (table->columns[column]->name == "GUARD_ID") {
                updateValue<typeCol, SysCol::COLTYPE::NUMBER>(row->guardId, column, table, fileOffset, -1);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysLob>) {
            if (table->columns[column]->name == "OBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->obj, column, table, fileOffset);
            } else if (table->columns[column]->name == "COL#") {
                updateValue<typeCol, SysCol::COLTYPE::NUMBER>(row->col, column, table, fileOffset);
            } else if (table->columns[column]->name == "INTCOL#") {
                updateValue<typeCol, SysCol::COLTYPE::NUMBER>(row->intCol, column, table, fileOffset);
            } else if (table->columns[column]->name == "LOBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->lObj, column, table, fileOffset);
            } else if (table->columns[column]->name == "TS#") {
                updateValue<typeTs, SysCol::COLTYPE::NUMBER>(row->ts, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysLobCompPart>) {
            if (table->columns[column]->name == "PARTOBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->partObj, column, table, fileOffset);
            } else if (table->columns[column]->name == "LOBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->lObj, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysLobFrag>) {
            if (table->columns[column]->name == "FRAGOBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->fragObj, column, table, fileOffset);
            } else if (table->columns[column]->name == "PARENTOBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->parentObj, column, table, fileOffset);
            } else if (table->columns[column]->name == "TS#") {
                updateValue<typeTs, SysCol::COLTYPE::NUMBER>(row->ts, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysObj>) {
            if (table->columns[column]->name == "OWNER#") {
                updateValue<typeUser, SysCol::COLTYPE::NUMBER>(row->owner, column, table, fileOffset);
            } else if (table->columns[column]->name == "OBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->obj, column, table, fileOffset);
            } else if (table->columns[column]->name == "DATAOBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->dataObj, column, table, fileOffset);
            } else if (table->columns[column]->name == "NAME") {
                updateValue<std::string, SysCol::COLTYPE::VARCHAR>(row->name, column, table, fileOffset, 0, SysObj::NAME_LENGTH);
            } else if (table->columns[column]->name == "TYPE#") {
                auto x = static_cast<uint16_t>(row->type);
                updateValue<uint16_t, SysCol::COLTYPE::NUMBER>(x, column, table, fileOffset);
                row->type = static_cast<SysObj::OBJTYPE>(x);
            } else if (table->columns[column]->name == "FLAGS") {
                updateValue<IntX, SysCol::COLTYPE::NUMBER>(row->flags, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysTab>) {
            if (table->columns[column]->name == "OBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->obj, column, table, fileOffset);
            } else if (table->columns[column]->name == "DATAOBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->dataObj, column, table, fileOffset);
            } else if (table->columns[column]->name == "TS#") {
                updateValue<typeTs, SysCol::COLTYPE::NUMBER>(row->ts, column, table, fileOffset);
            } else if (table->columns[column]->name == "CLUCOLS") {
                updateValue<typeCol, SysCol::COLTYPE::NUMBER>(row->cluCols, column, table, fileOffset);
            } else if (table->columns[column]->name == "FLAGS") {
                updateValue<IntX, SysCol::COLTYPE::NUMBER>(row->flags, column, table, fileOffset);
            } else if (table->columns[column]->name == "PROPERTY") {
                updateValue<IntX, SysCol::COLTYPE::NUMBER>(row->property, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysTabComPart>) {
            if (table->columns[column]->name == "OBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->obj, column, table, fileOffset);
            } else if (table->columns[column]->name == "DATAOBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->dataObj, column, table, fileOffset);
            } else if (table->columns[column]->name == "BO#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->bo, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysTabPart>) {
            if (table->columns[column]->name == "OBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->obj, column, table, fileOffset);
            } else if (table->columns[column]->name == "DATAOBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->dataObj, column, table, fileOffset);
            } else if (table->columns[column]->name == "BO#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->bo, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysTabSubPart>) {
            if (table->columns[column]->name == "OBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->obj, column, table, fileOffset);
            } else if (table->columns[column]->name == "DATAOBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->dataObj, column, table, fileOffset);
            } else if (table->columns[column]->name == "POBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->pObj, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysTs>) {
            if (table->columns[column]->name == "TS#") {
                updateValue<typeTs, SysCol::COLTYPE::NUMBER>(row->ts, column, table, fileOffset);
            } else if (table->columns[column]->name == "NAME") {
                updateValue<std::string, SysCol::COLTYPE::VARCHAR>(row->name, column, table, fileOffset, 0, SysTs::NAME_LENGTH);
            } else if (table->columns[column]->name == "BLOCKSIZE") {
                updateValue<uint32_t, SysCol::COLTYPE::NUMBER>(row->blockSize, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, SysUser>) {
            if (table->columns[column]->name == "USER#") {
                updateValue<typeUser, SysCol::COLTYPE::NUMBER>(row->user, column, table, fileOffset);
            } else if (table->columns[column]->name == "NAME") {
                updateValue<std::string, SysCol::COLTYPE::VARCHAR>(row->name, column, table, fileOffset, 0, SysUser::NAME_LENGTH);
            } else if (table->columns[column]->name == "SPARE1") {
                updateValue<IntX, SysCol::COLTYPE::NUMBER>(row->spare1, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, XdbTtSet>) {
            if (table->columns[column]->name == "GUID") {
                updateValue<std::string, SysCol::COLTYPE::RAW>(row->guid, column, table, fileOffset, 0, XdbTtSet::GUID_LENGTH);
            } else if (table->columns[column]->name == "TOKSUF") {
                updateValue<std::string, SysCol::COLTYPE::VARCHAR>(row->tokSuf, column, table, fileOffset, 0, XdbTtSet::TOKSUF_LENGTH);
            } else if (table->columns[column]->name == "FLAGS") {
                updateValue<uint64_t, SysCol::COLTYPE::NUMBER>(row->flags, column, table, fileOffset);
            } else if (table->columns[column]->name == "OBJ#") {
                updateValue<typeObj, SysCol::COLTYPE::NUMBER>(row->obj, column, table, fileOffset);
            }
        }

        if constexpr (std::is_same_v<TABLE, XdbXNm>) {
            if (table->columns[column]->name == "NMSPCURI") {
                updateValue<std::string, SysCol::COLTYPE::VARCHAR>(row->nmSpcUri, column, table, fileOffset, 0, XdbXNm::NMSPCURI_LENGTH);
            } else if (table->columns[column]->name == "ID") {
                updateValue<std::string, SysCol::COLTYPE::RAW>(row->id, column, table, fileOffset, 0, XdbXNm::ID_LENGTH);
            }
        }

        if constexpr (std::is_same_v<TABLE, XdbXPt>) {
            if (table->columns[column]->name == "PATH") {
                updateValue<std::string, SysCol::COLTYPE::RAW>(row->path, column, table, fileOffset, 0, XdbXPt::PATH_LENGTH);
            } else if (table->columns[column]->name == "ID") {
                updateValue<std::string, SysCol::COLTYPE::RAW>(row->id, column, table, fileOffset, 0, XdbXPt::ID_LENGTH);
            }
        }

        if constexpr (std::is_same_v<TABLE, XdbXQn>) {
            if (table->columns[column]->name == "NMSPCID") {
                updateValue<std::string, SysCol::COLTYPE::RAW>(row->nmSpcId, column, table, fileOffset, 0, XdbXQn::NMSPCID_LENGTH);
            } else if (table->columns[column]->name == "LOCALNAME") {
                updateValue<std::string, SysCol::COLTYPE::VARCHAR>(row->localName, column, table, fileOffset, 0, XdbXQn::LOCALNAME_LENGTH);
            } else if (table->columns[column]->name == "FLAGS") {
                updateValue<std::string, SysCol::COLTYPE::RAW>(row->flags, column, table, fileOffset, 0, XdbXQn::FLAGS_LENGTH);
            } else if (table->columns[column]->name == "ID") {
                updateValue<std::string, SysCol::COLTYPE::RAW>(row->id, column, table, fileOffset, 0, XdbXQn::ID_LENGTH);
            }
        }
    }

    template<class TABLE, class TABLEKEY, class TABLEUNORDEREDKEY>
    void SystemTransaction::updateAllValues(TablePack<TABLE, TABLEKEY, TABLEUNORDEREDKEY>* pack, const DbTable* table, TABLE* row, FileOffset fileOffset) {
        const typeCol baseMax = builder->valuesMax >> 6;
        for (typeCol base = 0; base <= baseMax; ++base) {
            const auto columnBase = static_cast<typeCol>(base << 6);
            typeMask set = builder->valuesSet[base];
            while (set != 0) {
                const typeCol pos = ffsll(set) - 1;
                set &= ~(1ULL << pos);
                const typeCol column = columnBase + pos;

                updateValues<TABLE>(table, row, column, fileOffset);
            }
        }

        pack->addKeys(row);

        if constexpr (TABLE::dependentTable())
            metadata->schema->touchTable(row->getDependentTable());

        if constexpr (TABLE::dependentTableLob())
            metadata->schema->touchTableLob(row->getDependentTableLob());

        if constexpr (TABLE::dependentTableLobFrag())
            metadata->schema->touchTableLobFrag(row->getDependentTableLobFrag());

        if constexpr (TABLE::dependentTablePart())
            metadata->schema->touchTablePart(row->getDependentTablePart());

        metadata->schema->touched = true;
    }

    XmlCtx* SystemTransaction::findMatchingXmlCtx(const DbTable* table) const {
        const auto& it = metadata->schema->schemaXmlMap.find(table->tokSuf);
        if (unlikely(it == metadata->schema->schemaXmlMap.end()))
            throw DataException(50068, "missing " + XdbXNm::tableName() + table->tokSuf + " table, find failed");
        return it->second;
    }

    void SystemTransaction::processInsert(const DbTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) {
        const RowId rowId(dataObj, bdba, slot);
        char str[RowId::SIZE + 1];
        rowId.toString(str);
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
            ctx->logTrace(Ctx::TRACE::SYSTEM, "insert table (name: " + table->owner + "." + table->name + ", ROWID: " + rowId.toString() + ")");

        switch (table->systemTable) {
            case DbTable::TABLE::NONE:
                break;

            case DbTable::TABLE::SYS_CCOL:
                updateAllValues(&metadata->schema->sysCColPack, table, metadata->schema->sysCColPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_CDEF:
                updateAllValues(&metadata->schema->sysCDefPack, table, metadata->schema->sysCDefPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_COL:
                updateAllValues(&metadata->schema->sysColPack, table, metadata->schema->sysColPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_DEFERRED_STG:
                updateAllValues(&metadata->schema->sysDeferredStgPack, table, metadata->schema->sysDeferredStgPack.forInsert(ctx, rowId, fileOffset),
                                fileOffset);
                break;

            case DbTable::TABLE::SYS_ECOL:
                updateAllValues(&metadata->schema->sysEColPack, table, metadata->schema->sysEColPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_LOB:
                updateAllValues(&metadata->schema->sysLobPack, table, metadata->schema->sysLobPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_LOB_COMP_PART:
                updateAllValues(&metadata->schema->sysLobCompPartPack, table, metadata->schema->sysLobCompPartPack.forInsert(ctx, rowId, fileOffset),
                                fileOffset);
                break;

            case DbTable::TABLE::SYS_LOB_FRAG:
                updateAllValues(&metadata->schema->sysLobFragPack, table, metadata->schema->sysLobFragPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_OBJ:
                updateAllValues(&metadata->schema->sysObjPack, table, metadata->schema->sysObjPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_TAB:
                updateAllValues(&metadata->schema->sysTabPack, table, metadata->schema->sysTabPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_TABCOMPART:
                updateAllValues(&metadata->schema->sysTabComPartPack, table, metadata->schema->sysTabComPartPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_TABPART:
                updateAllValues(&metadata->schema->sysTabPartPack, table, metadata->schema->sysTabPartPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_TABSUBPART:
                updateAllValues(&metadata->schema->sysTabSubPartPack, table, metadata->schema->sysTabSubPartPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_TS:
                updateAllValues(&metadata->schema->sysTsPack, table, metadata->schema->sysTsPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::SYS_USER:
                updateAllValues(&metadata->schema->sysUserPack, table, metadata->schema->sysUserPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::XDB_TTSET:
                updateAllValues(&metadata->schema->xdbTtSetPack, table, metadata->schema->xdbTtSetPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;

            case DbTable::TABLE::XDB_XNM: {
                auto* xmlCtx = findMatchingXmlCtx(table);
                updateAllValues(&xmlCtx->xdbXNmPack, table, xmlCtx->xdbXNmPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;
            }

            case DbTable::TABLE::XDB_XPT: {
                auto* xmlCtx = findMatchingXmlCtx(table);
                updateAllValues(&xmlCtx->xdbXPtPack, table, xmlCtx->xdbXPtPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;
            }

            case DbTable::TABLE::XDB_XQN: {
                auto* xmlCtx = findMatchingXmlCtx(table);
                updateAllValues(&xmlCtx->xdbXQnPack, table, xmlCtx->xdbXQnPack.forInsert(ctx, rowId, fileOffset), fileOffset);
                break;
            }
        }
        metadata->schema->touched = true;
    }

    void SystemTransaction::processUpdate(const DbTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) {
        const RowId rowId(dataObj, bdba, slot);
        char str[RowId::SIZE + 1];
        rowId.toString(str);
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
            ctx->logTrace(Ctx::TRACE::SYSTEM, "update table (name: " + table->owner + "." + table->name + ", ROWID: " + rowId.toString() + ")");

        switch (table->systemTable) {
            case DbTable::TABLE::NONE:
                break;

            case DbTable::TABLE::SYS_CCOL:
                if (auto* sysCCol = metadata->schema->sysCColPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysCColPack, table, sysCCol, fileOffset);
                break;

            case DbTable::TABLE::SYS_CDEF:
                if (auto* sysCDef = metadata->schema->sysCDefPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysCDefPack, table, sysCDef, fileOffset);
                break;

            case DbTable::TABLE::SYS_COL:
                if (auto* sysCol = metadata->schema->sysColPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysColPack, table, sysCol, fileOffset);
                break;

            case DbTable::TABLE::SYS_DEFERRED_STG:
                if (auto* sysDeferredStg = metadata->schema->sysDeferredStgPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysDeferredStgPack, table, sysDeferredStg, fileOffset);
                break;

            case DbTable::TABLE::SYS_ECOL:
                if (auto* sysECol = metadata->schema->sysEColPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysEColPack, table, sysECol, fileOffset);
                break;

            case DbTable::TABLE::SYS_LOB:
                if (auto* sysLob = metadata->schema->sysLobPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysLobPack, table, sysLob, fileOffset);
                break;

            case DbTable::TABLE::SYS_LOB_COMP_PART:
                if (auto* sysLobCompPart = metadata->schema->sysLobCompPartPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysLobCompPartPack, table, sysLobCompPart, fileOffset);
                break;

            case DbTable::TABLE::SYS_LOB_FRAG:
                if (auto* sysLobFrag = metadata->schema->sysLobFragPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysLobFragPack, table, sysLobFrag, fileOffset);
                break;

            case DbTable::TABLE::SYS_OBJ:
                if (auto* sysObj = metadata->schema->sysObjPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysObjPack, table, sysObj, fileOffset);
                break;

            case DbTable::TABLE::SYS_TAB:
                if (auto* sysTab = metadata->schema->sysTabPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysTabPack, table, sysTab, fileOffset);
                break;


            case DbTable::TABLE::SYS_TABCOMPART:
                if (auto* sysTabComPart = metadata->schema->sysTabComPartPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysTabComPartPack, table, sysTabComPart, fileOffset);
                break;

            case DbTable::TABLE::SYS_TABPART:
                if (auto* sysTabPart = metadata->schema->sysTabPartPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysTabPartPack, table, sysTabPart, fileOffset);
                break;

            case DbTable::TABLE::SYS_TABSUBPART:
                if (auto* sysTabSubPart = metadata->schema->sysTabSubPartPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysTabSubPartPack, table, sysTabSubPart, fileOffset);
                break;

            case DbTable::TABLE::SYS_TS:
                if (auto* sysTs = metadata->schema->sysTsPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysTsPack, table, sysTs, fileOffset);
                break;

            case DbTable::TABLE::SYS_USER:
                if (auto* sysUser = metadata->schema->sysUserPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->sysUserPack, table, sysUser, fileOffset);
                break;

            case DbTable::TABLE::XDB_TTSET:
                if (auto* xdbTtSet = metadata->schema->xdbTtSetPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&metadata->schema->xdbTtSetPack, table, xdbTtSet, fileOffset);
                break;

            case DbTable::TABLE::XDB_XNM: {
                auto* xmlCtx = findMatchingXmlCtx(table);
                if (auto* xdbXNm = xmlCtx->xdbXNmPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&xmlCtx->xdbXNmPack, table, xdbXNm, fileOffset);
                break;
            }

            case DbTable::TABLE::XDB_XPT: {
                auto* xmlCtx = findMatchingXmlCtx(table);
                if (auto* xdbXPt = xmlCtx->xdbXPtPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&xmlCtx->xdbXPtPack, table, xdbXPt, fileOffset);
                break;
            }

            case DbTable::TABLE::XDB_XQN: {
                auto* xmlCtx = findMatchingXmlCtx(table);
                if (auto* xdbXQn = xmlCtx->xdbXQnPack.forUpdate(ctx, rowId, fileOffset))
                    updateAllValues(&xmlCtx->xdbXQnPack, table, xdbXQn, fileOffset);
                break;
            }
        }
    }

    void SystemTransaction::processDelete(const DbTable* table, typeDataObj dataObj, typeDba bdba, typeSlot slot, FileOffset fileOffset) const {
        const RowId rowId(dataObj, bdba, slot);
        char str[RowId::SIZE + 1];
        rowId.toString(str);
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
            ctx->logTrace(Ctx::TRACE::SYSTEM, "delete table (name: " + table->owner + "." + table->name + ", ROWID: " + rowId.toString() + ")");

        switch (table->systemTable) {
            case DbTable::TABLE::NONE:
                break;

            case DbTable::TABLE::SYS_CCOL:
                metadata->schema->sysCColPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_CDEF:
                metadata->schema->sysCDefPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_COL:
                metadata->schema->sysColPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_DEFERRED_STG:
                metadata->schema->sysDeferredStgPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_ECOL:
                metadata->schema->sysEColPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_LOB:
                metadata->schema->sysLobPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_LOB_COMP_PART:
                metadata->schema->sysLobCompPartPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_LOB_FRAG:
                metadata->schema->sysLobFragPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_OBJ:
                metadata->schema->sysObjPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_TAB:
                metadata->schema->sysTabPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_TABCOMPART:
                metadata->schema->sysTabComPartPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_TABPART:
                metadata->schema->sysTabPartPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_TABSUBPART:
                metadata->schema->sysTabSubPartPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_TS:
                metadata->schema->sysTsPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::SYS_USER:
                metadata->schema->sysUserPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::XDB_TTSET:
                metadata->schema->xdbTtSetPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::XDB_XNM:
                findMatchingXmlCtx(table)->xdbXNmPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::XDB_XPT:
                findMatchingXmlCtx(table)->xdbXPtPack.drop(ctx, rowId, fileOffset, true);
                break;

            case DbTable::TABLE::XDB_XQN:
                findMatchingXmlCtx(table)->xdbXQnPack.drop(ctx, rowId, fileOffset, true);
                break;
        }
    }

    void SystemTransaction::commit(Scn scn) const {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
            ctx->logTrace(Ctx::TRACE::SYSTEM, "commit");

        if (!metadata->schema->touched)
            return;

        std::vector<std::string> msgs;
        std::unordered_map<typeObj, std::string> tablesDropped;
        std::unordered_map<typeObj, std::string> tablesUpdated;
        metadata->schema->scn = scn;
        metadata->schema->dropUnusedMetadata(metadata->users, metadata->schemaElements, tablesDropped);

        metadata->buildMaps(msgs, tablesUpdated);
        metadata->schema->resetTouched();

        for (const auto& msg: msgs)
            ctx->info(0, msg);
        for (const auto& [obj, tableName]: tablesDropped) {
            if (tablesUpdated.find(obj) != tablesUpdated.end())
                continue;
            ctx->info(0, "dropped metadata: " + tableName);
        }
        for (const auto& [_, tableName]: tablesUpdated) {
            ctx->info(0, "updated metadata: " + tableName);
        }

        metadata->schema->updateXmlCtx();
    }
}
