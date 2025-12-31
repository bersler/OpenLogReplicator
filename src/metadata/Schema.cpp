/* Base class for handling of schema
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

#include <cstring>
#include <vector>
#include <regex>

#include "../common/Ctx.h"
#include "../common/DbColumn.h"
#include "../common/DbLob.h"
#include "../common/DbTable.h"
#include "../common/XmlCtx.h"
#include "../common/exception/DataException.h"
#include "../locales/Locales.h"
#include "Schema.h"
#include "SchemaElement.h"

namespace OpenLogReplicator {
    Schema::Schema(Ctx* newCtx, Locales* newLocales):
            ctx(newCtx),
            locales(newLocales),
            sysUserAdaptive(sysUserRowId, 0, "", 0, 0, false) {}

    // FIXME: throws exception
    Schema::~Schema() {
        try {
            purgeMetadata();
        } catch (const DataException& e) {
            ctx->error(50029, "schema destructor exception: " + std::string(e.what()));
        }
        purgeDicts();
    }

    void Schema::purgeMetadata() {
        if (columnTmp != nullptr) {
            delete columnTmp;
            columnTmp = nullptr;
        }

        if (lobTmp != nullptr) {
            delete lobTmp;
            lobTmp = nullptr;
        }

        if (tableTmp != nullptr) {
            delete tableTmp;
            tableTmp = nullptr;
        }

        while (!tableMap.empty()) {
            const auto tableMapTt = tableMap.cbegin();
            const DbTable* table = tableMapTt->second;
            removeTableFromDict(table);
            delete table;
        }

        if (!lobPartitionMap.empty())
            ctx->error(50029, "schema lob partition map not empty, left: " + std::to_string(lobPartitionMap.size()) + " at exit");
        lobPartitionMap.clear();
        if (!lobIndexMap.empty())
            ctx->error(50029, "schema lob index map not empty, left: " + std::to_string(lobIndexMap.size()) + " at exit");
        lobIndexMap.clear();
        if (!tablePartitionMap.empty())
            ctx->error(50029, "schema table partition map not empty, left: " + std::to_string(tablePartitionMap.size()) + " at exit");
        tablePartitionMap.clear();

        tablesTouched.clear();
        identifiersTouched.clear();
    }

    void Schema::purgeDicts() {
        sysCColPack.clear(ctx);
        sysCDefPack.clear(ctx);
        sysColPack.clear(ctx);
        sysDeferredStgPack.clear(ctx);
        sysEColPack.clear(ctx);
        sysLobPack.clear(ctx);
        sysLobCompPartPack.clear(ctx);
        sysLobFragPack.clear(ctx);
        sysObjPack.clear(ctx);
        sysTabPack.clear(ctx);
        sysTabComPartPack.clear(ctx);
        sysTabPartPack.clear(ctx);
        sysTabSubPartPack.clear(ctx);
        sysTsPack.clear(ctx);
        sysUserPack.clear(ctx);
        xdbTtSetPack.clear(ctx);

        while (!schemaXmlMap.empty()) {
            const auto schemaXmlMapIt = schemaXmlMap.cbegin();
            XmlCtx* xmlCtx = schemaXmlMapIt->second;
            xmlCtx->purgeDicts();
            schemaXmlMap.erase(schemaXmlMapIt);
            delete xmlCtx;
        }

        resetTouched();
    }

    bool Schema::compare(Schema* otherSchema, std::string& msgs) const {
        if (!sysCColPack.compareTo(otherSchema->sysCColPack, msgs)) return false;
        if (!sysCDefPack.compareTo(otherSchema->sysCDefPack, msgs)) return false;
        if (!sysColPack.compareTo(otherSchema->sysColPack, msgs)) return false;
        if (!sysDeferredStgPack.compareTo(otherSchema->sysDeferredStgPack, msgs)) return false;
        if (!sysEColPack.compareTo(otherSchema->sysEColPack, msgs)) return false;
        if (!sysLobPack.compareTo(otherSchema->sysLobPack, msgs)) return false;
        if (!sysLobCompPartPack.compareTo(otherSchema->sysLobCompPartPack, msgs)) return false;
        if (!sysLobFragPack.compareTo(otherSchema->sysLobFragPack, msgs)) return false;
        if (!sysObjPack.compareTo(otherSchema->sysObjPack, msgs)) return false;
        if (!sysTabPack.compareTo(otherSchema->sysTabPack, msgs)) return false;
        if (!sysTabComPartPack.compareTo(otherSchema->sysTabComPartPack, msgs)) return false;
        if (!sysTabPartPack.compareTo(otherSchema->sysTabPartPack, msgs)) return false;
        if (!sysTabSubPartPack.compareTo(otherSchema->sysTabSubPartPack, msgs)) return false;
        if (!sysTsPack.compareTo(otherSchema->sysTsPack, msgs)) return false;
        if (!sysUserPack.compareTo(otherSchema->sysUserPack, msgs)) return false;
        if (!xdbTtSetPack.compareTo(otherSchema->xdbTtSetPack, msgs)) return false;
        for (const auto& [tokSuf, xmlCtx]: schemaXmlMap) {
            auto otherXmlCtxIt = otherSchema->schemaXmlMap.find(tokSuf);
            if (otherXmlCtxIt == otherSchema->schemaXmlMap.end())
                return false;
            const XmlCtx* otherXmlCtx = otherXmlCtxIt->second;
            if (!xmlCtx->xdbXNmPack.compareTo(otherXmlCtx->xdbXNmPack, msgs))
                return false;
            if (!xmlCtx->xdbXPtPack.compareTo(otherXmlCtx->xdbXPtPack, msgs))
                return false;
            if (!xmlCtx->xdbXQnPack.compareTo(otherXmlCtx->xdbXQnPack, msgs))
                return false;
        }
        msgs.assign("");
        return true;
    }

    void Schema::touchTable(typeObj obj) {
        if (obj == 0)
            return;

        identifiersTouched.insert(obj);
        const auto& tableMapIt = tableMap.find(obj);
        if (tableMapIt == tableMap.end())
            return;

        const auto& tablesTouchedIt = tablesTouched.find(tableMapIt->second);
        if (tablesTouchedIt != tablesTouched.end())
            return;

        tablesTouched.insert(tableMapIt->second);
    }

    void Schema::touchTableLob(typeObj lobObj) {
        const auto& it = sysLobPack.unorderedMapKey.find(SysLobLObj(lobObj));
        if (it != sysLobPack.unorderedMapKey.end())
            touchTable(it->second->obj);
    }

    void Schema::touchTableLobFrag(typeObj lobFragObj) {
        const auto& it = sysLobCompPartPack.unorderedMapKey.find(SysLobCompPartPartObj(lobFragObj));
        if (it != sysLobCompPartPack.unorderedMapKey.end()) {
            const auto& it2 = sysLobPack.unorderedMapKey.find(SysLobLObj(it->second->lObj));
            if (it2 != sysLobPack.unorderedMapKey.end())
                touchTable(it2->second->obj);
        }
    }

    void Schema::touchTablePart(typeObj obj) {
        const auto& it = sysObjPack.unorderedMapKey.find(SysObjObj(obj));
        if (it != sysObjPack.unorderedMapKey.end())
            touchTable(it->second->obj);
    }

    DbTable* Schema::checkTableDict(typeObj obj) const {
        const auto& it = tablePartitionMap.find(obj);
        if (it != tablePartitionMap.end())
            return it->second;

        return nullptr;
    }

    bool Schema::checkTableDictUncommitted(typeObj obj, std::string& owner, std::string& table) const {
        const auto& objIt = sysObjPack.unorderedMapKey.find(SysObjObj(obj));
        if (objIt == sysObjPack.unorderedMapKey.end())
            return false;
        const SysObj* sysObj = objIt->second;

        const auto& userIt = sysUserPack.unorderedMapKey.find(SysUserUser(sysObj->owner));
        if (userIt == sysUserPack.unorderedMapKey.end())
            return false;
        const SysUser* sysUser = userIt->second;

        table = sysObj->name;
        owner = sysUser->name;
        return true;
    }

    DbLob* Schema::checkLobDict(typeDataObj dataObj) const {
        const auto& it = lobPartitionMap.find(dataObj);
        if (it != lobPartitionMap.end())
            return it->second;

        return nullptr;
    }

    DbLob* Schema::checkLobIndexDict(typeDataObj dataObj) const {
        const auto& it = lobIndexMap.find(dataObj);
        if (it != lobIndexMap.end())
            return it->second;

        return nullptr;
    }

    void Schema::addTableToDict(DbTable* table) {
        if (unlikely(tableMap.find(table->obj) != tableMap.end()))
            throw DataException(50031, "can't add table (obj: " + std::to_string(table->obj) + ", dataobj: " + std::to_string(table->dataObj) + ")");

        tableMap.insert_or_assign(table->obj, table);

        for (auto* lob: table->lobs) {
            for (auto dataObj: lob->lobIndexes) {
                if (likely(lobIndexMap.find(dataObj) == lobIndexMap.end()))
                    lobIndexMap.insert_or_assign(dataObj, lob);
                else
                    throw DataException(50032, "can't add lob index element (dataobj: " + std::to_string(dataObj) + ")");
            }

            for (auto dataObj: lob->lobPartitions) {
                if (lobPartitionMap.find(dataObj) == lobPartitionMap.end())
                    lobPartitionMap.insert_or_assign(dataObj, lob);
            }
        }

        if (likely(tablePartitionMap.find(table->obj) == tablePartitionMap.end()))
            tablePartitionMap.insert_or_assign(table->obj, table);
        else
            throw DataException(50033, "can't add partition (obj: " + std::to_string(table->obj) + ", dataobj: " +
                                std::to_string(table->dataObj) + ")");

        for (const typeObj2 objx: table->tablePartitions) {
            const typeObj obj = objx >> 32;
            const typeDataObj dataObj = objx & 0xFFFFFFFF;

            if (likely(tablePartitionMap.find(obj) == tablePartitionMap.end()))
                tablePartitionMap.insert_or_assign(obj, table);
            else
                throw DataException(50034, "can't add partition element (obj: " + std::to_string(obj) + ", dataobj: " +
                                    std::to_string(dataObj) + ")");
        }
    }

    void Schema::removeTableFromDict(const DbTable* table) {
        auto tablePartitionMapIt = tablePartitionMap.find(table->obj);
        if (likely(tablePartitionMapIt != tablePartitionMap.end()))
            tablePartitionMap.erase(tablePartitionMapIt);
        else
            throw DataException(50035, "can't remove partition (obj: " + std::to_string(table->obj) + ", dataobj: " +
                                std::to_string(table->dataObj) + ")");

        for (const typeObj2 objx: table->tablePartitions) {
            const typeObj obj = objx >> 32;
            const typeDataObj dataObj = objx & 0xFFFFFFFF;

            tablePartitionMapIt = tablePartitionMap.find(obj);
            if (likely(tablePartitionMapIt != tablePartitionMap.end()))
                tablePartitionMap.erase(tablePartitionMapIt);
            else
                throw DataException(50036, "can't remove table partition element (obj: " + std::to_string(obj) + ", dataobj: " +
                                    std::to_string(dataObj) + ")");
        }

        for (const auto* const lob: table->lobs) {
            for (auto dataObj: lob->lobIndexes) {
                auto lobIndexMapIt = lobIndexMap.find(dataObj);
                if (likely(lobIndexMapIt != lobIndexMap.end()))
                    lobIndexMap.erase(lobIndexMapIt);
                else
                    throw DataException(50037, "can't remove lob index element (dataobj: " + std::to_string(dataObj) + ")");
            }

            for (auto dataObj: lob->lobPartitions) {
                auto lobPartitionMapIt = lobPartitionMap.find(dataObj);
                if (lobPartitionMapIt != lobPartitionMap.end())
                    lobPartitionMap.erase(lobPartitionMapIt);
            }
        }

        auto tableMapIt = tableMap.find(table->obj);
        if (likely(tableMapIt != tableMap.end()))
            tableMap.erase(tableMapIt);
        else
            throw DataException(50038, "can't remove table (obj: " + std::to_string(table->obj) + ", dataobj: " +
                                std::to_string(table->dataObj) + ")");
    }

    void Schema::dropUnusedMetadata(const std::set<std::string>& users, const std::vector<SchemaElement*>& schemaElements, std::unordered_map<typeObj,
                                    std::string>& msgs) {
        for (const DbTable* table: tablesTouched) {
            msgs[table->obj] = table->owner + "." + table->name + " (dataobj: " + std::to_string(table->dataObj) + ", obj: " +
                    std::to_string(table->obj) + ") ";
            removeTableFromDict(table);
            delete table;
        }
        tablesTouched.clear();

        // SYS.USER$
        for (const auto* sysUser: sysUserPack.setTouched) {
            if (users.find(sysUser->name) != users.end())
                continue;
            sysUserPack.drop(ctx, sysUser->rowId);
        }

        // SYS.OBJ$
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::ADAPTIVE_SCHEMA)) {
            // delete objects owned by users that are not in the list of users
            for (const auto* sysObj: sysObjPack.setTouched) {
                auto sysUserMapUserIt = sysUserPack.unorderedMapKey.find(SysUserUser(sysObj->owner));
                if (sysUserMapUserIt != sysUserPack.unorderedMapKey.end()) {
                    const SysUser* sysUser = sysUserMapUserIt->second;
                    if (sysUser->name == "SYS" || sysUser->name == "XDB") {
                        if (!sysUser->single)
                            continue;
                    } else
                        continue;

                    // SYS or XDB user, check if matches list of system table
                    for (const SchemaElement* element: schemaElements) {
                        const std::regex regexOwner(element->owner);
                        const std::regex regexTable(element->table);

                        // matches, keep it
                        if (regex_match(sysUser->name, regexOwner) && regex_match(sysObj->name, regexTable))
                            continue;
                    }
                }

                sysObjPack.drop(ctx, sysObj->rowId);
                touched = true;
            }
            sysObjPack.setTouched.clear();
        }

        // SYS.CCOL$
        for (const auto* sysCCol: sysCColPack.setTouched) {
            if (sysObjPack.unorderedMapKey.find(SysObjObj(sysCCol->obj)) != sysObjPack.unorderedMapKey.end())
                continue;
            sysCColPack.drop(ctx, sysCCol->rowId);
            touched = true;
        }
        sysCColPack.setTouched.clear();

        // SYS.CDEF$
        for (const auto* sysCDef: sysCDefPack.setTouched) {
            if (sysObjPack.unorderedMapKey.find(SysObjObj(sysCDef->obj)) != sysObjPack.unorderedMapKey.end())
                continue;
            sysCDefPack.drop(ctx, sysCDef->rowId);
            touched = true;
        }
        sysCDefPack.setTouched.clear();

        // SYS.COL$
        for (const auto* sysCol: sysColPack.setTouched) {
            if (sysObjPack.unorderedMapKey.find(SysObjObj(sysCol->obj)) != sysObjPack.unorderedMapKey.end())
                continue;
            sysColPack.drop(ctx, sysCol->rowId);
            touched = true;
        }
        sysColPack.setTouched.clear();

        // SYS.DEFERRED_STG$
        for (const auto* sysDeferredStg: sysDeferredStgPack.setTouched) {
            if (sysObjPack.unorderedMapKey.find(SysObjObj(sysDeferredStg->obj)) != sysObjPack.unorderedMapKey.end())
                continue;
            sysDeferredStgPack.drop(ctx, sysDeferredStg->rowId);
            touched = true;
        }
        sysDeferredStgPack.setTouched.clear();

        // SYS.ECOL$
        for (const auto* sysECol: sysEColPack.setTouched) {
            if (sysObjPack.unorderedMapKey.find(SysObjObj(sysECol->tabObj)) != sysObjPack.unorderedMapKey.end())
                continue;
            sysEColPack.drop(ctx, sysECol->rowId);
            touched = true;
        }
        sysEColPack.setTouched.clear();

        // SYS.LOB$
        for (const auto* sysLob: sysLobPack.setTouched) {
            if (sysObjPack.unorderedMapKey.find(SysObjObj(sysLob->obj)) != sysObjPack.unorderedMapKey.end())
                continue;
            sysLobPack.drop(ctx, sysLob->rowId);
            touched = true;
        }
        sysLobPack.setTouched.clear();

        // SYS.LOBCOMPPART$
        for (const auto* sysLobCompPart: sysLobCompPartPack.setTouched) {
            if (sysLobPack.unorderedMapKey.find(SysLobLObj(sysLobCompPart->lObj)) != sysLobPack.unorderedMapKey.end())
                continue;
            sysLobCompPartPack.drop(ctx, sysLobCompPart->rowId);
            touched = true;
        }
        sysLobCompPartPack.setTouched.clear();

        // SYS.LOBFRAG$
        for (const auto* sysLobFrag: sysLobFragPack.setTouched) {
            if (sysLobCompPartPack.unorderedMapKey.find(SysLobCompPartPartObj(sysLobFrag->parentObj)) != sysLobCompPartPack.unorderedMapKey.end())
                continue;
            if (sysLobPack.unorderedMapKey.find(SysLobLObj(sysLobFrag->parentObj)) != sysLobPack.unorderedMapKey.end())
                continue;
            sysLobFragPack.drop(ctx, sysLobFrag->rowId);
            touched = true;
        }
        sysLobFragPack.setTouched.clear();

        // SYS.TAB$
        for (const auto* sysTab: sysTabPack.setTouched) {
            if (sysObjPack.unorderedMapKey.find(SysObjObj(sysTab->obj)) != sysObjPack.unorderedMapKey.end())
                continue;
            sysTabPartPack.drop(ctx, sysTab->rowId);
            touched = true;
        }
        sysTabPack.setTouched.clear();

        // SYS.TABCOMPART$
        for (const auto* sysTabComPart: sysTabComPartPack.setTouched) {
            if (sysObjPack.unorderedMapKey.find(SysObjObj(sysTabComPart->obj)) != sysObjPack.unorderedMapKey.end())
                continue;
            sysTabComPartPack.drop(ctx, sysTabComPart->rowId);
            touched = true;
        }
        sysTabComPartPack.setTouched.clear();

        // SYS.TABPART$
        for (const auto* sysTabPart: sysTabPartPack.setTouched) {
            if (sysObjPack.unorderedMapKey.find(SysObjObj(sysTabPart->bo)) != sysObjPack.unorderedMapKey.end())
                continue;
            sysTabPartPack.drop(ctx, sysTabPart->rowId);
            touched = true;
        }
        sysTabPartPack.setTouched.clear();

        // SYS.TABSUBPART$
        for (const auto* sysTabSubPart: sysTabSubPartPack.setTouched) {
            if (sysObjPack.unorderedMapKey.find(SysObjObj(sysTabSubPart->pObj)) != sysObjPack.unorderedMapKey.end())
                continue;
            sysTabSubPartPack.drop(ctx, sysTabSubPart->rowId);
            touched = true;
        }
        sysTabSubPartPack.setTouched.clear();
    }

    void Schema::resetTouched() {
        tablesTouched.clear();
        identifiersTouched.clear();
        sysCColPack.setTouched.clear();
        sysCDefPack.setTouched.clear();
        sysColPack.setTouched.clear();
        sysDeferredStgPack.setTouched.clear();
        sysEColPack.setTouched.clear();
        sysLobPack.setTouched.clear();
        sysLobCompPartPack.setTouched.clear();
        sysLobFragPack.setTouched.clear();
        sysObjPack.setTouched.clear();
        sysTabPack.setTouched.clear();
        sysTabComPartPack.setTouched.clear();
        sysTabPartPack.setTouched.clear();
        sysTabSubPartPack.setTouched.clear();
        sysUserPack.setTouched.clear();
        touched = false;
    }

    void Schema::updateXmlCtx() {
        if (ctx->isFlagSet(Ctx::REDO_FLAGS::EXPERIMENTAL_XMLTYPE)) {
            xmlCtxDefault = nullptr;
            auto schemaXmlMapIt = schemaXmlMap.cbegin();
            while (schemaXmlMapIt != schemaXmlMap.cend()) {
                if (schemaXmlMapIt->second->flags == 0) {
                    xmlCtxDefault = schemaXmlMapIt->second;
                    break;
                }
                ++schemaXmlMapIt;
            }
            if (unlikely(xmlCtxDefault == nullptr))
                throw DataException(50069, "no active XML context found");
        }
    }

    void Schema::buildMaps(const std::string& owner, const std::string& table, const std::vector<std::string>& keyList, const std::string& key,
                           SchemaElement::TAG_TYPE tagType, const std::vector<std::string>& tagList, const std::string& tag __attribute__((unused)),
                           const std::string& condition, DbTable::OPTIONS options, std::unordered_map<typeObj, std::string>& tablesUpdated,
                           bool suppLogDbPrimary, bool suppLogDbAll, uint64_t defaultCharacterMapId, uint64_t defaultCharacterNcharMapId) {
        const std::regex regexOwner(owner);
        const std::regex regexTable(table);
        char sysLobConstraintName[26]{"SYS_LOB0000000000C00000$$"};

        for (auto obj: identifiersTouched) {
            auto sysObjMapObjTouchedIt = sysObjPack.unorderedMapKey.find(SysObjObj(obj));
            if (sysObjMapObjTouchedIt == sysObjPack.unorderedMapKey.end())
                continue;
            SysObj* sysObj = sysObjMapObjTouchedIt->second;

            if (sysObj->isDropped() || !sysObj->isTable() || !regex_match(sysObj->name, regexTable))
                continue;

            SysUser* sysUser = nullptr;
            auto sysUserMapUserIt = sysUserPack.unorderedMapKey.find(SysUserUser(sysObj->owner));
            if (sysUserMapUserIt == sysUserPack.unorderedMapKey.end()) {
                if (!ctx->isFlagSet(Ctx::REDO_FLAGS::ADAPTIVE_SCHEMA))
                    continue;
                sysUserAdaptive.name = "USER_" + std::to_string(sysObj->obj);
                sysUser = &sysUserAdaptive;
            } else {
                sysUser = sysUserMapUserIt->second;
                if (!regex_match(sysUser->name, regexOwner))
                    continue;
            }

            // Table already added with another rule
            if (tableMap.find(sysObj->obj) != tableMap.end()) {
                if (ctx->isLogLevelAt(Ctx::LOG::DEBUG))
                    tablesUpdated[sysObj->obj] = sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - already added (skipped)";
                continue;
            }

            // Object without SYS.TAB$
            auto sysTabMapObjIt = sysTabPack.unorderedMapKey.find(SysTabObj(sysObj->obj));
            if (sysTabMapObjIt == sysTabPack.unorderedMapKey.end()) {
                if (ctx->isLogLevelAt(Ctx::LOG::DEBUG))
                    tablesUpdated[sysObj->obj] = sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - " + SysTab::tableName() +
                            " entry missing (skipped)";
                continue;
            }
            const SysTab* sysTab = sysTabMapObjIt->second;

            // Skip binary objects
            if (sysTab->isBinary()) {
                if (ctx->isLogLevelAt(Ctx::LOG::DEBUG))
                    tablesUpdated[sysObj->obj] = sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - binary (skipped";
                continue;
            }

            // Skip Index Organized Tables (IOT)
            if (sysTab->isIot()) {
                if (ctx->isLogLevelAt(Ctx::LOG::DEBUG))
                    tablesUpdated[sysObj->obj] = sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - IOT (skipped)";
                continue;
            }

            // Skip temporary tables
            if (sysObj->isTemporary()) {
                if (ctx->isLogLevelAt(Ctx::LOG::DEBUG))
                    tablesUpdated[sysObj->obj] = sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - temporary table (skipped)";
                continue;
            }

            // Skip nested tables
            if (sysTab->isNested()) {
                if (ctx->isLogLevelAt(Ctx::LOG::DEBUG))
                    tablesUpdated[sysObj->obj] = sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - nested table (skipped)";
                continue;
            }

            bool compressed = false;
            if (sysTab->isPartitioned())
                compressed = false;
            else if (sysTab->isInitial()) {
                const auto& it = sysDeferredStgPack.unorderedMapKey.find(SysDeferredStgObj(sysObj->obj));
                if (it != sysDeferredStgPack.unorderedMapKey.end())
                    compressed = it->second->isCompressed();
            }

            // Skip compressed tables
            if (compressed) {
                if (ctx->isLogLevelAt(Ctx::LOG::DEBUG))
                    tablesUpdated[sysObj->obj] = sysUser->name + "." + sysObj->name + " (obj: " + std::to_string(sysObj->obj) + ") - compressed table (skipped)";
                continue;
            }

            typeCol keysCnt = 0;
            bool suppLogTablePrimary = false;
            bool suppLogTableAll = false;
            bool supLogColMissing = false;

            tableTmp = new DbTable(sysObj->obj, sysTab->dataObj, sysObj->owner, sysTab->cluCols,
                                   options, sysUser->name, sysObj->name);

            uint64_t lobPartitions = 0;
            uint64_t lobIndexes = 0;
            std::ostringstream lobIndexesList;
            std::ostringstream lobList;
            uint64_t tablePartitions = 0;

            if (sysTab->isPartitioned()) {
                const SysTabPartKey sysTabPartKey(sysObj->obj, 0);
                for (auto sysTabPartMapKeyIt = sysTabPartPack.mapKey.upper_bound(sysTabPartKey);
                     sysTabPartMapKeyIt != sysTabPartPack.mapKey.end() && sysTabPartMapKeyIt->first.bo == sysObj->obj; ++sysTabPartMapKeyIt) {

                    const SysTabPart* sysTabPart = sysTabPartMapKeyIt->second;
                    tableTmp->addTablePartition(sysTabPart->obj, sysTabPart->dataObj);
                    ++tablePartitions;
                }

                const SysTabComPartKey sysTabComPartKey(sysObj->obj, 0);
                for (auto sysTabComPartMapKeyIt = sysTabComPartPack.mapKey.upper_bound(sysTabComPartKey);
                     sysTabComPartMapKeyIt != sysTabComPartPack.mapKey.end() && sysTabComPartMapKeyIt->first.bo == sysObj->obj; ++sysTabComPartMapKeyIt) {

                    const SysTabSubPartKey sysTabSubPartKeyFirst(sysTabComPartMapKeyIt->second->obj, 0);
                    for (auto sysTabSubPartMapKeyIt = sysTabSubPartPack.mapKey.upper_bound(sysTabSubPartKeyFirst);
                         sysTabSubPartMapKeyIt != sysTabSubPartPack.mapKey.end() && sysTabSubPartMapKeyIt->first.pObj == sysTabComPartMapKeyIt->second->obj;
                         ++sysTabSubPartMapKeyIt) {

                        const SysTabSubPart* sysTabSubPart = sysTabSubPartMapKeyIt->second;
                        tableTmp->addTablePartition(sysTabSubPart->obj, sysTabSubPart->dataObj);
                        ++tablePartitions;
                    }
                }
            }

            if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::SUPPLEMENTAL_LOG) && !DbTable::isSystemTable(options) &&
                !suppLogDbAll && !sysUser->isSuppLogAll()) {

                const SysCDefKey sysCDefKeyFirst(sysObj->obj, 0);
                for (auto sysCDefMapKeyIt = sysCDefPack.mapKey.upper_bound(sysCDefKeyFirst);
                     sysCDefMapKeyIt != sysCDefPack.mapKey.end() && sysCDefMapKeyIt->first.obj == sysObj->obj;
                     ++sysCDefMapKeyIt) {
                    const SysCDef* sysCDef = sysCDefMapKeyIt->second;
                    if (sysCDef->isSupplementalLogPK())
                        suppLogTablePrimary = true;
                    else if (sysCDef->isSupplementalLogAll())
                        suppLogTableAll = true;
                }
            }

            const RowId rowId;
            const SysColSeg sysColSegFirst(sysObj->obj, 0, rowId);
            for (auto sysColMapSegIt = sysColPack.mapKey.upper_bound(sysColSegFirst); sysColMapSegIt != sysColPack.mapKey.end() &&
                                                                                 sysColMapSegIt->first.obj == sysObj->obj; ++sysColMapSegIt) {
                SysCol* sysCol = sysColMapSegIt->second;
                if (sysCol->segCol == 0)
                    continue;

                uint64_t charmapId = 0;
                typeCol numPk = 0;
                typeCol numSup = 0;
                typeCol guardSeg = -1;

                const SysEColKey sysEColKey(sysObj->obj, sysCol->intCol);
                auto sysEColIt = sysEColPack.unorderedMapKey.find(sysEColKey);
                if (sysEColIt != sysEColPack.unorderedMapKey.end())
                    guardSeg = sysEColIt->second->guardId;

                if (sysCol->charsetForm == 1) {
                    if (sysCol->type == SysCol::COLTYPE::CLOB) {
                        charmapId = defaultCharacterNcharMapId;
                    } else
                        charmapId = defaultCharacterMapId;
                } else if (sysCol->charsetForm == 2)
                    charmapId = defaultCharacterNcharMapId;
                else
                    charmapId = sysCol->charsetId;

                if (sysCol->type == SysCol::COLTYPE::VARCHAR || sysCol->type == SysCol::COLTYPE::CHAR || sysCol->type == SysCol::COLTYPE::CLOB) {
                    auto characterMapIt = locales->characterMap.find(charmapId);
                    if (unlikely(characterMapIt == locales->characterMap.end())) {
                        ctx->hint("check in database for name: SELECT NLS_CHARSET_NAME(" + std::to_string(charmapId) + ") FROM DUAL;");
                        throw DataException(50026, "table " + std::string(sysUser->name) + "." + sysObj->name +
                                                   " - unsupported character set id: " + std::to_string(charmapId) + " for column: " + sysCol->name);
                    }
                }

                if (tagType == SchemaElement::TAG_TYPE::LIST)
                    tableTmp->tagCols.resize(tagList.size());

                const SysCColKey sysCColKeyFirst(sysObj->obj, 0, sysCol->intCol);
                for (auto sysCColMapKeyIt = sysCColPack.mapKey.upper_bound(sysCColKeyFirst);
                     sysCColMapKeyIt != sysCColPack.mapKey.end() && sysCColMapKeyIt->first.obj == sysObj->obj && sysCColMapKeyIt->first.intCol == sysCol->intCol;
                     ++sysCColMapKeyIt) {
                    SysCCol* sysCCol = sysCColMapKeyIt->second;

                    // Count the number of PKs the column is part of
                    auto sysCDefMapConIt = sysCDefPack.unorderedMapKey.find(SysCDefCon(sysCCol->con));
                    if (sysCDefMapConIt == sysCDefPack.unorderedMapKey.end()) {
                        ctx->warning(70005, "data in " + SysCDef::tableName() + " missing for CON#: " + std::to_string(sysCCol->con));
                        continue;
                    }
                    const SysCDef* sysCDef = sysCDefMapConIt->second;
                    if (sysCDef->isPK())
                        ++numPk;

                    // Supplemental logging
                    if (sysCCol->spare1.isZero() && sysCDef->isSupplementalLog())
                        ++numSup;
                }

                // Part of a defined primary key
                if (!keyList.empty()) {
                    // Manually defined PK overlaps with table PK
                    if (numPk > 0 && (suppLogTablePrimary || sysUser->isSuppLogPrimary() || suppLogDbPrimary))
                        numSup = 1;
                    numPk = 0;
                    for (const auto& val: keyList) {
                        if (sysCol->name == val) {
                            numPk = 1;
                            ++keysCnt;
                            if (numSup == 0)
                                supLogColMissing = true;
                            break;
                        }
                    }
                } else {
                    if (numPk > 0 && numSup == 0)
                        supLogColMissing = true;
                }

                //typeCol tagCol = -1;
                // Part of a defined tag
                switch (tagType) {
                    case SchemaElement::TAG_TYPE::NONE:
                        break;

                    case SchemaElement::TAG_TYPE::PK:
                        if (numPk > 0) {
                            //tagCol = tableTmp->tagCols.size();
                            tableTmp->tagCols.push_back(sysCol->segCol);
                        }
                        break;

                    case SchemaElement::TAG_TYPE::LIST: {
                        for (uint x = 0; x < tagList.size(); ++x) {
                            if (sysCol->name !=  tagList[x])
                                continue;

                            //tagCol = x;
                            tableTmp->tagCols[x] = sysCol->segCol;
                            break;
                        }
                        break;
                    }

                    case SchemaElement::TAG_TYPE::ALL:
                        //tagCol = tableTmp->tagCols.size();
                        tableTmp->tagCols.push_back(sysCol->segCol);
                        break;
                }

                bool xmlType = false;
                // For system-generated columns, check column name from base column
                std::string columnName = sysCol->name;
                if (sysCol->isSystemGenerated()) {
                    //RowId rid2(0, 0, 0);
                    //SysColSeg sysColSegFirst2(sysObj->obj - 1, 0, rid2);
                    for (auto sysColMapSegIt2 = sysColPack.mapKey.upper_bound(sysColSegFirst); sysColMapSegIt2 != sysColPack.mapKey.end() &&
                                                                                          sysColMapSegIt2->first.obj <= sysObj->obj; ++sysColMapSegIt2) {
                        const SysCol* sysCol2 = sysColMapSegIt2->second;
                        if (sysCol->col == sysCol2->col && sysCol2->segCol == 0) {
                            columnName = sysCol2->name;
                            xmlType = true;
                            break;
                        }
                    }
                }

                columnTmp = new DbColumn(sysCol->col, guardSeg, sysCol->segCol, columnName,
                                         sysCol->type, sysCol->length, sysCol->precision, sysCol->scale,
                                         charmapId, numPk, sysCol->isNullable(), sysCol->isHidden() &&
                                                                                 !(xmlType && ctx->isFlagSet(Ctx::REDO_FLAGS::EXPERIMENTAL_XMLTYPE)),
                                         sysCol->isStoredAsLob(), sysCol->isSystemGenerated(), sysCol->isNested(),
                                         sysCol->isUnused(), sysCol->isAdded(), sysCol->isGuard(), xmlType);

                tableTmp->addColumn(columnTmp);
                columnTmp = nullptr;
            }
            if (unlikely(tableTmp->columns.size() < static_cast<size_t>(tableTmp->maxSegCol))) {
                ctx->warning(50073, "table " + std::string(sysUser->name) + "." + sysObj->name + " - missmatch in column details: " +
                                    std::to_string(tableTmp->columns.size()) + " < " + std::to_string(tableTmp->maxSegCol));
                tableTmp->maxSegCol = tableTmp->columns.size();
            }

            if (!DbTable::isSystemTable(options)) {
                const SysLobKey sysLobKeyFirst(sysObj->obj, 0);
                for (auto sysLobMapKeyIt = sysLobPack.mapKey.upper_bound(sysLobKeyFirst);
                     sysLobMapKeyIt != sysLobPack.mapKey.end() && sysLobMapKeyIt->first.obj == sysObj->obj; ++sysLobMapKeyIt) {

                    const SysLob* sysLob = sysLobMapKeyIt->second;

                    auto sysObjMapObjIt = sysObjPack.unorderedMapKey.find(SysObjObj(sysLob->lObj));
                    if (unlikely(sysObjMapObjIt == sysObjPack.unorderedMapKey.end()))
                        throw DataException(50027, "table " + std::string(sysUser->name) + "." + sysObj->name + " couldn't find obj for lob " +
                                                   std::to_string(sysLob->lObj));
                    const typeObj lobDataObj = sysObjMapObjIt->second->dataObj;

                    if (ctx->isLogLevelAt(Ctx::LOG::DEBUG))
                        tablesUpdated[sysLob->lObj] = "LOB: " + std::to_string(sysObj->obj) + ":" + std::to_string(sysLob->col) + ":" +
                                std::to_string(sysLob->intCol) + ":" + std::to_string(lobDataObj) + ":" + std::to_string(sysLob->lObj);

                    lobTmp = new DbLob(tableTmp, sysLob->obj, lobDataObj, sysLob->lObj, sysLob->col,
                                       sysLob->intCol);

                    // Indexes
                    std::ostringstream str;
                    str << "SYS_IL" << std::setw(10) << std::setfill('0') << sysObj->obj << "C" << std::setw(5)
                        << std::setfill('0') << sysLob->intCol << "$$";
                    const std::string lobIndexName = str.str();

                    const SysObjNameKey sysObjNameKeyFirst(sysObj->owner, lobIndexName, 0, 0);
                    for (auto sysObjMapNameIt = sysObjPack.mapKey.upper_bound(sysObjNameKeyFirst);
                         sysObjMapNameIt != sysObjPack.mapKey.end() &&
                         sysObjMapNameIt->first.name == lobIndexName &&
                         sysObjMapNameIt->first.owner == sysObj->owner; ++sysObjMapNameIt) {

                        if (sysObjMapNameIt->first.dataObj == 0)
                            continue;

                        lobTmp->addIndex(sysObjMapNameIt->first.dataObj);
                        if (ctx->isTraceSet(Ctx::TRACE::LOB))
                            lobIndexesList << " " << std::dec << sysObjMapNameIt->first.dataObj << "/" << sysObjMapNameIt->second->obj;
                        ++lobIndexes;
                    }

                    if (lobTmp->lobIndexes.empty()) {
                        ctx->warning(60021, "missing LOB index for LOB (OBJ#: " + std::to_string(sysObj->obj) + ", DATAOBJ#: " +
                                            std::to_string(sysLob->lObj) + ", COL#: " + std::to_string(sysLob->intCol) + ")");
                    }

                    // Partitioned lob
                    if (sysTab->isPartitioned()) {
                        // Partitions
                        const SysLobFragKey sysLobFragKey(sysLob->lObj, 0);
                        for (auto sysLobFragMapKeyIt = sysLobFragPack.mapKey.upper_bound(sysLobFragKey);
                             sysLobFragMapKeyIt != sysLobFragPack.mapKey.end() &&
                             sysLobFragMapKeyIt->first.parentObj == sysLob->lObj; ++sysLobFragMapKeyIt) {

                            const SysLobFrag* sysLobFrag = sysLobFragMapKeyIt->second;
                            auto sysObjMapObjIt2 = sysObjPack.unorderedMapKey.find(SysObjObj(sysLobFrag->fragObj));
                            if (unlikely(sysObjMapObjIt2 == sysObjPack.unorderedMapKey.end()))
                                throw DataException(50028, "table " + std::string(sysUser->name) + "." + sysObj->name +
                                                           " couldn't find obj for lob frag " + std::to_string(sysLobFrag->fragObj));
                            const typeObj lobFragDataObj = sysObjMapObjIt2->second->dataObj;

                            lobTmp->addPartition(lobFragDataObj, getLobBlockSize(sysLobFrag->ts));
                            ++lobPartitions;
                        }

                        // Subpartitions
                        const SysLobCompPartKey sysLobCompPartKey(sysLob->lObj, 0);
                        for (auto sysLobCompPartMapKeyIt = sysLobCompPartPack.mapKey.upper_bound(sysLobCompPartKey);
                             sysLobCompPartMapKeyIt != sysLobCompPartPack.mapKey.end() &&
                             sysLobCompPartMapKeyIt->first.lObj == sysLob->lObj; ++sysLobCompPartMapKeyIt) {

                            const SysLobCompPart* sysLobCompPart = sysLobCompPartMapKeyIt->second;

                            const SysLobFragKey sysLobFragKey2(sysLobCompPart->partObj, 0);
                            for (auto sysLobFragMapKeyIt = sysLobFragPack.mapKey.upper_bound(sysLobFragKey2);
                                 sysLobFragMapKeyIt != sysLobFragPack.mapKey.end() &&
                                 sysLobFragMapKeyIt->first.parentObj == sysLobCompPart->partObj; ++sysLobFragMapKeyIt) {

                                const SysLobFrag* sysLobFrag = sysLobFragMapKeyIt->second;
                                auto sysObjMapObjIt2 = sysObjPack.unorderedMapKey.find(SysObjObj(sysLobFrag->fragObj));
                                if (unlikely(sysObjMapObjIt2 == sysObjPack.unorderedMapKey.end()))
                                    throw DataException(50028, "table " + std::string(sysUser->name) + "." + sysObj->name +
                                                               " couldn't find obj for lob frag " + std::to_string(sysLobFrag->fragObj));
                                const typeObj lobFragDataObj = sysObjMapObjIt2->second->dataObj;

                                lobTmp->addPartition(lobFragDataObj, getLobBlockSize(sysLobFrag->ts));
                                ++lobPartitions;
                            }
                        }
                    }

                    lobTmp->addPartition(lobTmp->dataObj, getLobBlockSize(sysLob->ts));
                    tableTmp->addLob(lobTmp);
                    if (ctx->isTraceSet(Ctx::TRACE::LOB))
                        lobList << " " << std::dec << lobTmp->obj << "/" << lobTmp->dataObj << "/" << std::dec << lobTmp->lObj;
                    lobTmp = nullptr;
                }

                // 0123456 7890123456 7 89012 34
                // SYS_LOB xxxxxxxxxx C yyyyy $$
                typeObj obj2 = sysObj->obj;
                for (uint j = 0; j < 10; ++j) {
                    sysLobConstraintName[16 - j] = (obj2 % 10) + '0';
                    obj2 /= 10;
                }

                const SysObjNameKey sysObjNameKeyName(sysObj->owner, sysLobConstraintName, 0, 0);
                for (auto sysObjMapNameIt = sysObjPack.mapKey.upper_bound(sysObjNameKeyName); sysObjMapNameIt != sysObjPack.mapKey.end();
                     ++sysObjMapNameIt) {
                    SysObj* sysObjLob = sysObjMapNameIt->second;
                    const char* colStr = sysObjLob->name.c_str();

                    if (sysObjLob->name.length() != 25 || memcmp(colStr, sysLobConstraintName, 18) != 0 || colStr[23] != '$' || colStr[24] != '$')
                        continue;

                    // Decode column id
                    typeCol col = 0;
                    for (uint j = 0; j < 5; ++j) {
                        col += colStr[18 + j] - '0';
                        col *= 10;
                    }

                    // FIXME: potentially slow for tables with large number of LOB columns
                    DbLob* dbLob = nullptr;
                    for (auto* lobIt: tableTmp->lobs) {
                        if (lobIt->intCol == col) {
                            dbLob = lobIt;
                            break;
                        }
                    }

                    if (dbLob == nullptr) {
                        lobTmp = new DbLob(tableTmp, sysObj->obj, 0, 0, col, col);
                        tableTmp->addLob(lobTmp);
                        dbLob = lobTmp;
                        lobTmp = nullptr;
                    }

                    dbLob->addPartition(sysObjLob->dataObj, getLobBlockSize(sysTab->ts));
                }
            }

            // Check if a table has all listed columns
            if (unlikely(static_cast<typeCol>(keyList.size()) != keysCnt))
                throw DataException(10041, "table " + std::string(sysUser->name) + "." + sysObj->name + " - couldn't find all column sets (" +
                                           key + ")");

            if (tagType == SchemaElement::TAG_TYPE::LIST)
                for (uint x = 0; x < tableTmp->tagCols.size(); ++x)
                    if (tableTmp->tagCols[x] == 0)
                        throw DataException(10041, "table " + std::string(sysUser->name) + "." + sysObj->name + " - couldn't find all tag sets (" +
                                                   tagList[x] + ")");

            std::ostringstream ss;
            ss << sysUser->name << "." << sysObj->name << " (dataobj: " << std::dec << sysTab->dataObj << ", obj: " << std::dec << sysObj->obj <<
               ", columns: " << std::dec << tableTmp->maxSegCol << ", lobs: " << std::dec << tableTmp->totalLobs << lobList.str() <<
               ", lob-idx: " << std::dec << lobIndexes << lobIndexesList.str() << ")";
            if (sysTab->isClustered())
                ss << ", part of cluster";
            if (sysTab->isPartitioned())
                ss << ", partitioned(table: " << std::dec << tablePartitions << ", lob: " << lobPartitions << ")";
            if (sysTab->isDependencies())
                ss << ", row dependencies";
            if (sysTab->isRowMovement())
                ss << ", row movement enabled";

            if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::SUPPLEMENTAL_LOG) && !DbTable::isSystemTable(options)) {
                // Use a default primary key
                if (keyList.empty()) {
                    if (tableTmp->totalPk == 0)
                        ss << ", primary key missing";
                    else if (!suppLogTablePrimary && !suppLogTableAll && !sysUser->isSuppLogPrimary() && !sysUser->isSuppLogAll() &&
                             !suppLogDbPrimary && !suppLogDbAll && supLogColMissing)
                        ss << ", supplemental log missing, try: ALTER TABLE " << sysUser->name << "." << sysObj->name <<
                           " ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;";

                // User defined primary key
                } else {
                    if (!suppLogTableAll && !sysUser->isSuppLogAll() && !suppLogDbAll && supLogColMissing)
                        ss << ", supplemental log missing, try: ALTER TABLE " << sysUser->name << "." << sysObj->name << " ADD SUPPLEMENTAL LOG GROUP GRP" <<
                           std::dec << sysObj->obj << " (" << key << ") ALWAYS;";
                }
            }
            tablesUpdated[sysObj->obj] = ss.str();

            tableTmp->setCondition(condition);
            addTableToDict(tableTmp);
            tableTmp = nullptr;
        }
    }

    uint16_t Schema::getLobBlockSize(typeTs ts) const {
        const auto& it = sysTsPack.unorderedMapKey.find(SysTsTs(ts));
        if (it != sysTsPack.unorderedMapKey.end()) {
            const typeDba pageSize = it->second->blockSize;
            if (pageSize == 8192)
                return 8132;
            if (pageSize == 16384)
                return 16264;
            if (pageSize == 32768)
                return 32528;
            ctx->warning(60022, "missing TS#: " + std::to_string(ts) + ", BLOCKSIZE: " + std::to_string(pageSize) + ")");
        } else
            ctx->warning(60022, "missing TS#: " + std::to_string(ts) + ")");

        // Default value?
        return 8132;
    }
}
