/* Base class for handling of schema
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

#include <list>
#include <regex>
#include <unistd.h>

#include "global.h"
#include "ConfigurationException.h"
#include "OracleAnalyzer.h"
#include "OracleColumn.h"
#include "OracleIncarnation.h"
#include "OracleObject.h"
#include "OutputBuffer.h"
#include "Reader.h"
#include "RowId.h"
#include "RuntimeException.h"
#include "Schema.h"
#include "SchemaElement.h"
#include "State.h"

namespace OpenLogReplicator {
    Schema::Schema(OracleAnalyzer* oracleAnalyzer) :
        oracleAnalyzer(oracleAnalyzer),
        schemaObject(nullptr),
        schemaColumn(nullptr),
        sysCColTouched(false),
        sysCDefTouched(false),
        sysColTouched(false),
        sysDeferredStgTouched(false),
        sysEColTouched(false),
        sysObjTouched(false),
        sysTabTouched(false),
        sysTabComPartTouched(false),
        sysTabPartTouched(false),
        sysTabSubPartTouched(false),
        sysUserTouched(false),
        touched(false),
        savedDeleted(false) {
    }

    Schema::~Schema() {
        dropSchema();
        for (SchemaElement* element : elements)
            delete element;
        elements.clear();
        users.clear();
    }

    void Schema::dropSchema(void) {
        if (schemaObject != nullptr) {
            delete schemaObject;
            schemaObject = nullptr;
        }

        if (schemaColumn != nullptr) {
            delete schemaColumn;
            schemaColumn = nullptr;
        }

        partitionMap.clear();

        for (auto it : objectMap) {
            OracleObject* object = it.second;
            delete object;
        }
        objectMap.clear();

        for (auto it : sysCColMapRowId) {
            SysCCol* sysCCol = it.second;
            delete sysCCol;
        }
        sysCColMapRowId.clear();
        sysCColMapKey.clear();

        for (auto it : sysCDefMapRowId) {
            SysCDef* sysCDef = it.second;
            delete sysCDef;
        }
        sysCDefMapRowId.clear();
        sysCDefMapCon.clear();
        sysCDefMapKey.clear();

        for (auto it : sysColMapRowId) {
            SysCol* sysCol = it.second;
            delete sysCol;
        }
        sysColMapRowId.clear();
        sysColMapKey.clear();
        sysColMapSeg.clear();

        for (auto it : sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = it.second;
            delete sysDeferredStg;
        }
        sysDeferredStgMapRowId.clear();
        sysDeferredStgMapObj.clear();

        for (auto it : sysEColMapRowId) {
            SysECol* sysECol = it.second;
            delete sysECol;
        }
        sysEColMapRowId.clear();
        sysEColMapKey.clear();

        for (auto it : sysObjMapRowId) {
            SysObj* sysObj = it.second;
            delete sysObj;
        }
        sysObjMapRowId.clear();
        sysObjMapObj.clear();

        for (auto it : sysTabMapRowId) {
            SysTab* sysTab = it.second;
            delete sysTab;
        }
        sysTabMapRowId.clear();
        sysTabMapObj.clear();

        for (auto it : sysTabComPartMapRowId) {
            SysTabComPart* sysTabComPart = it.second;
            delete sysTabComPart;
        }
        sysTabComPartMapRowId.clear();
        sysTabComPartMapObj.clear();
        sysTabComPartMapKey.clear();

        for (auto it : sysTabPartMapRowId) {
            SysTabPart* sysTabPart = it.second;
            delete sysTabPart;
        }
        sysTabPartMapRowId.clear();
        sysTabPartMapKey.clear();

        for (auto it : sysTabSubPartMapRowId) {
            SysTabSubPart* sysTabSubPart = it.second;
            delete sysTabSubPart;
        }
        sysTabSubPartMapRowId.clear();
        sysTabSubPartMapKey.clear();

        for (auto it : sysUserMapRowId) {
            SysUser* sysUser = it.second;
            delete sysUser;
        }
        sysUserMapRowId.clear();
        sysUserMapUser.clear();
    }

    bool Schema::readSchema(void) {
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: searching for previous schema");

        std::set<std::string> namesList;
        oracleAnalyzer->state->list(namesList);

        typeSCN fileScnMax = ZERO_SCN;
        for (std::string jsonName : namesList) {
            std::string prefix(oracleAnalyzer->database + "-schema-");
            if (jsonName.length() < prefix.length() || jsonName.substr(0, prefix.length()).compare(prefix) != 0)
                continue;

            TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: found previous schema: " << jsonName);
            std::string fileScnStr(jsonName.substr(prefix.length(), jsonName.length()));
            typeSCN fileScn;
            try {
                fileScn = strtoull(fileScnStr.c_str(), nullptr, 10);
            } catch (std::exception& e) {
                //ignore other files
                continue;
            }
            if (fileScn <= oracleAnalyzer->firstScn && (fileScn > fileScnMax || fileScnMax == ZERO_SCN))
                fileScnMax = fileScn;
            if (oracleAnalyzer->schemaFirstScn == ZERO_SCN || oracleAnalyzer->schemaFirstScn > fileScn)
                oracleAnalyzer->schemaFirstScn = fileScn;
            schemaScnList.insert(fileScn);
        }

        //none found
        if (fileScnMax == ZERO_SCN)
            return false;

        bool toDrop = false;
        bool firstFound = false;
        std::set<typeSCN>::iterator it = schemaScnList.end();

        while (it != schemaScnList.begin()) {
            --it;
            std::string jsonName(oracleAnalyzer->database + "-schema-" + std::to_string(*it));

            toDrop = false;
            if (*it > oracleAnalyzer->firstScn && oracleAnalyzer->firstScn != ZERO_SCN) {
                toDrop = true;
            } else {
                if (readSchema(jsonName, *it))
                    toDrop = true;
            }

            if (toDrop) {
                if ((oracleAnalyzer->flags & REDO_FLAGS_CHECKPOINT_KEEP) == 0) {
                    TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: delete: " << jsonName << " scn: " << std::dec << *it);
                    oracleAnalyzer->state->drop(jsonName);
                }
                it = schemaScnList.erase(it);
            }
        }

        return true;
    }

    bool Schema::readSchema(std::string& jsonName, typeSCN fileScn) {
        if (oracleAnalyzer->schemaScn != ZERO_SCN)
            return true;
        dropSchema();

        INFO("reading schema for " << oracleAnalyzer->database << " for scn: " << fileScn);
        std::string schemaJSON;
        rapidjson::Document document;
        oracleAnalyzer->state->read(jsonName, SCHEMA_FILE_MAX_SIZE, schemaJSON, false);

        if (schemaJSON.length() == 0 || document.Parse(schemaJSON.c_str()).HasParseError()) {
            WARNING("parsing " << jsonName << " at offset: " << document.GetErrorOffset() << ", message: " << GetParseError_En(document.GetParseError()));
            return false;
        }

        //SYS.USER$
        const rapidjson::Value& sysUserJSON = getJSONfieldA(jsonName, document, "sys-user");

        for (rapidjson::SizeType i = 0; i < sysUserJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(jsonName, SYSUSER_ROWID_LENGTH, sysUserJSON[i], "row-id");
            typeUSER user = getJSONfieldU32(jsonName, sysUserJSON[i], "user");
            const char* name = getJSONfieldS(jsonName, SYSUSER_NAME_LENGTH, sysUserJSON[i], "name");

            const rapidjson::Value& spare1JSON = getJSONfieldA(jsonName, sysUserJSON[i], "spare1");
            if (spare1JSON.Size() != 2) {
                WARNING("bad JSON in " << jsonName << ", spare1 should be an array with 2 elements");
                return false;
            }
            uint64_t spare11 = getJSONfieldU64(jsonName, spare1JSON, "spare1", 0);
            uint64_t spare12 = getJSONfieldU64(jsonName, spare1JSON, "spare1", 1);
            uint64_t single = getJSONfieldU64(jsonName, sysUserJSON[i], "single");

            dictSysUserAdd(rowId, user, name, spare11, spare12, single);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.USER$: " << std::dec << sysUserJSON.Size());

        //SYS.OBJ$
        const rapidjson::Value& sysObjJSON = getJSONfieldA(jsonName, document, "sys-obj");

        for (rapidjson::SizeType i = 0; i < sysObjJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(jsonName, SYSOBJ_ROWID_LENGTH, sysObjJSON[i], "row-id");
            typeUSER owner = getJSONfieldU32(jsonName, sysObjJSON[i], "owner");
            typeOBJ obj = getJSONfieldU32(jsonName, sysObjJSON[i], "obj");
            typeDATAOBJ dataObj = getJSONfieldU32(jsonName, sysObjJSON[i], "data-obj");
            typeTYPE type = getJSONfieldU16(jsonName, sysObjJSON[i], "type");
            const char* name = getJSONfieldS(jsonName, SYSOBJ_NAME_LENGTH, sysObjJSON[i], "name");

            const rapidjson::Value& flagsJSON = getJSONfieldA(jsonName, sysObjJSON[i], "flags");
            if (flagsJSON.Size() != 2) {
                WARNING("bad JSON in " << jsonName << ", flags should be an array with 2 elements");
                return false;
            }
            uint64_t flags1 = getJSONfieldU64(jsonName, flagsJSON, "flags", 0);
            uint64_t flags2 = getJSONfieldU64(jsonName, flagsJSON, "flags", 1);
            uint64_t single = getJSONfieldU64(jsonName, sysObjJSON[i], "single");

            dictSysObjAdd(rowId, owner, obj, dataObj, type, name, flags1, flags2, single);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.OBJ$: " << std::dec << sysObjJSON.Size());

        //SYS.COL$
        const rapidjson::Value& sysColJSON = getJSONfieldA(jsonName, document, "sys-col");

        for (rapidjson::SizeType i = 0; i < sysColJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(jsonName, SYSCOL_ROWID_LENGTH, sysColJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU32(jsonName, sysColJSON[i], "obj");
            typeCOL col = getJSONfieldI16(jsonName, sysColJSON[i], "col");
            typeCOL segCol = getJSONfieldI16(jsonName, sysColJSON[i], "seg-col");
            typeCOL intCol = getJSONfieldI16(jsonName, sysColJSON[i], "int-col");
            const char* name = getJSONfieldS(jsonName, SYSCOL_NAME_LENGTH, sysColJSON[i], "name");
            typeTYPE type = getJSONfieldU16(jsonName, sysColJSON[i], "type");
            uint64_t length = getJSONfieldU64(jsonName, sysColJSON[i], "length");
            int64_t precision = getJSONfieldI64(jsonName, sysColJSON[i], "precision");
            int64_t scale = getJSONfieldI64(jsonName, sysColJSON[i], "scale");
            uint64_t charsetForm = getJSONfieldU64(jsonName, sysColJSON[i], "charset-form");
            uint64_t charsetId = getJSONfieldU64(jsonName, sysColJSON[i], "charset-id");
            int64_t null_ = getJSONfieldI64(jsonName, sysColJSON[i], "null");
            const rapidjson::Value& propertyJSON = getJSONfieldA(jsonName, sysColJSON[i], "property");
            if (propertyJSON.Size() != 2) {
                WARNING("bad JSON in " << jsonName << ", property should be an array with 2 elements");
                return false;
            }
            uint64_t property1 = getJSONfieldU64(jsonName, propertyJSON, "property", 0);
            uint64_t property2 = getJSONfieldU64(jsonName, propertyJSON, "property", 1);

            dictSysColAdd(rowId, obj, col, segCol, intCol, name, type, length, precision, scale, charsetForm, charsetId, null_, property1, property2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.COL$: " << std::dec << sysColJSON.Size());

        //SYS.CCOL$
        const rapidjson::Value& sysCColJSON = getJSONfieldA(jsonName, document, "sys-ccol");

        for (rapidjson::SizeType i = 0; i < sysCColJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(jsonName, SYSCCOL_ROWID_LENGTH, sysCColJSON[i], "row-id");
            typeCON con = getJSONfieldU32(jsonName, sysCColJSON[i], "con");
            typeCON intCol = getJSONfieldI16(jsonName, sysCColJSON[i], "int-col");
            typeOBJ obj = getJSONfieldU32(jsonName, sysCColJSON[i], "obj");
            const rapidjson::Value& spare1JSON = getJSONfieldA(jsonName, sysCColJSON[i], "spare1");
            if (spare1JSON.Size() != 2) {
                WARNING("bad JSON in " << jsonName << ", spare1 should be an array with 2 elements");
                return false;
            }
            uint64_t spare11 = getJSONfieldU64(jsonName, spare1JSON, "spare1", 0);
            uint64_t spare12 = getJSONfieldU64(jsonName, spare1JSON, "spare1", 1);

            dictSysCColAdd(rowId, con, intCol, obj, spare11, spare12);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.CCOL$: " << std::dec << sysCColJSON.Size());

        //SYS.CDEF$
        const rapidjson::Value& sysCDefJSON = getJSONfieldA(jsonName, document, "sys-cdef");

        for (rapidjson::SizeType i = 0; i < sysCDefJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(jsonName, SYSCDEF_ROWID_LENGTH, sysCDefJSON[i], "row-id");
            typeCON con = getJSONfieldU32(jsonName, sysCDefJSON[i], "con");
            typeOBJ obj = getJSONfieldU32(jsonName, sysCDefJSON[i], "obj");
            typeTYPE type = getJSONfieldU16(jsonName, sysCDefJSON[i], "type");

            dictSysCDefAdd(rowId, con, obj, type);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.CDEF$: " << std::dec << sysCDefJSON.Size());

        //SYS.DEFERRED_STG$
        const rapidjson::Value& sysDeferredStgJSON = getJSONfieldA(jsonName, document, "sys-deferredstg");

        for (rapidjson::SizeType i = 0; i < sysDeferredStgJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(jsonName, SYSDEFERREDSTG_ROWID_LENGTH, sysDeferredStgJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU32(jsonName, sysDeferredStgJSON[i], "obj");

            const rapidjson::Value& flagsStgJSON = getJSONfieldA(jsonName, sysDeferredStgJSON[i], "flags-stg");
            if (flagsStgJSON.Size() != 2) {
                WARNING("bad JSON in " << jsonName << ", flags-stg should be an array with 2 elements");
                return false;
            }
            uint64_t flagsStg1 = getJSONfieldU64(jsonName, flagsStgJSON, "flags-stg", 0);
            uint64_t flagsStg2 = getJSONfieldU64(jsonName, flagsStgJSON, "flags-stg", 1);

            dictSysDeferredStgAdd(rowId, obj, flagsStg1, flagsStg2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.DEFERRED_STG$: " << std::dec << sysDeferredStgJSON.Size());

        //SYS.ECOL$
        const rapidjson::Value& sysEColJSON = getJSONfieldA(jsonName, document, "sys-ecol");

        for (rapidjson::SizeType i = 0; i < sysEColJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(jsonName, SYSECOL_ROWID_LENGTH, sysEColJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU32(jsonName, sysEColJSON[i], "tab-obj");
            typeCOL colNum = getJSONfieldI16(jsonName, sysEColJSON[i], "col-num");
            typeCOL guardId = getJSONfieldI16(jsonName, sysEColJSON[i], "guard-id");

            dictSysEColAdd(rowId, obj, colNum, guardId);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.ECOL$: " << std::dec << sysEColJSON.Size());

        //SYS.TAB$
        const rapidjson::Value& sysTabJSON = getJSONfieldA(jsonName, document, "sys-tab");

        for (rapidjson::SizeType i = 0; i < sysTabJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(jsonName, SYSTAB_ROWID_LENGTH, sysTabJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU32(jsonName, sysTabJSON[i], "obj");
            typeDATAOBJ dataObj = getJSONfieldU32(jsonName, sysTabJSON[i], "data-obj");
            typeCOL cluCols = getJSONfieldI16(jsonName, sysTabJSON[i], "clu-cols");

            const rapidjson::Value& flagsJSON = getJSONfieldA(jsonName, sysTabJSON[i], "flags");
            if (flagsJSON.Size() != 2) {
                WARNING("bad JSON in " << jsonName << ", flags should be an array with 2 elements");
                return false;
            }
            uint64_t flags1 = getJSONfieldU64(jsonName, flagsJSON, "flags", 0);
            uint64_t flags2 = getJSONfieldU64(jsonName, flagsJSON, "flags", 1);

            const rapidjson::Value& propertyJSON = getJSONfieldA(jsonName, sysTabJSON[i], "property");
            if (propertyJSON.Size() != 2) {
                WARNING("bad JSON in " << jsonName << ", property should be an array with 2 elements");
                return false;
            }
            uint64_t property1 = getJSONfieldU64(jsonName, propertyJSON, "property", 0);
            uint64_t property2 = getJSONfieldU64(jsonName, propertyJSON, "property", 1);

            dictSysTabAdd(rowId, obj, dataObj, cluCols, flags1, flags2, property1, property2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TAB$: " << std::dec << sysTabJSON.Size());

        //SYS.TABPART$
        const rapidjson::Value& sysTabPartJSON = getJSONfieldA(jsonName, document, "sys-tabpart");

        for (rapidjson::SizeType i = 0; i < sysTabPartJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(jsonName, SYSTABPART_ROWID_LENGTH, sysTabPartJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU32(jsonName, sysTabPartJSON[i], "obj");
            typeDATAOBJ dataObj = getJSONfieldU32(jsonName, sysTabPartJSON[i], "data-obj");
            typeOBJ bo = getJSONfieldU32(jsonName, sysTabPartJSON[i], "bo");

            dictSysTabPartAdd(rowId, obj, dataObj, bo);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABPART$: " << std::dec << sysTabPartJSON.Size());

        //SYS.TABCOMPART$
        const rapidjson::Value& sysTabComPartJSON = getJSONfieldA(jsonName, document, "sys-tabcompart");

        for (rapidjson::SizeType i = 0; i < sysTabComPartJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(jsonName, SYSTABCOMPART_ROWID_LENGTH, sysTabComPartJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU32(jsonName, sysTabComPartJSON[i], "obj");
            typeDATAOBJ dataObj = getJSONfieldU32(jsonName, sysTabComPartJSON[i], "data-obj");
            typeOBJ bo = getJSONfieldU32(jsonName, sysTabComPartJSON[i], "bo");

            dictSysTabComPartAdd(rowId, obj, dataObj, bo);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABCOMPART$: " << std::dec << sysTabComPartJSON.Size());

        //SYS.TABSUBPART$
        const rapidjson::Value& sysTabSubPartJSON = getJSONfieldA(jsonName, document, "sys-tabsubpart");

        for (rapidjson::SizeType i = 0; i < sysTabSubPartJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(jsonName, SYSTABSUBPART_ROWID_LENGTH, sysTabSubPartJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU32(jsonName, sysTabSubPartJSON[i], "obj");
            typeDATAOBJ dataObj = getJSONfieldU32(jsonName, sysTabSubPartJSON[i], "data-obj");
            typeOBJ pObj = getJSONfieldU32(jsonName, sysTabSubPartJSON[i], "p-obj");

            dictSysTabSubPartAdd(rowId, obj, dataObj, pObj);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABSUBPART$: " << std::dec << sysTabSubPartJSON.Size());

        //database metadata
        const char* databaseRead = getJSONfieldS(jsonName, JSON_PARAMETER_LENGTH, document, "database");
        if (oracleAnalyzer->database.compare(databaseRead) != 0) {
            WARNING("invalid database for " << jsonName << " - " << databaseRead << " instead of " << oracleAnalyzer->database << " - skipping file");
            return false;
        }

        uint64_t bigEndian = getJSONfieldU64(jsonName, document, "big-endian");
        if (bigEndian == 1)
            oracleAnalyzer->setBigEndian();
        else if (bigEndian != 0) {
            WARNING("invalid \"big-endian\" value " << jsonName << " - " << std::dec << bigEndian << " - skipping file");
            return false;
        }

        typeACTIVATION activationRead = getJSONfieldU32(jsonName, document, "activation");
        if (oracleAnalyzer->activation == 0)
            oracleAnalyzer->activation = activationRead;

        std::string contextRead(getJSONfieldS(jsonName, VCONTEXT_LENGTH, document, "context"));
        if (oracleAnalyzer->context.length() == 0)
            oracleAnalyzer->context = contextRead;
        else if (oracleAnalyzer->context.compare(contextRead) != 0) {
            WARNING("invalid \"context\" for " << jsonName << " - " << std::dec << contextRead << " instead of " << oracleAnalyzer->context << " - skipping file");
            return false;
        }

        typeCONID conIdRead = getJSONfieldI16(jsonName, document, "con-id");
        if (oracleAnalyzer->conId == -1)
            oracleAnalyzer->conId = conIdRead;
        else if (oracleAnalyzer->conId != conIdRead) {
            WARNING("invalid \"con-id\" for " << jsonName << " - " << std::dec << conIdRead << " instead of " << oracleAnalyzer->conId << " - skipping file");
            return false;
        }

        std::string conNameRead = getJSONfieldS(jsonName, VCONTEXT_LENGTH, document, "con-name");
        if (oracleAnalyzer->conName.length() == 0)
            oracleAnalyzer->conName = conNameRead;
        else if (oracleAnalyzer->conName.compare(conNameRead) != 0) {
            WARNING("invalid \"con-name\" for " << jsonName << " - " << conNameRead << " instead of " << oracleAnalyzer->conName << " - skipping file");
            return false;
        }

        if (oracleAnalyzer->dbRecoveryFileDest.length() == 0)
            oracleAnalyzer->dbRecoveryFileDest = getJSONfieldS(jsonName, VPARAMETER_LENGTH, document, "db-recovery-file-dest");

        if (oracleAnalyzer->dbBlockChecksum.length() == 0)
            oracleAnalyzer->dbBlockChecksum = getJSONfieldS(jsonName, VPARAMETER_LENGTH, document, "db-block-checksum");

        if (oracleAnalyzer->logArchiveFormat.length() == 0)
            oracleAnalyzer->logArchiveFormat = getJSONfieldS(jsonName, VPARAMETER_LENGTH, document, "log-archive-format");

        if (oracleAnalyzer->logArchiveDest.length() == 0)
            oracleAnalyzer->logArchiveDest = getJSONfieldS(jsonName, VPARAMETER_LENGTH, document, "log-archive-dest");

        if (oracleAnalyzer->nlsCharacterSet.length() == 0)
            oracleAnalyzer->nlsCharacterSet = getJSONfieldS(jsonName, VPROPERTY_LENGTH, document, "nls-character-set");

        if (oracleAnalyzer->nlsNcharCharacterSet.length() == 0)
            oracleAnalyzer->nlsNcharCharacterSet = getJSONfieldS(jsonName, VPROPERTY_LENGTH, document, "nls-nchar-character-set");

        if (oracleAnalyzer->outputBuffer->defaultCharacterMapId == 0)
            oracleAnalyzer->outputBuffer->setNlsCharset(oracleAnalyzer->nlsCharacterSet, oracleAnalyzer->nlsNcharCharacterSet);

        if (oracleAnalyzer->readers.size() == 0) {
            const rapidjson::Value& onlineRedoJSON = getJSONfieldA(jsonName, document, "online-redo");

            for (rapidjson::SizeType i = 0; i < onlineRedoJSON.Size(); ++i) {
                uint64_t group = getJSONfieldI64(jsonName, onlineRedoJSON[i], "group");
                const rapidjson::Value& path = getJSONfieldA(jsonName, onlineRedoJSON[i], "path");

                Reader* onlineReader = oracleAnalyzer->readerCreate(group);
                if (onlineReader != nullptr) {
                    onlineReader->paths.clear();
                    for (rapidjson::SizeType j = 0; j < path.Size(); ++j) {
                        const rapidjson::Value& pathVal = path[j];
                        onlineReader->paths.push_back(pathVal.GetString());
                    }
                }
            }

            if ((oracleAnalyzer->flags & REDO_FLAGS_ARCH_ONLY) == 0)
                oracleAnalyzer->checkOnlineRedoLogs();

            oracleAnalyzer->archReader = oracleAnalyzer->readerCreate(0);
        }

        if (oracleAnalyzer->oiSet.size() == 0) {
            const rapidjson::Value& incarnationsJSON = getJSONfieldA(jsonName, document, "incarnations");
            for (rapidjson::SizeType i = 0; i < incarnationsJSON.Size(); ++i) {
                uint32_t incarnation = getJSONfieldU32(jsonName, incarnationsJSON[i], "incarnation");
                typeSCN resetlogsScn = getJSONfieldU64(jsonName, incarnationsJSON[i], "resetlogs-scn");
                typeSCN priorResetlogsScn = getJSONfieldU64(jsonName, incarnationsJSON[i], "prior-resetlogs-scn");
                const char* status = getJSONfieldS(jsonName, 128, incarnationsJSON[i], "status");
                typeRESETLOGS resetlogs = getJSONfieldU32(jsonName, incarnationsJSON[i], "resetlogs");
                uint32_t priorIncarnation = getJSONfieldU32(jsonName, incarnationsJSON[i], "prior-incarnation");

                OracleIncarnation *oi = new OracleIncarnation(incarnation, resetlogsScn, priorResetlogsScn, status,
                        resetlogs, priorIncarnation);
                if (oi == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << (sizeof(OracleIncarnation)) << " bytes memory (for: oracle incarnation)");
                }
                oracleAnalyzer->oiSet.insert(oi);

                if (oi->current)
                    oracleAnalyzer->oiCurrent = oi;
            }
        }

        const rapidjson::Value& usersJSON = getJSONfieldA(jsonName, document, "users");
        for (rapidjson::SizeType i = 0; i < usersJSON.Size(); ++i) {
            const rapidjson::Value& userJSON = usersJSON[i];;
            users.insert(userJSON.GetString());
        }

        //rebuild object structures
        for (SchemaElement* element : elements) {
            DEBUG("- creating table schema for owner: " << element->owner << " table: " << element->table << " options: " <<
                    (uint64_t) element->options);

            if ((element->options & OPTIONS_SYSTEM_TABLE) == 0 && users.find(element->owner) == users.end()) {
                RUNTIME_FAIL("owner \"" << element->owner << "\" is missing in schema file: " <<
                        jsonName << " - recreate schema file (delete old file and force creation of new)");
            }
            buildMaps(element->owner, element->table, element->keys, element->keysStr, element->options, true);
        }
        oracleAnalyzer->schemaScn = fileScn;

        return false;
    }

    void Schema::writeSchema(void) {
        if (oracleAnalyzer->schemaScn == ZERO_SCN && (oracleAnalyzer->flags & REDO_FLAGS_SCHEMALESS) != 0)
            return;

        std::string jsonName(oracleAnalyzer->database + "-schema-" + std::to_string(oracleAnalyzer->schemaScn));
        TRACE(TRACE2_SYSTEM, "SYSTEM: writing: " << jsonName << " scn: " << std::dec << oracleAnalyzer->schemaScn);

        std::stringstream ss;
        ss << "{\"database\":\"" << oracleAnalyzer->database << "\"," <<
                "\"big-endian\":" << std::dec << oracleAnalyzer->bigEndian << "," <<
                "\"activation\":" << std::dec << oracleAnalyzer->activation << "," <<
                "\"context\":\"" << oracleAnalyzer->context << "\"," <<
                "\"con-id\":" << std::dec << oracleAnalyzer->conId << "," <<
                "\"con-name\":\"" << oracleAnalyzer->conName << "\"," <<
                "\"db-recovery-file-dest\":\"";
        writeEscapeValue(ss, oracleAnalyzer->dbRecoveryFileDest);
        ss << "\"," << "\"db-block-checksum\":\"";
        writeEscapeValue(ss, oracleAnalyzer->dbBlockChecksum);
        ss << "\"," << "\"log-archive-dest\":\"";
        writeEscapeValue(ss, oracleAnalyzer->logArchiveDest);
        ss << "\"," << "\"log-archive-format\":\"";
        writeEscapeValue(ss, oracleAnalyzer->logArchiveFormat);
        ss << "\"," << "\"nls-character-set\":\"";
        writeEscapeValue(ss, oracleAnalyzer->nlsCharacterSet);
        ss << "\"," << "\"nls-nchar-character-set\":\"";
        writeEscapeValue(ss, oracleAnalyzer->nlsNcharCharacterSet);

        ss << "\"," SCHEMA_ENDL << "\"online-redo\":[";

        bool hasPrev = false;
        bool hasPrev2;
        for (Reader* reader : oracleAnalyzer->readers) {
            if (reader->group == 0)
                continue;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            hasPrev2 = false;
            ss SCHEMA_ENDL << "{\"group\":" << reader->group << ",\"path\":[";
            for (std::string& path : reader->paths) {
                if (hasPrev2)
                    ss << ",";
                else
                    hasPrev2 = true;

                ss << "\"";
                writeEscapeValue(ss, path);
                ss << "\"";
            }
            ss << "]}";
        }

        ss << "]," SCHEMA_ENDL << "\"incarnations\":[";
        hasPrev = false;
        for (OracleIncarnation* oi : oracleAnalyzer->oiSet) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;
            ss SCHEMA_ENDL << "{\"incarnation\":" << oi->incarnation << "," <<
                    "\"resetlogs-scn\":" << oi->resetlogsScn << "," <<
                    "\"prior-resetlogs-scn\":" << oi->priorResetlogsScn << "," <<
                    "\"status\":\"" << oi->status << "\"," <<
                    "\"resetlogs\":" << oi->resetlogs << "," <<
                    "\"prior-incarnation\":" << oi->priorIncarnation << "}";
        }

        ss << "]," SCHEMA_ENDL << "\"users\":[";
        hasPrev = false;
        for (std::string user : users) {
            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;
            ss SCHEMA_ENDL << "\"" << user << "\"";
        }

        //SYS.CCOL$
        ss << "]," SCHEMA_ENDL << "\"sys-ccol\":[";
        hasPrev = false;
        for (auto it : sysCColMapRowId) {
            SysCCol* sysCCol = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysCCol->rowId << "\"," <<
                    "\"con\":" << std::dec << sysCCol->con << "," <<
                    "\"int-col\":" << std::dec << sysCCol->intCol << "," <<
                    "\"obj\":" << std::dec << sysCCol->obj << "," <<
                    "\"spare1\":" << std::dec << sysCCol->spare1 << "}";
            sysCCol->saved = true;
        }

        //SYS.CDEF$
        ss << "]," SCHEMA_ENDL << "\"sys-cdef\":[";
        hasPrev = false;
        for (auto it : sysCDefMapRowId) {
            SysCDef* sysCDef = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysCDef->rowId << "\"," <<
                    "\"con\":" << std::dec << sysCDef->con << "," <<
                    "\"obj\":" << std::dec << sysCDef->obj << "," <<
                    "\"type\":" << std::dec << sysCDef->type << "}";
            sysCDef->saved = true;
        }

        //SYS.COL$
        ss << "]," SCHEMA_ENDL << "\"sys-col\":[";
        hasPrev = false;
        for (auto it : sysColMapRowId) {
            SysCol* sysCol = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysCol->rowId << "\"," <<
                    "\"obj\":" << std::dec << sysCol->obj << "," <<
                    "\"col\":" << std::dec << sysCol->col << "," <<
                    "\"seg-col\":" << std::dec << sysCol->segCol << "," <<
                    "\"int-col\":" << std::dec << sysCol->intCol << "," <<
                    "\"name\":\"" << sysCol->name << "\"," <<
                    "\"type\":" << std::dec << sysCol->type << "," <<
                    "\"length\":" << std::dec << sysCol->length << "," <<
                    "\"precision\":" << std::dec << sysCol->precision << "," <<
                    "\"scale\":" << std::dec << sysCol->scale << "," <<
                    "\"charset-form\":" << std::dec << sysCol->charsetForm << "," <<
                    "\"charset-id\":" << std::dec << sysCol->charsetId << "," <<
                    "\"null\":" << std::dec << sysCol->null_ << "," <<
                    "\"property\":" << sysCol->property << "}";
            sysCol->saved = true;
        }

        //SYS.DEFERRED_STG$
        ss << "]," SCHEMA_ENDL << "\"sys-deferredstg\":[";
        hasPrev = false;
        for (auto it : sysDeferredStgMapRowId) {
            SysDeferredStg* sysDeferredStg = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysDeferredStg->rowId << "\"," <<
                    "\"obj\":" << std::dec << sysDeferredStg->obj << "," <<
                    "\"flags-stg\":" << std::dec << sysDeferredStg->flagsStg << "}";
            sysDeferredStg->saved = true;
        }

        //SYS.ECOL$
        ss << "]," SCHEMA_ENDL << "\"sys-ecol\":[";
        hasPrev = false;
        for (auto it : sysEColMapRowId) {
            SysECol* sysECol = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysECol->rowId << "\"," <<
                    "\"tab-obj\":" << std::dec << sysECol->tabObj << "," <<
                    "\"col-num\":" << std::dec << sysECol->colNum << "," <<
                    "\"guard-id\":" << std::dec << sysECol->guardId << "}";
            sysECol->saved = true;
        }

        //SYS.OBJ$
        ss << "]," SCHEMA_ENDL << "\"sys-obj\":[";
        hasPrev = false;
        for (auto it : sysObjMapRowId) {
            SysObj* sysObj = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysObj->rowId << "\"," <<
                    "\"owner\":" << std::dec << sysObj->owner << "," <<
                    "\"obj\":" << std::dec << sysObj->obj << "," <<
                    "\"data-obj\":" << std::dec << sysObj->dataObj << "," <<
                    "\"name\":\"" << sysObj->name << "\"," <<
                    "\"type\":" << std::dec << sysObj->type << "," <<
                    "\"flags\":" << std::dec << sysObj->flags << "," <<
                    "\"single\":" << std::dec << (uint64_t)sysObj->single <<"}";
            sysObj->saved = true;
        }

        //SYS.TAB$
        ss << "]," SCHEMA_ENDL << "\"sys-tab\":[";
        hasPrev = false;
        for (auto it : sysTabMapRowId) {
            SysTab* sysTab = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysTab->rowId << "\"," <<
                    "\"obj\":" << std::dec << sysTab->obj << "," <<
                    "\"data-obj\":" << std::dec << sysTab->dataObj << "," <<
                    "\"clu-cols\":" << std::dec << sysTab->cluCols << "," <<
                    "\"flags\":" << std::dec << sysTab->flags << "," <<
                    "\"property\":" << std::dec << sysTab->property << "}";
            sysTab->saved = true;
        }

        //SYS.TABCOMPART$
        ss << "]," SCHEMA_ENDL << "\"sys-tabcompart\":[";
        hasPrev = false;
        for (auto it : sysTabComPartMapRowId) {
            SysTabComPart* sysTabComPart = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysTabComPart->rowId << "\"," <<
                    "\"obj\":" << std::dec << sysTabComPart->obj << "," <<
                    "\"data-obj\":" << std::dec << sysTabComPart->dataObj << "," <<
                    "\"bo\":" << std::dec << sysTabComPart->bo << "}";
            sysTabComPart->saved = true;
        }

        //SYS.TABPART$
        ss << "]," SCHEMA_ENDL << "\"sys-tabpart\":[";
        hasPrev = false;
        for (auto it : sysTabPartMapRowId) {
            SysTabPart* sysTabPart = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysTabPart->rowId << "\"," <<
                    "\"obj\":" << std::dec << sysTabPart->obj << "," <<
                    "\"data-obj\":" << std::dec << sysTabPart->dataObj << "," <<
                    "\"bo\":" << std::dec << sysTabPart->bo << "}";
            sysTabPart->saved = true;
        }

        //SYS.TABSUBPART$
        ss << "]," SCHEMA_ENDL << "\"sys-tabsubpart\":[";
        hasPrev = false;
        for (auto it : sysTabSubPartMapRowId) {
            SysTabSubPart* sysTabSubPart = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysTabSubPart->rowId << "\"," <<
                    "\"obj\":" << std::dec << sysTabSubPart->obj << "," <<
                    "\"data-obj\":" << std::dec << sysTabSubPart->dataObj << "," <<
                    "\"p-obj\":" << std::dec << sysTabSubPart->pObj << "}";
            sysTabSubPart->saved = true;
        }

        //SYS.USER$
        ss << "]," SCHEMA_ENDL << "\"sys-user\":[";
        hasPrev = false;
        for (auto it : sysUserMapRowId) {
            SysUser* sysUser = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysUser->rowId << "\"," <<
                    "\"user\":" << std::dec << sysUser->user << "," <<
                    "\"name\":\"" << sysUser->name << "\"," <<
                    "\"spare1\":" << std::dec << sysUser->spare1 << "," <<
                    "\"single\":" << std::dec << (uint64_t)sysUser->single << "}";
            sysUser->saved = true;
        }

        ss << "]}";
        oracleAnalyzer->state->write(jsonName, ss);

        savedDeleted = false;

        schemaScnList.insert(oracleAnalyzer->schemaScn);
        if (oracleAnalyzer->checkpointScn != ZERO_SCN) {
            bool unlinkFile = false;
            bool firstFound = false;
            std::set<typeSCN>::iterator it = schemaScnList.end();

            while (it != schemaScnList.begin()) {
                --it;
                std::string jsonName(oracleAnalyzer->database + "-schema-" + std::to_string(*it));

                unlinkFile = false;
                if (*it > oracleAnalyzer->schemaScn) {
                    continue;
                } else {
                    if (!firstFound)
                        firstFound = true;
                    else
                        unlinkFile = true;
                }

                if (unlinkFile) {
                    if ((oracleAnalyzer->flags & REDO_FLAGS_SCHEMA_KEEP) == 0) {
                        TRACE(TRACE2_SYSTEM, "SYSTEM: delete: " << jsonName << " schema scn: " << std::dec << *it);
                        oracleAnalyzer->state->drop(jsonName);
                    }
                    it = schemaScnList.erase(it);
                }
            }
        } else {
            TRACE(TRACE2_SYSTEM, "SYSTEM: no delete, no scn checkpoint present");
        }
    }

    void Schema::addToDict(OracleObject* object) {
        if (objectMap.find(object->obj) != objectMap.end()) {
            CONFIG_FAIL("can't add object (obj: " << std::dec << object->obj << ", dataObj: " << object->dataObj << ")");
        }
        objectMap[object->obj] = object;

        if (partitionMap.find(object->obj) != partitionMap.end()) {
            CONFIG_FAIL("can't add partition (obj: " << std::dec << object->obj << ", dataObj: " << object->dataObj << ")");
        }
        partitionMap[object->obj] = object;

        for (typeOBJ2 objx : object->partitions) {
            typeOBJ partitionObj = objx >> 32;
            typeDATAOBJ partitionDataObj = objx & 0xFFFFFFFF;

            if (partitionMap.find(partitionObj) != partitionMap.end()) {
                CONFIG_FAIL("can't add partition element (obj: " << std::dec << partitionObj << ", dataObj: " << partitionDataObj << ")");
            }
            partitionMap[partitionObj] = object;
        }
    }

    void Schema::removeFromDict(OracleObject* object) {
        if (objectMap.find(object->obj) == objectMap.end()) {
            CONFIG_FAIL("can't remove object (obj: " << std::dec << object->obj << ", dataObj: " << object->dataObj << ")");
        }
        objectMap.erase(object->obj);

        if (partitionMap.find(object->obj) == partitionMap.end()) {
            CONFIG_FAIL("can't remove partition (obj: " << std::dec << object->obj << ", dataObj: " << object->dataObj << ")");
        }
        partitionMap.erase(object->obj);

        for (typeOBJ2 objx : object->partitions) {
            typeOBJ partitionObj = objx >> 32;
            typeDATAOBJ partitionDataObj = objx & 0xFFFFFFFF;

            if (partitionMap.find(partitionObj) == partitionMap.end()) {
                CONFIG_FAIL("can't remove partition element (obj: " << std::dec << partitionObj << ", dataObj: " << partitionDataObj << ")");
            }
            partitionMap.erase(partitionObj);
        }
    }

    OracleObject* Schema::checkDict(typeOBJ obj, typeDATAOBJ dataObj) {
        auto it = partitionMap.find(obj);
        if (it == partitionMap.end())
            return nullptr;
        return it->second;
    }

    std::stringstream& Schema::writeEscapeValue(std::stringstream& ss, std::string& str) {
        const char* c_str = str.c_str();
        for (uint64_t i = 0; i < str.length(); ++i) {
            if (*c_str == '\t' || *c_str == '\r' || *c_str == '\n' || *c_str == '\b') {
                //skip
            } else if (*c_str == '"' || *c_str == '\\' /* || *c_str == '/' */) {
                ss << '\\' << *c_str;
            } else {
                ss << *c_str;
            }
            ++c_str;
        }
        return ss;
    }

    bool Schema::refreshIndexes(void) {
        bool changedSchema = savedDeleted;

        std::list<RowId> removeRowId;
        //SYS.USER$
        if (sysUserTouched) {
            sysUserMapUser.clear();

            for (auto it : sysUserMapRowId) {
                SysUser* sysUser = it.second;

                if (sysUser->single || users.find(sysUser->name) != users.end()) {
                    sysUserMapUser[sysUser->user] = sysUser;
                    if (sysUser->touched) {
                        touchUser(sysUser->user);
                        sysUser->touched = false;
                        changedSchema = true;
                    }
                    continue;
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage USER$ (rowid: " << it.first << ", USER#: " << std::dec << sysUser->user << ", NAME: " <<
                        sysUser->name << ", SPARE1: " << sysUser->spare1 << ")");
                removeRowId.push_back(it.first);
                delete sysUser;
            }

            for (RowId rowId: removeRowId)
                sysUserMapRowId.erase(rowId);
            removeRowId.clear();
            sysUserTouched = false;
        }

        //SYS.OBJ$
        if (sysObjTouched) {
            sysObjMapObj.clear();
            for (auto it : sysObjMapRowId) {
                SysObj* sysObj = it.second;

                auto sysUserIt = sysUserMapUser.find(sysObj->owner);
                if (sysUserIt != sysUserMapUser.end()) {
                    SysUser* sysUser = sysUserIt->second;
                    if (!sysUser->single || sysObj->single) {
                        sysObjMapObj[sysObj->obj] = sysObj;
                        if (sysObj->touched) {
                            touchObj(sysObj->obj);
                            sysObj->touched = false;
                            changedSchema = true;
                        }
                        continue;
                    }
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage OBJ$ (rowid: " << it.first << ", OWNER#: " << std::dec << sysObj->owner << ", OBJ#: " <<
                        sysObj->obj << ", DATAOBJ#: " << sysObj->dataObj << ", TYPE#: " << sysObj->type << ", NAME: '" << sysObj->name <<
                        "', FLAGS: " << sysObj->flags << ")");
                removeRowId.push_back(it.first);
                delete sysObj;
            }

            for (RowId rowId: removeRowId)
                sysObjMapRowId.erase(rowId);
            removeRowId.clear();
            sysObjTouched = false;
        }

        //SYS.CCOL$
        if (sysCColTouched) {
            sysCColMapKey.clear();

            for (auto it : sysCColMapRowId) {
                SysCCol* sysCCol = it.second;

                if (sysObjMapObj.find(sysCCol->obj) != sysObjMapObj.end()) {
                    SysCColKey sysCColKey(sysCCol->obj, sysCCol->intCol, sysCCol->con);
                    sysCColMapKey[sysCColKey] = sysCCol;
                    if (sysCCol->touched) {
                        touchObj(sysCCol->obj);
                        sysCCol->touched = false;
                        changedSchema = true;
                    }
                    continue;
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage CCOL$ (rowid: " << it.first << ", CON#: " << std::dec << sysCCol->con << ", INTCOL#: " <<
                        sysCCol->intCol << ", OBJ#: " << sysCCol->obj << ", SPARE1: " << sysCCol->spare1 << ")");
                removeRowId.push_back(it.first);
                delete sysCCol;
            }

            for (RowId rowId: removeRowId)
                sysCColMapRowId.erase(rowId);
            removeRowId.clear();
            sysCColTouched = false;
        }

        //SYS.CDEF$
        if (sysCDefTouched) {
            sysCDefMapKey.clear();
            sysCDefMapCon.clear();

            for (auto it : sysCDefMapRowId) {
                SysCDef* sysCDef = it.second;

                if (sysObjMapObj.find(sysCDef->obj) != sysObjMapObj.end()) {
                    SysCDefKey sysCDefKey(sysCDef->obj, sysCDef->con);
                    sysCDefMapKey[sysCDefKey] = sysCDef;
                    sysCDefMapCon[sysCDef->con] = sysCDef;
                    if (sysCDef->touched) {
                        touchObj(sysCDef->obj);
                        sysCDef->touched = false;
                        changedSchema = true;
                    }
                    continue;
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage CDEF$ (rowid: " << it.first << ", CON#: " << std::dec << sysCDef->con << ", OBJ#: " <<
                        sysCDef->obj << ", type: " << sysCDef->type << ")");
                removeRowId.push_back(it.first);
                delete sysCDef;
            }

            for (RowId rowId: removeRowId)
                sysCDefMapRowId.erase(rowId);
            removeRowId.clear();
            sysCDefTouched = false;
        }

        //SYS.COL$
        if (sysColTouched) {
            sysColMapKey.clear();
            sysColMapSeg.clear();

            for (auto it : sysColMapRowId) {
                SysCol* sysCol = it.second;

                if (sysObjMapObj.find(sysCol->obj) != sysObjMapObj.end()) {
                    SysColKey sysColKey(sysCol->obj, sysCol->intCol);
                    sysColMapKey[sysColKey] = sysCol;
                    SysColSeg sysColSeg(sysCol->obj, sysCol->segCol);
                    sysColMapSeg[sysColSeg] = sysCol;
                    if (sysCol->touched) {
                        touchObj(sysCol->obj);
                        sysCol->touched = false;
                        changedSchema = true;
                    }
                    continue;
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage COL$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysCol->obj << ", COL#: " << sysCol->col <<
                        ", SEGCOL#: " << sysCol->segCol << ", INTCOL#: " << sysCol->intCol << ", NAME: '" << sysCol->name << "', TYPE#: " <<
                        sysCol->type << ", LENGTH: " << sysCol->length << ", PRECISION#: " << sysCol->precision << ", SCALE: " << sysCol->scale <<
                        ", CHARSETFORM: " << sysCol->charsetForm << ", CHARSETID: " << sysCol->charsetId << ", NULL$: " << sysCol->null_ <<
                        ", PROPERTY: " << sysCol->property << ")");
                removeRowId.push_back(it.first);
                delete sysCol;
            }

            for (RowId rowId: removeRowId)
                sysColMapRowId.erase(rowId);
            removeRowId.clear();
            sysColTouched = false;
        }

        //SYS.DEFERRED_STG$
        if (sysDeferredStgTouched) {
            sysDeferredStgMapObj.clear();

            for (auto it : sysDeferredStgMapRowId) {
                SysDeferredStg* sysDeferredStg = it.second;

                if (sysObjMapObj.find(sysDeferredStg->obj) != sysObjMapObj.end()) {
                    sysDeferredStgMapObj[sysDeferredStg->obj] = sysDeferredStg;
                    if (sysDeferredStg->touched) {
                        touchObj(sysDeferredStg->obj);
                        sysDeferredStg->touched = false;
                        changedSchema = true;
                    }
                    continue;
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage DEFERRED_STG$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysDeferredStg->obj <<
                        ", FLAGS_STG: " << sysDeferredStg->flagsStg << ")");
                removeRowId.push_back(it.first);
                delete sysDeferredStg;
            }

            for (RowId rowId: removeRowId)
                sysDeferredStgMapRowId.erase(rowId);
            removeRowId.clear();
            sysDeferredStgTouched = false;
        }

        //SYS.ECOL$
        if (sysEColTouched) {
            sysEColMapKey.clear();

            for (auto it : sysEColMapRowId) {
                SysECol* sysECol = it.second;

                if (sysObjMapObj.find(sysECol->tabObj) != sysObjMapObj.end()) {
                    SysEColKey sysEColKey(sysECol->tabObj, sysECol->colNum);
                    sysEColMapKey[sysEColKey] = sysECol;
                    if (sysECol->touched) {
                        touchObj(sysECol->tabObj);
                        sysECol->touched = false;
                        changedSchema = true;
                    }
                    continue;
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage ECOL$ (rowid: " << it.first << ", TABOBJ#: " << std::dec << sysECol->tabObj << ", COLNUM: " <<
                        sysECol->colNum << ", GUARD_ID: " << sysECol->guardId << ")");
                removeRowId.push_back(it.first);
                delete sysECol;
            }

            for (RowId rowId: removeRowId)
                sysEColMapRowId.erase(rowId);
            removeRowId.clear();
            sysEColTouched = false;
        }

        //SYS.TAB$
        if (sysTabTouched) {
            sysTabMapObj.clear();

            for (auto it : sysTabMapRowId) {
                SysTab* sysTab = it.second;

                if (sysObjMapObj.find(sysTab->obj) != sysObjMapObj.end()) {
                    sysTabMapObj[sysTab->obj] = sysTab;
                    if (sysTab->touched) {
                        touchObj(sysTab->obj);
                        sysTab->touched = false;
                        changedSchema = true;
                    }
                    continue;
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TAB$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysTab->obj << ", DATAOBJ#: " <<
                        sysTab->dataObj << ", CLUCOLS: " << sysTab->cluCols << ", FLAGS: " << sysTab->flags << ", PROPERTY: " << sysTab->property << ")");
                removeRowId.push_back(it.first);
                delete sysTab;
            }

            for (RowId rowId: removeRowId)
                sysTabMapRowId.erase(rowId);
            removeRowId.clear();
            sysTabTouched = false;
        }

        //SYS.TABCOMPART$
        if (sysTabComPartTouched) {
            sysTabComPartMapKey.clear();
            sysTabComPartMapObj.clear();

            for (auto it : sysTabComPartMapRowId) {
                SysTabComPart* sysTabComPart = it.second;

                sysTabComPartMapObj[sysTabComPart->obj] = sysTabComPart;
                if (sysObjMapObj.find(sysTabComPart->obj) != sysObjMapObj.end()) {
                    SysTabComPartKey sysTabComPartKey(sysTabComPart->bo, sysTabComPart->obj);
                    sysTabComPartMapKey[sysTabComPartKey] = sysTabComPart;
                    if (sysTabComPart->touched) {
                        touchObj(sysTabComPart->bo);
                        sysTabComPart->touched = false;
                        changedSchema = true;
                    }
                    continue;
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABCOMPART$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysTabComPart->obj << ", DATAOBJ#: " <<
                        sysTabComPart->dataObj << ", BO#: " << sysTabComPart->bo << ")");
                removeRowId.push_back(it.first);
                delete sysTabComPart;
            }

            for (RowId rowId: removeRowId)
                sysTabComPartMapRowId.erase(rowId);
            removeRowId.clear();
            sysTabComPartTouched = false;
        }

        //SYS.TABPART$
        if (sysTabPartTouched) {
            sysTabPartMapKey.clear();

            for (auto it : sysTabPartMapRowId) {
                SysTabPart* sysTabPart = it.second;

                if (sysObjMapObj.find(sysTabPart->obj) != sysObjMapObj.end()) {
                    SysTabPartKey sysTabPartKey(sysTabPart->bo, sysTabPart->obj);
                    sysTabPartMapKey[sysTabPartKey] = sysTabPart;
                    if (sysTabPart->touched) {
                        touchObj(sysTabPart->bo);
                        sysTabPart->touched = false;
                        changedSchema = true;
                    }
                    continue;
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABPART$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysTabPart->obj << ", DATAOBJ#: " <<
                        sysTabPart->dataObj << ", BO#: " << sysTabPart->bo << ")");
                removeRowId.push_back(it.first);
                delete sysTabPart;
            }

            for (RowId rowId: removeRowId)
                sysTabPartMapRowId.erase(rowId);
            removeRowId.clear();
            sysTabPartTouched = false;
        }

        //SYS.TABSUBPART$
        if (sysTabSubPartTouched) {
            sysTabSubPartMapKey.clear();

            for (auto it : sysTabSubPartMapRowId) {
                SysTabSubPart* sysTabSubPart = it.second;

                if (sysObjMapObj.find(sysTabSubPart->obj) != sysObjMapObj.end()) {
                    SysTabSubPartKey sysTabSubPartKey(sysTabSubPart->pObj, sysTabSubPart->obj);
                    sysTabSubPartMapKey[sysTabSubPartKey] = sysTabSubPart;
                    if (sysTabSubPart->touched) {
                        //find SYS.TABCOMPART$
                        auto it = sysTabComPartMapObj.find(sysTabSubPart->pObj);
                        if (it != sysTabComPartMapObj.end()) {
                            SysTabComPart* sysTabComPart = it->second;
                            touchObj(sysTabComPart->bo);
                        }

                        sysTabSubPart->touched = false;
                        changedSchema = true;
                    }
                    continue;
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABSUBPART$ (rowid: " << it.first << ", OBJ#: " << std::dec << sysTabSubPart->obj << ", DATAOBJ#: " <<
                        sysTabSubPart->dataObj << ", POBJ#: " << sysTabSubPart->pObj << ")");
                removeRowId.push_back(it.first);
                delete sysTabSubPart;
            }

            for (RowId rowId: removeRowId)
                sysTabSubPartMapRowId.erase(rowId);
            removeRowId.clear();
            sysTabSubPartTouched = false;
        }

        touched = false;
        return changedSchema;
    }

    void Schema::rebuildMaps(void) {
        for (typeUSER user : usersTouched) {
            for (auto it = objectMap.cbegin(); it != objectMap.cend() ; ) {

                OracleObject* object = it->second;
                if (object->user == user) {
                    removeFromDict(object);
                    INFO("dropped schema: " << object->owner << "." << object->name << " (dataobj: " << std::dec << object->dataObj
                            << ", obj: " << object->obj << ")");
                    objectMap.erase(it++);
                    delete object;
                } else {
                    ++it;
                }
            }
        }
        usersTouched.clear();

        for (typeOBJ obj : partitionsTouched) {
            auto objectMapIt = partitionMap.find(obj);
            if (objectMapIt != partitionMap.end()) {
                OracleObject* object = objectMapIt->second;
                touchObj(object->obj);
            }
        }
        partitionsTouched.clear();

        for (typeOBJ obj : objectsTouched) {
            auto objectMapIt = objectMap.find(obj);
            if (objectMapIt != objectMap.end()) {
                OracleObject* object = objectMapIt->second;
                removeFromDict(object);
                INFO("dropped schema: " << object->owner << "." << object->name << " (dataobj: " << std::dec << object->dataObj
                        << ", obj: " << object->obj << ")");
                delete object;
            }
        }
        objectsTouched.clear();

        for (SchemaElement* element : elements)
            buildMaps(element->owner, element->table, element->keys, element->keysStr, element->options, false);
    }

    void Schema::buildMaps(std::string& owner, std::string& table, std::vector<std::string>& keys, std::string& keysStr, typeOPTIONS options, bool output) {
        uint64_t tabCnt = 0;
        std::regex regexOwner(owner);
        std::regex regexTable(table);

        for (auto itObj : sysObjMapRowId) {
            SysObj* sysObj = itObj.second;
            if (sysObj->isDropped() || !sysObj->isTable() || !regex_match(sysObj->name, regexTable))
                continue;

            auto sysUserMapUserIt = sysUserMapUser.find(sysObj->owner);
            if (sysUserMapUserIt == sysUserMapUser.end())
                continue;
            SysUser* sysUser = sysUserMapUserIt->second;
            if (!regex_match(sysUser->name, regexOwner))
                continue;

            //table already added with another rule
            if (objectMap.find(sysObj->obj) != objectMap.end()) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << std::dec << sysObj->obj << ") - already added");
                continue;
            }

            //object without SYS.TAB$
            auto sysTabMapObjIt = sysTabMapObj.find(sysObj->obj);
            if (sysTabMapObjIt == sysTabMapObj.end()) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << std::dec << sysObj->obj << ") - SYS.TAB$ entry missing");
                continue;
            }
            SysTab* sysTab = sysTabMapObjIt->second;

            //skip binary objects
            if (sysTab->isBinary()) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << std::dec << sysObj->obj << ") - binary");
                continue;
            }

            //skip Index Organized Tables (IOT)
            if (sysTab->isIot()) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << std::dec << sysObj->obj << ") - IOT");
                continue;
            }

            //skip temporary tables
            if (sysObj->isTemporary()) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << std::dec << sysObj->obj << ") - temporary table");
                continue;
            }

            //skip nested tables
            if (sysTab->isNested()) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << std::dec << sysObj->obj << ") - nested table");
                continue;
            }

            bool compressed = false;
            if (sysTab->isPartitioned())
                compressed = false;
            else if (sysTab->isInitial()) {
                SysDeferredStg* sysDeferredStg = sysDeferredStgMapObj[sysObj->obj];
                if (sysDeferredStg != nullptr)
                    compressed = sysDeferredStg->isCompressed();
            }
            //skip compressed tables
            if (compressed) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << std::dec << sysObj->obj << ") - compressed table");
                continue;
            }

            typeCOL totalPk = 0;
            typeCOL maxSegCol = 0;
            typeCOL keysCnt = 0;
            bool suppLogTablePrimary = false;
            bool suppLogTableAll = false;
            bool supLogColMissing = false;

            schemaObject = new OracleObject(sysObj->obj, sysTab->dataObj, sysObj->owner, sysTab->cluCols, options, sysUser->name, sysObj->name);
            if (schemaObject == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OracleObject) << " bytes memory (for: object creation2)");
            }
            ++tabCnt;

            uint64_t partitions = 0;
            if (sysTab->isPartitioned()) {
                SysTabPartKey sysTabPartKeyFirst(sysObj->obj, 0);
                for (auto itTabPart = sysTabPartMapKey.upper_bound(sysTabPartKeyFirst);
                        itTabPart != sysTabPartMapKey.end() && itTabPart->first.bo == sysObj->obj; ++itTabPart) {

                    SysTabPart* sysTabPart = itTabPart->second;
                    schemaObject->addPartition(sysTabPart->obj, sysTabPart->dataObj);
                    ++partitions;
                }

                SysTabComPartKey sysTabComPartKeyFirst(sysObj->obj, 0);
                for (auto itTabComPart = sysTabComPartMapKey.upper_bound(sysTabComPartKeyFirst);
                        itTabComPart != sysTabComPartMapKey.end() && itTabComPart->first.bo == sysObj->obj; ++itTabComPart) {

                    SysTabSubPartKey sysTabSubPartKeyFirst(itTabComPart->second->obj, 0);
                    for (auto itTabSubPart = sysTabSubPartMapKey.upper_bound(sysTabSubPartKeyFirst);
                            itTabSubPart != sysTabSubPartMapKey.end() && itTabSubPart->first.pObj == itTabComPart->second->obj; ++itTabSubPart) {

                        SysTabSubPart* sysTabSubPart = itTabSubPart->second;
                        schemaObject->addPartition(itTabSubPart->second->obj, itTabSubPart->second->dataObj);
                        ++partitions;
                    }
                }
            }

            if ((oracleAnalyzer->disableChecks & DISABLE_CHECK_SUPPLEMENTAL_LOG) == 0 && (options & OPTIONS_SYSTEM_TABLE) == 0 &&
                    !oracleAnalyzer->suppLogDbAll && !sysUser->isSuppLogAll()) {

                SysCDefKey sysCDefKeyFirst(sysObj->obj, 0);
                for (auto itCDef = sysCDefMapKey.upper_bound(sysCDefKeyFirst);
                        itCDef != sysCDefMapKey.end() && itCDef->first.obj == sysObj->obj;
                        ++itCDef) {
                    SysCDef* sysCDef = itCDef->second;
                    if (sysCDef->isSupplementalLogPK())
                        suppLogTablePrimary = true;
                    else if (sysCDef->isSupplementalLogAll())
                        suppLogTableAll = true;
                }
            }

            SysColSeg sysColSegFirst(sysObj->obj, 0);
            for (auto itCol = sysColMapSeg.upper_bound(sysColSegFirst);
                    itCol != sysColMapSeg.end() && itCol->first.obj == sysObj->obj; ++itCol) {

                SysCol* sysCol = itCol->second;
                if (sysCol->segCol == 0)
                    continue;

                uint64_t charmapId = 0;
                typeCOL numPk = 0;
                typeCOL numSup = 0;
                typeCOL guardSegNo = -1;

                SysEColKey sysEColKey(sysObj->obj, sysCol->segCol);
                SysECol* sysECol = sysEColMapKey[sysEColKey];
                if (sysECol != nullptr)
                    guardSegNo = sysECol->guardId;

                if (sysCol->charsetForm == 1)
                    charmapId = oracleAnalyzer->outputBuffer->defaultCharacterMapId;
                else if (sysCol->charsetForm == 2)
                    charmapId = oracleAnalyzer->outputBuffer->defaultCharacterNcharMapId;
                else
                    charmapId = sysCol->charsetId;

                //check character set for char and varchar2
                if (sysCol->type == 1 || sysCol->type == 96) {
                    auto it = oracleAnalyzer->outputBuffer->characterMap.find(charmapId);
                    if (it == oracleAnalyzer->outputBuffer->characterMap.end()) {
                        ERROR("HINT: check in database for name: SELECT NLS_CHARSET_NAME(" << std::dec << charmapId << ") FROM DUAL;");
                        RUNTIME_FAIL("table " << sysUser->name << "." << sysObj->name << " - unsupported character set id: " << std::dec << charmapId <<
                                " for column: " << sysObj->name << "." << sysCol->name);
                    }
                }

                SysCColKey sysCColKeyFirst(sysObj->obj, sysCol->intCol, 0);
                for (auto itCCol = sysCColMapKey.upper_bound(sysCColKeyFirst);
                        itCCol != sysCColMapKey.end() && itCCol->first.obj == sysObj->obj && itCCol->first.intCol == sysCol->intCol;
                        ++itCCol) {
                    SysCCol* sysCCol = itCCol->second;

                    //count number of PK the column is part of
                    SysCDef* sysCDef = sysCDefMapCon[sysCCol->con];
                    if (sysCDef == nullptr) {
                        DEBUG("SYS.CDEF$ missing for CON: " << sysCCol->con);
                        continue;
                    }
                    if (sysCDef->isPK())
                        ++numPk;

                    //supplemental logging
                    if (sysCCol->spare1.isZero() && sysCDef->isSupplementalLog())
                        ++numSup;
                }

                //part of defined primary key
                if (keys.size() > 0) {
                    //manually defined pk overlaps with table pk
                    if (numPk > 0 && (suppLogTablePrimary || sysUser->isSuppLogPrimary() || oracleAnalyzer->suppLogDbPrimary))
                        numSup = 1;
                    numPk = 0;
                    for (std::vector<std::string>::iterator it = keys.begin(); it != keys.end(); ++it) {
                        if (strcmp(sysCol->name.c_str(), it->c_str()) == 0) {
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

                DEBUG("  - col: " << std::dec << sysCol->segCol << ": " << sysCol->name << " (pk: " << std::dec << numPk << ", S: " << std::dec << numSup << ", G: " << std::dec << guardSegNo << ")");

                schemaColumn = new OracleColumn(sysCol->col, guardSegNo, sysCol->segCol, sysCol->name, sysCol->type,
                        sysCol->length, sysCol->precision, sysCol->scale, numPk, charmapId, (sysCol->null_ == 0), sysCol->isInvisible(),
                        sysCol->isStoredAsLob(), sysCol->isConstraint(), sysCol->isNested(), sysCol->isUnused(), sysCol->isAdded(),
                        sysCol->isGuard());
                if (schemaColumn == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(OracleColumn) << " bytes memory (for: column creation3)");
                }

                totalPk += numPk;
                if (sysCol->segCol > maxSegCol)
                    maxSegCol = sysCol->segCol;

                schemaObject->addColumn(schemaColumn);
                schemaColumn = nullptr;
            }

            //check if table has all listed columns
            if (keys.size() != keysCnt) {
                RUNTIME_FAIL("table " << sysUser->name << "." << sysObj->name << " couldn't find all column set (" << keysStr << ")");
            }

            std::stringstream ss;
            if (output)
                ss << "- found: ";
            else
                ss << "updated schema: ";
            ss << sysUser->name << "." << sysObj->name << " (dataobj: " << std::dec << sysTab->dataObj << ", obj: " << std::dec << sysObj->obj << ", columns: " << std::dec << maxSegCol << ")";
            if (sysTab->isClustered())
                ss << ", part of cluster";
            if (sysTab->isPartitioned())
                ss << ", partitioned(" << std::dec << partitions << ")";
            if (sysTab->isDependencies())
                ss << ", row dependencies";
            if (sysTab->isRowMovement())
                ss << ", row movement enabled";

            if ((oracleAnalyzer->disableChecks & DISABLE_CHECK_SUPPLEMENTAL_LOG) == 0 && (options & OPTIONS_SYSTEM_TABLE) == 0) {
                //use default primary key
                if (keys.size() == 0) {
                    if (totalPk == 0)
                        ss << ", primary key missing";
                    else if (!suppLogTablePrimary &&
                            !suppLogTableAll &&
                            !sysUser->isSuppLogPrimary() &&
                            !sysUser->isSuppLogAll() &&
                            !oracleAnalyzer->suppLogDbPrimary && !oracleAnalyzer->suppLogDbAll && supLogColMissing)
                        ss << ", supplemental log missing, try: ALTER TABLE " << sysUser->name << "." << sysObj->name << " ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;";
                //user defined primary key
                } else {
                    if (!suppLogTableAll &&
                            !sysUser->isSuppLogAll() &&
                            !oracleAnalyzer->suppLogDbAll &&
                            supLogColMissing)
                        ss << ", supplemental log missing, try: ALTER TABLE " << sysUser->name << "." << sysObj->name << " ADD SUPPLEMENTAL LOG GROUP GRP" << std::dec << sysObj->obj << " (" << keysStr << ") ALWAYS;";
                }
            }
            INFO(ss.str());

            schemaObject->maxSegCol = maxSegCol;
            schemaObject->totalPk = totalPk;
            schemaObject->updatePK();
            addToDict(schemaObject);
            schemaObject = nullptr;
        }
    }

    SchemaElement* Schema::addElement(const char* owner, const char* table, typeOPTIONS options) {
        if (!checkNameCase(owner)) {
            RUNTIME_FAIL("owner \"" << owner << "\" contains lower case characters, value must be upper case");
        }
        if (!checkNameCase(table)) {
            RUNTIME_FAIL("table \"" << table << "\" contains lower case characters, value must be upper case");
        }
        SchemaElement* element = new SchemaElement(owner, table, options);
        if (element == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SchemaElement) << " bytes memory (for: schema element)");
        }
        elements.push_back(element);
        return element;
    }

    bool Schema::dictSysCColAdd(const char* rowIdStr, typeCON con, typeCOL intCol, typeOBJ obj, uint64_t spare11, uint64_t spare12) {
        RowId rowId(rowIdStr);
        if (sysCColMapRowId.find(rowId) != sysCColMapRowId.end())
            return false;

        SysCCol* sysCCol = new SysCCol(rowId, con, intCol, obj, spare11, spare12, false);
        if (sysCCol == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SysCCol) << " bytes memory (for: SysCCol)");
        }
        sysCColMapRowId[rowId] = sysCCol;
        SysCColKey sysCColKey(obj, intCol, con);
        sysCColMapKey[sysCColKey] = sysCCol;

        return true;
    }

    bool Schema::dictSysCDefAdd(const char* rowIdStr, typeCON con, typeOBJ obj, typeTYPE type) {
        RowId rowId(rowIdStr);
        if (sysCDefMapRowId.find(rowId) != sysCDefMapRowId.end())
            return false;

        SysCDef* sysCDef = new SysCDef(rowId, con, obj, type, false);
        if (sysCDef == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SysCDef) << " bytes memory (for: SysCDef)");
        }
        sysCDefMapRowId[rowId] = sysCDef;
        sysCDefMapCon[con] = sysCDef;
        SysCDefKey sysCDefKey(obj, con);
        sysCDefMapKey[sysCDefKey] = sysCDef;

        return true;
    }

    bool Schema::dictSysColAdd(const char* rowIdStr, typeOBJ obj, typeCOL col, typeCOL segCol, typeCOL intCol, const char* name, typeTYPE type, uint64_t length,
            int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, bool null_, uint64_t property1, uint64_t property2) {
        RowId rowId(rowIdStr);
        if (sysColMapRowId.find(rowId) != sysColMapRowId.end())
            return false;

        if (strlen(name) > SYSCOL_NAME_LENGTH) {
            RUNTIME_FAIL("SYS.COL$ too long value for NAME (value: '" << name << "', length: " << std::dec << strlen(name) << ")");
        }
        SysCol* sysCol = new SysCol(rowId, obj, col, segCol, intCol, name, type, length, precision, scale, charsetForm, charsetId,
                null_, property1, property2, false);
        if (sysCol == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SysCol) << " bytes memory (for: SysCol)");
        }
        sysColMapRowId[rowId] = sysCol;
        SysColKey sysColKey(obj, intCol);
        sysColMapKey[sysColKey] = sysCol;
        SysColSeg sysColSeg(obj, segCol);
        sysColMapSeg[sysColSeg] = sysCol;

        return true;
    }

    bool Schema::dictSysDeferredStgAdd(const char* rowIdStr, typeOBJ obj, uint64_t flagsStg1, uint64_t flagsStg2) {
        RowId rowId(rowIdStr);
        if (sysDeferredStgMapRowId.find(rowId) != sysDeferredStgMapRowId.end())
            return false;

        SysDeferredStg* sysDeferredStg = new SysDeferredStg(rowId, obj, flagsStg1, flagsStg2, false);
        if (sysDeferredStg == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SysDeferredStg) << " bytes memory (for: SysDeferredStg)");
        }
        sysDeferredStgMapRowId[rowId] = sysDeferredStg;
        sysDeferredStgMapObj[obj] = sysDeferredStg;

        return true;
    }

    bool Schema::dictSysEColAdd(const char* rowIdStr, typeOBJ tabObj, typeCOL colNum, typeCOL guardId) {
        RowId rowId(rowIdStr);
        if (sysEColMapRowId.find(rowId) != sysEColMapRowId.end())
            return false;

        SysECol* sysECol = new SysECol(rowId, tabObj, colNum, guardId, false);
        if (sysECol == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SysECol) << " bytes memory (for: SysECol)");
        }
        sysEColMapRowId[rowId] = sysECol;
        SysEColKey sysEColKey(tabObj, colNum);
        sysEColMapKey[sysEColKey] = sysECol;

        return true;
    }

    bool Schema::dictSysObjAdd(const char* rowIdStr, typeUSER owner, typeOBJ obj, typeDATAOBJ dataObj, typeTYPE type, const char* name,
            uint64_t flags1, uint64_t flags2, bool single) {
        RowId rowId(rowIdStr);

        auto sysObjIt = sysObjMapRowId.find(rowId);
        if (sysObjIt != sysObjMapRowId.end()) {
            SysObj* sysObj = sysObjIt->second;
            if (!single && sysObj->single) {
                sysObj->single = false;
                TRACE(TRACE2_SYSTEM, "SYSTEM: disabling single option for object " << name << " (owner " << std::dec << owner << ")");
            }
            return false;
        }

        if (strlen(name) > SYSOBJ_NAME_LENGTH) {
            RUNTIME_FAIL("SYS.OBJ$ too long value for NAME (value: '" << name << "', length: " << std::dec << strlen(name) << ")");
        }
        SysObj* sysObj = new SysObj(rowId, owner, obj, dataObj, type, name, flags1, flags2, single, false);
        if (sysObj == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SysObj) << " bytes memory (for: SysObj)");
        }
        sysObjMapRowId[rowId] = sysObj;
        sysObjMapObj[obj] = sysObj;

        return true;
    }

    bool Schema::dictSysTabAdd(const char* rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeCOL cluCols, uint64_t flags1, uint64_t flags2,
            uint64_t property1, uint64_t property2) {
        RowId rowId(rowIdStr);
        if (sysTabMapRowId.find(rowId) != sysTabMapRowId.end())
            return false;

        SysTab* sysTab = new SysTab(rowId, obj, dataObj, cluCols, flags1, flags2, property1, property2, false);
        if (sysTab == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SysTab) << " bytes memory (for: SysTab)");
        }
        sysTabMapRowId[rowId] = sysTab;
        sysTabMapObj[obj] = sysTab;

        return true;
    }

    bool Schema::dictSysTabComPartAdd(const char* rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo) {
        RowId rowId(rowIdStr);
        if (sysTabComPartMapRowId.find(rowId) != sysTabComPartMapRowId.end())
            return false;

        SysTabComPart* sysTabComPart = new SysTabComPart(rowId, obj, dataObj, bo, false);
        if (sysTabComPart == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SysTabComPart) << " bytes memory (for: SysTabComPart)");
        }
        sysTabComPartMapRowId[rowId] = sysTabComPart;
        SysTabComPartKey sysTabComPartKey(bo, obj);
        sysTabComPartMapKey[sysTabComPartKey] = sysTabComPart;
        sysTabComPartMapObj[sysTabComPart->obj] = sysTabComPart;

        return true;
    }

    bool Schema::dictSysTabPartAdd(const char* rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo) {
        RowId rowId(rowIdStr);
        if (sysTabPartMapRowId.find(rowId) != sysTabPartMapRowId.end())
            return false;

        SysTabPart* sysTabPart = new SysTabPart(rowId, obj, dataObj, bo, false);
        if (sysTabPart == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SysTabPart) << " bytes memory (for: SysTabPart)");
        }
        sysTabPartMapRowId[rowId] = sysTabPart;
        SysTabPartKey sysTabPartKey(bo, obj);
        sysTabPartMapKey[sysTabPartKey] = sysTabPart;

        return true;
    }

    bool Schema::dictSysTabSubPartAdd(const char* rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ pObj) {
        RowId rowId(rowIdStr);
        if (sysTabSubPartMapRowId.find(rowId) != sysTabSubPartMapRowId.end())
            return false;

        SysTabSubPart* sysTabSubPart = new SysTabSubPart(rowId, obj, dataObj, pObj, false);
        if (sysTabSubPart == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SysTabSubPart) << " bytes memory (for: SysTabSubPart)");
        }
        sysTabSubPartMapRowId[rowId] = sysTabSubPart;
        SysTabSubPartKey sysTabSubPartKey(pObj, obj);
        sysTabSubPartMapKey[sysTabSubPartKey] = sysTabSubPart;

        return true;
    }

    bool Schema::dictSysUserAdd(const char* rowIdStr, typeUSER user, const char* name, uint64_t spare11, uint64_t spare12, bool single) {
        RowId rowId(rowIdStr);

        auto sysUserIt = sysUserMapRowId.find(rowId);
        if (sysUserIt != sysUserMapRowId.end()) {
            SysUser* sysUser = sysUserIt->second;
            if (sysUser->single) {
                if (!single) {
                    sysUser->single = false;
                    TRACE(TRACE2_SYSTEM, "SYSTEM: disabling single option for user " << name << " (" << std::dec << user << ")");
                }
                return true;
            }

            return false;
        }

        if (strlen(name) > SYSUSER_NAME_LENGTH) {
            RUNTIME_FAIL("SYS.USER$ too long value for NAME (value: '" << name << "', length: " << std::dec << strlen(name) << ")");
        }
        SysUser* sysUser = new SysUser(rowId, user, name, spare11, spare12, single, false);
        if (sysUser == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << std::dec << sizeof(class SysUser) << " bytes memory (for: SysUser)");
        }
        sysUserMapRowId[rowId] = sysUser;
        sysUserMapUser[user] = sysUser;

        return true;
    }

    void Schema::touchObj(typeOBJ obj) {
        if (obj == 0)
            return;

        if (objectsTouched.find(obj) == objectsTouched.end())
            objectsTouched.insert(obj);
    }

    void Schema::touchPart(typeOBJ obj) {
        if (obj == 0)
            return;

        if (partitionsTouched.find(obj) == partitionsTouched.end())
            partitionsTouched.insert(obj);
    }

    void Schema::touchUser(typeUSER user) {
        if (user == 0)
            return;

        if (usersTouched.find(user) == usersTouched.end())
            usersTouched.insert(user);
    }

    bool Schema::checkNameCase(const char* name) {
        uint64_t num = 0;
        while (*(name + num) != 0) {
            if (islower((unsigned char)*(name + num)))
                return false;

            if (num == 1024) {
                RUNTIME_FAIL("\"" << name << "\" is too long");
            }
            ++num;
        }

        return true;
    }
}
