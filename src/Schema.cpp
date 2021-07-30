/* Base class for handling of schema
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

#include <dirent.h>
#include <errno.h>
#include <list>
#include <regex>
#include <sys/stat.h>
#include <unistd.h>

#include "global.h"
#include "ConfigurationException.h"
#include "OracleAnalyzer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OutputBuffer.h"
#include "Reader.h"
#include "RowId.h"
#include "RuntimeException.h"
#include "Schema.h"
#include "SchemaElement.h"

using namespace rapidjson;
using namespace std;

namespace OpenLogReplicator {
    Schema::Schema(OracleAnalyzer* oracleAnalyzer) :
        oracleAnalyzer(oracleAnalyzer),
        schemaObject(nullptr),
        sysCColTouched(false),
        sysCDefTouched(false),
        sysColTouched(false),
        sysDeferredStgTouched(false),
        sysEColTouched(false),
        sysObjTouched(false),
        sysSegTouched(false),
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

        for (auto it : sysSegMapRowId) {
            SysSeg* sysSeg = it.second;
            delete sysSeg;
        }
        sysSegMapRowId.clear();
        sysSegMapKey.clear();

        for (auto it : sysTabMapRowId) {
            SysTab* sysTab = it.second;
            delete sysTab;
        }
        sysTabMapRowId.clear();
        sysTabMapObj.clear();
        sysTabMapKey.clear();

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
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: searching for previous schema on: " << oracleAnalyzer->checkpointPath);
        DIR* dir;
        if ((dir = opendir(oracleAnalyzer->checkpointPath.c_str())) == nullptr) {
            RUNTIME_FAIL("can't access directory: " << oracleAnalyzer->checkpointPath);
        }

        string newLastCheckedDay;
        struct dirent* ent;
        typeSCN fileScnMax = ZERO_SCN;
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;

            struct stat fileStat;
            string fileName(ent->d_name);

            string fullName(oracleAnalyzer->checkpointPath + "/" + ent->d_name);
            if (stat(fullName.c_str(), &fileStat)) {
                WARNING("reading information for file: " << fullName << " - " << strerror(errno));
                continue;
            }

            if (S_ISDIR(fileStat.st_mode))
                continue;

            string prefix(oracleAnalyzer->database + "-schema-");
            if (fileName.length() < prefix.length() || fileName.substr(0, prefix.length()).compare(prefix) != 0)
                continue;

            string suffix(".json");
            if (fileName.length() < suffix.length() || fileName.substr(fileName.length() - suffix.length()).compare(suffix) != 0)
                continue;

            TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: found previous schema: " << oracleAnalyzer->checkpointPath << "/" << fileName);
            string fileScnStr(fileName.substr(prefix.length(), fileName.length() - suffix.length()));
            typeSCN fileScn;
            try {
                fileScn = strtoull(fileScnStr.c_str(), nullptr, 10);
            } catch (exception& e) {
                //ignore other files
                continue;
            }
            if (fileScn <= oracleAnalyzer->firstScn && (fileScn > fileScnMax || fileScnMax == ZERO_SCN))
                fileScnMax = fileScn;
            if (oracleAnalyzer->schemaFirstScn == ZERO_SCN || oracleAnalyzer->schemaFirstScn > fileScn)
                oracleAnalyzer->schemaFirstScn = fileScn;
            schemaScnList.insert(fileScn);
        }
        closedir(dir);

        //none found
        if (fileScnMax == ZERO_SCN)
            return false;

        bool unlinkFile = false, firstFound = false;
        set<typeSCN>::iterator it = schemaScnList.end();

        while (it != schemaScnList.begin()) {
            --it;
            string fileName(oracleAnalyzer->checkpointPath + "/" + oracleAnalyzer->database + "-schema-" + to_string(*it) + ".json");

            unlinkFile = false;
            if (*it > oracleAnalyzer->firstScn && oracleAnalyzer->firstScn != ZERO_SCN) {
                unlinkFile = true;
            } else {
                if (readSchemaFile(fileName, *it))
                    unlinkFile = true;
            }

            if (unlinkFile) {
                if ((oracleAnalyzer->flags & REDO_FLAGS_CHECKPOINT_KEEP) == 0) {
                    TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: delete file: " << fileName << " scn: " << dec << *it);
                    unlink(fileName.c_str());
                }
                it = schemaScnList.erase(it);
            }
        }

        return true;
    }

    bool Schema::readSchemaFile(string& fileName, typeSCN fileScn) {
        if (oracleAnalyzer->schemaScn != ZERO_SCN)
            return true;
        dropSchema();

        ifstream infile;
        infile.open(fileName.c_str(), ios::in);

        if (!infile.is_open()) {
            WARNING("error reading " << fileName);
            return false;
        }
        INFO("reading schema for " << oracleAnalyzer->database << " for scn: " << fileScn);

        string schemaJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (schemaJSON.length() == 0 || document.Parse(schemaJSON.c_str()).HasParseError()) {
            WARNING("parsing " << fileName << " at offset: " << document.GetErrorOffset() << ", message: " << GetParseError_En(document.GetParseError()));
            return false;
        }

        //SYS.USER$
        const Value& sysUserJSON = getJSONfieldA(fileName, document, "sys-user");

        for (SizeType i = 0; i < sysUserJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysUserJSON[i], "row-id");
            typeUSER user = getJSONfieldU(fileName, sysUserJSON[i], "user");
            const char* name = getJSONfieldS(fileName, sysUserJSON[i], "name");

            const Value& spare1JSON = getJSONfieldA(fileName, sysUserJSON[i], "spare1");
            if (spare1JSON.Size() != 2) {
                WARNING("bad JSON in " << fileName << ", spare1 should be an array with 2 elements");
                return false;
            }
            uint64_t spare11 = getJSONfieldU(fileName, spare1JSON, "spare1", 0);
            uint64_t spare12 = getJSONfieldU(fileName, spare1JSON, "spare1", 1);
            uint64_t single = getJSONfieldU(fileName, sysUserJSON[i], "single");

            dictSysUserAdd(rowId, user, name, spare11, spare12, single);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.USER$: " << dec << sysUserJSON.Size());

        //SYS.OBJ$
        const Value& sysObjJSON = getJSONfieldA(fileName, document, "sys-obj");

        for (SizeType i = 0; i < sysObjJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysObjJSON[i], "row-id");
            typeUSER owner = getJSONfieldU(fileName, sysObjJSON[i], "owner");
            typeOBJ obj = getJSONfieldU(fileName, sysObjJSON[i], "obj");
            typeDATAOBJ dataObj = getJSONfieldU(fileName, sysObjJSON[i], "data-obj");
            typeTYPE type = getJSONfieldU(fileName, sysObjJSON[i], "type");
            const char* name = getJSONfieldS(fileName, sysObjJSON[i], "name");

            const Value& flagsJSON = getJSONfieldA(fileName, sysObjJSON[i], "flags");
            if (flagsJSON.Size() != 2) {
                WARNING("bad JSON in " << fileName << ", flags should be an array with 2 elements");
                return false;
            }
            uint64_t flags1 = getJSONfieldU(fileName, flagsJSON, "flags", 0);
            uint64_t flags2 = getJSONfieldU(fileName, flagsJSON, "flags", 1);
            uint64_t single = getJSONfieldU(fileName, sysObjJSON[i], "single");

            dictSysObjAdd(rowId, owner, obj, dataObj, type, name, flags1, flags2, single);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.OBJ$: " << dec << sysObjJSON.Size());

        //SYS.COL$
        const Value& sysColJSON = getJSONfieldA(fileName, document, "sys-col");

        for (SizeType i = 0; i < sysColJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysColJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU(fileName, sysColJSON[i], "obj");
            typeCOL col = getJSONfieldI(fileName, sysColJSON[i], "col");
            typeCOL segCol = getJSONfieldI(fileName, sysColJSON[i], "seg-col");
            typeCOL intCol = getJSONfieldI(fileName, sysColJSON[i], "int-col");
            const char* name = getJSONfieldS(fileName, sysColJSON[i], "name");
            typeTYPE type = getJSONfieldU(fileName, sysColJSON[i], "type");
            uint64_t length = getJSONfieldU(fileName, sysColJSON[i], "length");
            int64_t precision = getJSONfieldI(fileName, sysColJSON[i], "precision");
            int64_t scale = getJSONfieldI(fileName, sysColJSON[i], "scale");
            uint64_t charsetForm = getJSONfieldU(fileName, sysColJSON[i], "charset-form");
            uint64_t charsetId = getJSONfieldU(fileName, sysColJSON[i], "charset-id");
            int64_t null_ = getJSONfieldI(fileName, sysColJSON[i], "null");
            const Value& propertyJSON = getJSONfieldA(fileName, sysColJSON[i], "property");
            if (propertyJSON.Size() != 2) {
                WARNING("bad JSON in " << fileName << ", property should be an array with 2 elements");
                return false;
            }
            uint64_t property1 = getJSONfieldU(fileName, propertyJSON, "property", 0);
            uint64_t property2 = getJSONfieldU(fileName, propertyJSON, "property", 1);

            dictSysColAdd(rowId, obj, col, segCol, intCol, name, type, length, precision, scale, charsetForm, charsetId, null_, property1, property2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.COL$: " << dec << sysColJSON.Size());

        //SYS.CCOL$
        const Value& sysCColJSON = getJSONfieldA(fileName, document, "sys-ccol");

        for (SizeType i = 0; i < sysCColJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysCColJSON[i], "row-id");
            typeCON con = getJSONfieldI(fileName, sysCColJSON[i], "con");
            typeCON intCol = getJSONfieldI(fileName, sysCColJSON[i], "int-col");
            typeOBJ obj = getJSONfieldU(fileName, sysCColJSON[i], "obj");
            const Value& spare1JSON = getJSONfieldA(fileName, sysCColJSON[i], "spare1");
            if (spare1JSON.Size() != 2) {
                WARNING("bad JSON in " << fileName << ", spare1 should be an array with 2 elements");
                return false;
            }
            uint64_t spare11 = getJSONfieldU(fileName, spare1JSON, "spare1", 0);
            uint64_t spare12 = getJSONfieldU(fileName, spare1JSON, "spare1", 1);

            dictSysCColAdd(rowId, con, intCol, obj, spare11, spare12);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.CCOL$: " << dec << sysCColJSON.Size());

        //SYS.CDEF$
        const Value& sysCDefJSON = getJSONfieldA(fileName, document, "sys-cdef");

        for (SizeType i = 0; i < sysCDefJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysCDefJSON[i], "row-id");
            typeCON con = getJSONfieldI(fileName, sysCDefJSON[i], "con");
            typeOBJ obj = getJSONfieldU(fileName, sysCDefJSON[i], "obj");
            typeTYPE type = getJSONfieldU(fileName, sysCDefJSON[i], "type");

            dictSysCDefAdd(rowId, con, obj, type);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.CDEF$: " << dec << sysCDefJSON.Size());

        //SYS.DEFERRED_STG$
        const Value& sysDeferredStgJSON = getJSONfieldA(fileName, document, "sys-deferredstg");

        for (SizeType i = 0; i < sysDeferredStgJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysDeferredStgJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU(fileName, sysDeferredStgJSON[i], "obj");

            const Value& flagsStgJSON = getJSONfieldA(fileName, sysDeferredStgJSON[i], "flags-stg");
            if (flagsStgJSON.Size() != 2) {
                WARNING("bad JSON in " << fileName << ", flags-stg should be an array with 2 elements");
                return false;
            }
            uint64_t flagsStg1 = getJSONfieldU(fileName, flagsStgJSON, "flags-stg", 0);
            uint64_t flagsStg2 = getJSONfieldU(fileName, flagsStgJSON, "flags-stg", 1);

            dictSysDeferredStgAdd(rowId, obj, flagsStg1, flagsStg2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.DEFERRED_STG$: " << dec << sysDeferredStgJSON.Size());

        //SYS.ECOL$
        const Value& sysEColJSON = getJSONfieldA(fileName, document, "sys-ecol");

        for (SizeType i = 0; i < sysEColJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysEColJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU(fileName, sysEColJSON[i], "tab-obj");
            typeCOL colNum = getJSONfieldI(fileName, sysEColJSON[i], "col-num");
            typeCOL guardId = getJSONfieldI(fileName, sysEColJSON[i], "guard-id");

            dictSysEColAdd(rowId, obj, colNum, guardId);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.ECOL$: " << dec << sysEColJSON.Size());

        //SYS.SEG$
        const Value& sysSegJSON = getJSONfieldA(fileName, document, "sys-seg");

        for (SizeType i = 0; i < sysSegJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysSegJSON[i], "row-id");
            uint32_t file = getJSONfieldU(fileName, sysSegJSON[i], "file");
            uint32_t block = getJSONfieldU(fileName, sysSegJSON[i], "block");
            uint32_t ts = getJSONfieldU(fileName, sysSegJSON[i], "ts");

            const Value& spare1JSON = getJSONfieldA(fileName, sysSegJSON[i], "spare1");
            if (spare1JSON.Size() != 2) {
                WARNING("bad JSON in " << fileName << ", spare1 should be an array with 2 elements");
                return false;
            }
            uint64_t spare11 = getJSONfieldU(fileName, spare1JSON, "spare1", 0);
            uint64_t spare12 = getJSONfieldU(fileName, spare1JSON, "spare1", 1);

            dictSysSegAdd(rowId, file, block, ts, spare11, spare12);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.SEG$: " << dec << sysSegJSON.Size());

        //SYS.TAB$
        const Value& sysTabJSON = getJSONfieldA(fileName, document, "sys-tab");

        for (SizeType i = 0; i < sysTabJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysTabJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU(fileName, sysTabJSON[i], "obj");
            typeDATAOBJ dataObj = getJSONfieldU(fileName, sysTabJSON[i], "data-obj");
            uint32_t ts = getJSONfieldU(fileName, sysTabJSON[i], "ts");
            uint32_t file = getJSONfieldU(fileName, sysTabJSON[i], "file");
            uint32_t block = getJSONfieldU(fileName, sysTabJSON[i], "block");
            typeCOL cluCols = getJSONfieldI(fileName, sysTabJSON[i], "clu-cols");

            const Value& flagsJSON = getJSONfieldA(fileName, sysTabJSON[i], "flags");
            if (flagsJSON.Size() != 2) {
                WARNING("bad JSON in " << fileName << ", flags should be an array with 2 elements");
                return false;
            }
            uint64_t flags1 = getJSONfieldU(fileName, flagsJSON, "flags", 0);
            uint64_t flags2 = getJSONfieldU(fileName, flagsJSON, "flags", 1);

            const Value& propertyJSON = getJSONfieldA(fileName, sysTabJSON[i], "property");
            if (propertyJSON.Size() != 2) {
                WARNING("bad JSON in " << fileName << ", property should be an array with 2 elements");
                return false;
            }
            uint64_t property1 = getJSONfieldU(fileName, propertyJSON, "property", 0);
            uint64_t property2 = getJSONfieldU(fileName, propertyJSON, "property", 1);

            dictSysTabAdd(rowId, obj, dataObj, ts, file, block, cluCols, flags1, flags2, property1, property2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TAB$: " << dec << sysTabJSON.Size());

        //SYS.TABPART$
        const Value& sysTabPartJSON = getJSONfieldA(fileName, document, "sys-tabpart");

        for (SizeType i = 0; i < sysTabPartJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysTabPartJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU(fileName, sysTabPartJSON[i], "obj");
            typeDATAOBJ dataObj = getJSONfieldU(fileName, sysTabPartJSON[i], "data-obj");
            typeOBJ bo = getJSONfieldU(fileName, sysTabPartJSON[i], "bo");

            dictSysTabPartAdd(rowId, obj, dataObj, bo);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABPART$: " << dec << sysTabPartJSON.Size());

        //SYS.TABCOMPART$
        const Value& sysTabComPartJSON = getJSONfieldA(fileName, document, "sys-tabcompart");

        for (SizeType i = 0; i < sysTabComPartJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysTabComPartJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU(fileName, sysTabComPartJSON[i], "obj");
            typeDATAOBJ dataObj = getJSONfieldU(fileName, sysTabComPartJSON[i], "data-obj");
            typeOBJ bo = getJSONfieldU(fileName, sysTabComPartJSON[i], "bo");

            dictSysTabComPartAdd(rowId, obj, dataObj, bo);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABCOMPART$: " << dec << sysTabComPartJSON.Size());

        //SYS.TABSUBPART$
        const Value& sysTabSubPartJSON = getJSONfieldA(fileName, document, "sys-tabsubpart");

        for (SizeType i = 0; i < sysTabSubPartJSON.Size(); ++i) {
            const char* rowId = getJSONfieldS(fileName, sysTabSubPartJSON[i], "row-id");
            typeOBJ obj = getJSONfieldU(fileName, sysTabSubPartJSON[i], "obj");
            typeDATAOBJ dataObj = getJSONfieldU(fileName, sysTabSubPartJSON[i], "data-obj");
            typeOBJ pObj = getJSONfieldU(fileName, sysTabSubPartJSON[i], "p-obj");

            dictSysTabSubPartAdd(rowId, obj, dataObj, pObj);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABSUBPART$: " << dec << sysTabSubPartJSON.Size());

        //database metadata
        const char* databaseRead = getJSONfieldS(fileName, document, "database");
        if (oracleAnalyzer->database.compare(databaseRead) != 0) {
            WARNING("invalid database for " << fileName << " - " << databaseRead << " instead of " << oracleAnalyzer->database << " - skipping file");
            return false;
        }

        bool bigEndian = getJSONfieldU(fileName, document, "big-endian");
        if (bigEndian)
            oracleAnalyzer->setBigEndian();

        typeRESETLOGS resetlogsRead = getJSONfieldU(fileName, document, "resetlogs");
        if (oracleAnalyzer->resetlogs == 0)
            oracleAnalyzer->resetlogs = resetlogsRead;

        if (oracleAnalyzer->resetlogs != resetlogsRead) {
            WARNING("invalid resetlogs for " << fileName << " - " << dec << resetlogsRead << " instead of " << oracleAnalyzer->resetlogs << " - skipping file");
            return false;
        }

        typeACTIVATION activationRead = getJSONfieldU(fileName, document, "activation");
        if (oracleAnalyzer->activation == 0)
            oracleAnalyzer->activation = activationRead;

        if (oracleAnalyzer->activation != activationRead) {
            WARNING("invalid activation for " << fileName << " - " << dec << activationRead << " instead of " << oracleAnalyzer->activation << " - skipping file");
            return false;
        }

        string contextRead(getJSONfieldS(fileName, document, "context"));
        if (oracleAnalyzer->context.length() == 0)
            oracleAnalyzer->context = contextRead;
        else if (oracleAnalyzer->context.compare(contextRead) != 0) {
            WARNING("invalid context for " << fileName << " - " << dec << contextRead << " instead of " << oracleAnalyzer->context << " - skipping file");
            return false;
        }

        typeCONID conIdRead = getJSONfieldI(fileName, document, "con-id");
        if (oracleAnalyzer->conId == -1)
            oracleAnalyzer->conId = conIdRead;
        else if (oracleAnalyzer->conId != conIdRead) {
            WARNING("invalid con_id for " << fileName << " - " << dec << conIdRead << " instead of " << oracleAnalyzer->conId << " - skipping file");
            return false;
        }

        oracleAnalyzer->conName = getJSONfieldS(fileName, document, "con-name");
        oracleAnalyzer->dbRecoveryFileDest = getJSONfieldS(fileName, document, "db-recovery-file-dest");
        oracleAnalyzer->dbBlockChecksum = getJSONfieldS(fileName, document, "db-block-checksum");

        if (oracleAnalyzer->logArchiveFormat.length() == 0)
            oracleAnalyzer->logArchiveFormat = getJSONfieldS(fileName, document, "log-archive-format");

        oracleAnalyzer->logArchiveDest = getJSONfieldS(fileName, document, "log-archive-dest");
        oracleAnalyzer->nlsCharacterSet = getJSONfieldS(fileName, document, "nls-character-set");
        oracleAnalyzer->nlsNcharCharacterSet = getJSONfieldS(fileName, document, "nls-nchar-character-set");

        oracleAnalyzer->outputBuffer->setNlsCharset(oracleAnalyzer->nlsCharacterSet, oracleAnalyzer->nlsNcharCharacterSet);

        const Value& onlineRedoJSON = getJSONfieldA(fileName, document, "online-redo");

        for (SizeType i = 0; i < onlineRedoJSON.Size(); ++i) {
            uint64_t group = getJSONfieldI(fileName, onlineRedoJSON[i], "group");

            const Value& path = onlineRedoJSON[i]["path"];
            if (!path.IsArray()) {
                RUNTIME_FAIL("bad JSON, path-mapping should be array");
            }

            Reader* onlineReader = oracleAnalyzer->readerCreate(group);
            if (onlineReader != nullptr) {
                for (SizeType j = 0; j < path.Size(); ++j) {
                    const Value& pathVal = path[j];
                    onlineReader->paths.push_back(pathVal.GetString());
                }
            }
        }

        if ((oracleAnalyzer->flags & REDO_FLAGS_ARCH_ONLY) == 0)
            oracleAnalyzer->checkOnlineRedoLogs();
        oracleAnalyzer->archReader = oracleAnalyzer->readerCreate(0);

        const Value& usersJSON = getJSONfieldA(fileName, document, "users");

        for (SizeType i = 0; i < usersJSON.Size(); ++i) {
            const Value& userJSON = usersJSON[i];;
            users.insert(userJSON.GetString());
        }

        infile.close();

        //rebuild object structures
        for (SchemaElement* element : elements) {
            DEBUG("- creating table schema for owner: " << element->owner << " table: " << element->table << " options: " <<
                    (uint64_t) element->options);

            if ((element->options & OPTIONS_SCHEMA_TABLE) == 0 && users.find(element->owner) == users.end()) {
                RUNTIME_FAIL("owner \"" << element->owner << "\" is missing in schema file: " <<
                        fileName << " - recreate schema file (delete old file and force creation of new)");
            }
            buildMaps(element->owner, element->table, element->keys, element->keysStr, element->options, true);
        }
        oracleAnalyzer->schemaScn = fileScn;

        return false;
    }

    void Schema::writeSchema(void) {
        if (oracleAnalyzer->schemaScn == ZERO_SCN && (oracleAnalyzer->flags & REDO_FLAGS_SCHEMALESS) != 0)
            return;

        string fileName(oracleAnalyzer->checkpointPath + "/" + oracleAnalyzer->database + "-schema-" + to_string(oracleAnalyzer->schemaScn) + ".json");
        TRACE(TRACE2_SYSTEM, "SYSTEM: writing file: " << fileName << " scn: " << dec << oracleAnalyzer->schemaScn);
        ofstream outfile;
        outfile.open(fileName.c_str(), ios::out | ios::trunc);

        if (!outfile.is_open()) {
            RUNTIME_FAIL("writing schema data");
        }

        stringstream ss;
        ss << "{\"database\":\"" << oracleAnalyzer->database << "\"," <<
                "\"big-endian\":" << dec << oracleAnalyzer->bigEndian << "," <<
                "\"resetlogs\":" << dec << oracleAnalyzer->resetlogs << "," <<
                "\"activation\":" << dec << oracleAnalyzer->activation << "," <<
                "\"context\":\"" << oracleAnalyzer->context << "\"," <<
                "\"con-id\":" << dec << oracleAnalyzer->conId << "," <<
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

        bool hasPrev = false, hasPrev2;
        for (Reader* reader : oracleAnalyzer->readers) {
            if (reader->group == 0)
                continue;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            hasPrev2 = false;
            ss SCHEMA_ENDL << "{\"group\":" << reader->group << ",\"path\":[";
            for (string& path : reader->paths) {
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

        ss << "]," SCHEMA_ENDL << "\"users\":[";
        hasPrev = false;
        for (string user : users) {
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
                    "\"con\":" << dec << sysCCol->con << "," <<
                    "\"int-col\":" << dec << sysCCol->intCol << "," <<
                    "\"obj\":" << dec << sysCCol->obj << "," <<
                    "\"spare1\":" << dec << sysCCol->spare1 << "}";
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
                    "\"con\":" << dec << sysCDef->con << "," <<
                    "\"obj\":" << dec << sysCDef->obj << "," <<
                    "\"type\":" << dec << sysCDef->type << "}";
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
                    "\"obj\":" << dec << sysCol->obj << "," <<
                    "\"col\":" << dec << sysCol->col << "," <<
                    "\"seg-col\":" << dec << sysCol->segCol << "," <<
                    "\"int-col\":" << dec << sysCol->intCol << "," <<
                    "\"name\":\"" << sysCol->name << "\"," <<
                    "\"type\":" << dec << sysCol->type << "," <<
                    "\"length\":" << dec << sysCol->length << "," <<
                    "\"precision\":" << dec << sysCol->precision << "," <<
                    "\"scale\":" << dec << sysCol->scale << "," <<
                    "\"charset-form\":" << dec << sysCol->charsetForm << "," <<
                    "\"charset-id\":" << dec << sysCol->charsetId << "," <<
                    "\"null\":" << dec << sysCol->null_ << "," <<
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
                    "\"obj\":" << dec << sysDeferredStg->obj << "," <<
                    "\"flags-stg\":" << dec << sysDeferredStg->flagsStg << "}";
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
                    "\"tab-obj\":" << dec << sysECol->tabObj << "," <<
                    "\"col-num\":" << dec << sysECol->colNum << "," <<
                    "\"guard-id\":" << dec << sysECol->guardId << "}";
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
                    "\"owner\":" << dec << sysObj->owner << "," <<
                    "\"obj\":" << dec << sysObj->obj << "," <<
                    "\"data-obj\":" << dec << sysObj->dataObj << "," <<
                    "\"name\":\"" << sysObj->name << "\"," <<
                    "\"type\":" << dec << sysObj->type << "," <<
                    "\"flags\":" << dec << sysObj->flags << "," <<
                    "\"single\":" << dec << (uint64_t)sysObj->single <<"}";
            sysObj->saved = true;
        }

        //SYS.SEG$
        ss << "]," SCHEMA_ENDL << "\"sys-seg\":[";
        hasPrev = false;
        for (auto it : sysSegMapRowId) {
            SysSeg* sysSeg = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss SCHEMA_ENDL << "{\"row-id\":\"" << sysSeg->rowId << "\"," <<
                    "\"file\":" << dec << sysSeg->file << "," <<
                    "\"block\":" << dec << sysSeg->block << "," <<
                    "\"ts\":" << dec << sysSeg->ts << "," <<
                    "\"spare1\":" << dec << sysSeg->spare1 << "}";
            sysSeg->saved = true;
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
                    "\"obj\":" << dec << sysTab->obj << "," <<
                    "\"data-obj\":" << dec << sysTab->dataObj << "," <<
                    "\"ts\":" << dec << sysTab->ts << "," <<
                    "\"file\":" << dec << sysTab->file << "," <<
                    "\"block\":" << dec << sysTab->block << "," <<
                    "\"clu-cols\":" << dec << sysTab->cluCols << "," <<
                    "\"flags\":" << dec << sysTab->flags << "," <<
                    "\"property\":" << dec << sysTab->property << "}";
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
                    "\"obj\":" << dec << sysTabComPart->obj << "," <<
                    "\"data-obj\":" << dec << sysTabComPart->dataObj << "," <<
                    "\"bo\":" << dec << sysTabComPart->bo << "}";
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
                    "\"obj\":" << dec << sysTabPart->obj << "," <<
                    "\"data-obj\":" << dec << sysTabPart->dataObj << "," <<
                    "\"bo\":" << dec << sysTabPart->bo << "}";
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
                    "\"obj\":" << dec << sysTabSubPart->obj << "," <<
                    "\"data-obj\":" << dec << sysTabSubPart->dataObj << "," <<
                    "\"p-obj\":" << dec << sysTabSubPart->pObj << "}";
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
                    "\"user\":" << dec << sysUser->user << "," <<
                    "\"name\":\"" << sysUser->name << "\"," <<
                    "\"spare1\":" << dec << sysUser->spare1 << "," <<
                    "\"single\":" << dec << (uint64_t)sysUser->single << "}";
            sysUser->saved = true;
        }

        ss << "]}";
        outfile << ss.rdbuf();

        outfile.close();
        savedDeleted = false;

        schemaScnList.insert(oracleAnalyzer->schemaScn);
        if (oracleAnalyzer->checkpointScn != ZERO_SCN) {
            bool unlinkFile = false, firstFound = false;
            set<typeSCN>::iterator it = schemaScnList.end();

            while (it != schemaScnList.begin()) {
                --it;
                string fileName(oracleAnalyzer->checkpointPath + "/" + oracleAnalyzer->database + "-schema-" + to_string(*it) + ".json");

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
                        TRACE(TRACE2_SYSTEM, "SYSTEM: delete file: " << fileName << " schema scn: " << dec << *it);
                        unlink(fileName.c_str());
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
            CONFIG_FAIL("can't add object (obj: " << dec << object->obj << ", dataObj: " << object->dataObj << ")");
        }
        objectMap[object->obj] = object;

        if (partitionMap.find(object->obj) != partitionMap.end()) {
            CONFIG_FAIL("can't add partition (obj: " << dec << object->obj << ", dataObj: " << object->dataObj << ")");
        }
        partitionMap[object->obj] = object;

        for (typeOBJ2 objx : object->partitions) {
            typeOBJ partitionObj = objx >> 32;
            typeDATAOBJ partitionDataObj = objx & 0xFFFFFFFF;

            if (partitionMap.find(partitionObj) != partitionMap.end()) {
                CONFIG_FAIL("can't add partition element (obj: " << dec << partitionObj << ", dataObj: " << partitionDataObj << ")");
            }
            partitionMap[partitionObj] = object;
        }
    }

    void Schema::removeFromDict(OracleObject* object) {
        if (objectMap.find(object->obj) == objectMap.end()) {
            CONFIG_FAIL("can't remove object (obj: " << dec << object->obj << ", dataObj: " << object->dataObj << ")");
        }
        objectMap.erase(object->obj);

        if (partitionMap.find(object->obj) == partitionMap.end()) {
            CONFIG_FAIL("can't remove partition (obj: " << dec << object->obj << ", dataObj: " << object->dataObj << ")");
        }
        partitionMap.erase(object->obj);

        for (typeOBJ2 objx : object->partitions) {
            typeOBJ partitionObj = objx >> 32;
            typeDATAOBJ partitionDataObj = objx & 0xFFFFFFFF;

            if (partitionMap.find(partitionObj) == partitionMap.end()) {
                CONFIG_FAIL("can't remove partition element (obj: " << dec << partitionObj << ", dataObj: " << partitionDataObj << ")");
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

    stringstream& Schema::writeEscapeValue(stringstream& ss, string& str) {
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

        list<RowId> removeRowId;
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

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage USER$ (rowid: " << it.first << ", USER#: " << dec << sysUser->user << ", NAME: " <<
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

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage OBJ$ (rowid: " << it.first << ", OWNER#: " << dec << sysObj->owner << ", OBJ#: " <<
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

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage CCOL$ (rowid: " << it.first << ", CON#: " << dec << sysCCol->con << ", INTCOL#: " <<
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

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage CDEF$ (rowid: " << it.first << ", CON#: " << dec << sysCDef->con << ", OBJ#: " <<
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

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage COL$ (rowid: " << it.first << ", OBJ#: " << dec << sysCol->obj << ", COL#: " << sysCol->col <<
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

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage DEFERRED_STG$ (rowid: " << it.first << ", OBJ#: " << dec << sysDeferredStg->obj <<
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

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage ECOL$ (rowid: " << it.first << ", TABOBJ#: " << dec << sysECol->tabObj << ", COLNUM: " <<
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
            sysTabMapKey.clear();

            for (auto it : sysTabMapRowId) {
                SysTab* sysTab = it.second;

                if (sysObjMapObj.find(sysTab->obj) != sysObjMapObj.end()) {
                    sysTabMapObj[sysTab->obj] = sysTab;
                    if (sysTab->file != 0 || sysTab->block != 0) {
                        SysTabKey sysTabKey(sysTab->file, sysTab->block, sysTab->ts);
                        sysTabMapKey[sysTabKey] = sysTab;
                    }
                    if (sysTab->touched) {
                        touchObj(sysTab->obj);
                        sysTab->touched = false;
                        changedSchema = true;
                    }
                    continue;
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TAB$ (rowid: " << it.first << ", OBJ#: " << dec << sysTab->obj << ", DATAOBJ#: " <<
                        sysTab->dataObj << ", TS#: " << sysTab->ts << ", FILE#: " << sysTab->file << ", BLOCK#: " << sysTab->block <<
                        ", CLUCOLS: " << sysTab->cluCols << ", FLAGS: " << sysTab->flags << ", PROPERTY: " << sysTab->property << ")");
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

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABCOMPART$ (rowid: " << it.first << ", OBJ#: " << dec << sysTabComPart->obj << ", DATAOBJ#: " <<
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

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABCOMPART$ (rowid: " << it.first << ", OBJ#: " << dec << sysTabPart->obj << ", DATAOBJ#: " <<
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

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TABCOMPART$ (rowid: " << it.first << ", OBJ#: " << dec << sysTabSubPart->obj << ", DATAOBJ#: " <<
                        sysTabSubPart->dataObj << ", POBJ#: " << sysTabSubPart->pObj << ")");
                removeRowId.push_back(it.first);
                delete sysTabSubPart;
            }

            for (RowId rowId: removeRowId)
                sysTabSubPartMapRowId.erase(rowId);
            removeRowId.clear();
            sysTabSubPartTouched = false;
        }

        //SYS.SEG$
        if (sysSegTouched) {
            sysSegMapKey.clear();

            for (auto it : sysSegMapRowId) {
                SysSeg* sysSeg = it.second;

                //non-zero segment
                if (sysSeg->file != 0 || sysSeg->block != 0) {
                    SysTabKey sysTabKey(sysSeg->file, sysSeg->block, sysSeg->ts);
                    //find SYS.TAB$
                    auto sysTabMapKeyIt = sysTabMapKey.find(sysTabKey);
                    if (sysTabMapKeyIt != sysTabMapKey.end()) {
                        SysTab* sysTab = sysTabMapKeyIt->second;
                        //find SYS.OBJ$
                        if (sysObjMapObj.find(sysTab->obj) != sysObjMapObj.end()) {
                            SysSegKey sysSegKey(sysSeg->file, sysSeg->block, sysSeg->ts);
                            sysSegMapKey[sysSegKey] = sysSeg;
                            if (sysSeg->touched) {
                                touchObj(sysTab->obj);
                                sysSeg->touched = false;
                                changedSchema = true;
                            }
                            continue;
                        }
                    }
                }

                TRACE(TRACE2_SYSTEM, "SYSTEM: garbage TAB$ (rowid: " << it.first << ", FILE#: " << dec << sysSeg->file << ", BLOCK#: " <<
                        sysSeg->block << ", TS#: " << sysSeg->ts << ", SPARE1: " << sysSeg->spare1 << ")");
                removeRowId.push_back(it.first);
                delete sysSeg;
            }

            for (RowId rowId: removeRowId)
                sysSegMapRowId.erase(rowId);
            removeRowId.clear();
            sysSegTouched = false;
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
                    INFO("dropped schema: " << object->owner << "." << object->name << " (dataobj: " << dec << object->dataObj
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
                INFO("dropped schema: " << object->owner << "." << object->name << " (dataobj: " << dec << object->dataObj
                        << ", obj: " << object->obj << ")");
                delete object;
            }
        }
        objectsTouched.clear();

        for (SchemaElement* element : elements)
            buildMaps(element->owner, element->table, element->keys, element->keysStr, element->options, false);
    }

    void Schema::buildMaps(string& owner, string& table, vector<string>& keys, string& keysStr, typeOPTIONS options, bool output) {
        uint64_t tabCnt = 0;
        regex regexOwner(owner), regexTable(table);

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
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << dec << sysObj->obj << ") - already added");
                continue;
            }

            //object without SYS.TAB$
            auto sysTabMapObjIt = sysTabMapObj.find(sysObj->obj);
            if (sysTabMapObjIt == sysTabMapObj.end()) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << dec << sysObj->obj << ") - SYS.TAB$ entry missing");
                continue;
            }
            SysTab* sysTab = sysTabMapObjIt->second;

            //skip Index Organized Tables (IOT)
            if (sysTab->isIot()) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << dec << sysObj->obj << ") - IOT");
                continue;
            }

            //skip temporary tables
            if (sysObj->isTemporary()) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << dec << sysObj->obj << ") - temporary table");
                continue;
            }

            //skip nested tables
            if (sysTab->isNested()) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << dec << sysObj->obj << ") - nested table");
                continue;
            }

            bool compressed = false;
            if (sysTab->isPartitioned())
                compressed = false;
            else if (sysTab->isInitial()) {
                SysDeferredStg* sysDeferredStg = sysDeferredStgMapObj[sysObj->obj];
                if (sysDeferredStg != nullptr)
                    compressed = sysDeferredStg->isCompressed();
            } else {
                SysSegKey sysSegKey(sysTab->file, sysTab->block, sysTab->ts);
                SysSeg* sysSeg = sysSegMapKey[sysSegKey];
                if (sysSeg != nullptr)
                    compressed = sysSeg->isCompressed();
            }
            //skip compressed tables
            if (compressed) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << dec << sysObj->obj << ") - compressed table");
                continue;
            }

            typeCOL totalPk = 0, maxSegCol = 0, keysCnt = 0;
            bool suppLogTablePrimary = false, suppLogTableAll = false, supLogColMissing = false;

            schemaObject = new OracleObject(sysObj->obj, sysTab->dataObj, sysObj->owner, sysTab->cluCols, options, sysUser->name.c_str(), sysObj->name.c_str());
            if (schemaObject == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleObject) << " bytes memory (for: object creation2)");
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

            if ((oracleAnalyzer->disableChecks & DISABLE_CHECK_SUPPLEMENTAL_LOG) == 0 && (options & OPTIONS_SCHEMA_TABLE) == 0 &&
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
                typeCOL numPk = 0, numSup = 0, guardSegNo = -1;

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
                        ERROR("HINT: check in database for name: SELECT NLS_CHARSET_NAME(" << dec << charmapId << ") FROM DUAL;");
                        RUNTIME_FAIL("table " << sysUser->name << "." << sysObj->name << " - unsupported character set id: " << dec << charmapId <<
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
                    for (vector<string>::iterator it = keys.begin(); it != keys.end(); ++it) {
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

                DEBUG("  - col: " << dec << sysCol->segCol << ": " << sysCol->name << " (pk: " << dec << numPk << ", S: " << dec << numSup << ", G: " << dec << guardSegNo << ")");

                OracleColumn* column = new OracleColumn(sysCol->col, guardSegNo, sysCol->segCol, sysCol->name.c_str(), sysCol->type,
                        sysCol->length, sysCol->precision, sysCol->scale, numPk, charmapId, (sysCol->null_ == 0), sysCol->isInvisible(),
                        sysCol->isStoredAsLob(), sysCol->isConstraint(), sysCol->isAdded(), sysCol->isGuard());
                if (column == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleColumn) << " bytes memory (for: column creation3)");
                }

                totalPk += numPk;
                if (sysCol->segCol > maxSegCol)
                    maxSegCol = sysCol->segCol;

                schemaObject->addColumn(column);
            }

            //check if table has all listed columns
            if (keys.size() != keysCnt) {
                RUNTIME_FAIL("table " << sysUser->name << "." << sysObj->name << " couldn't find all column set (" << keysStr << ")");
            }

            stringstream ss;
            if (output)
                ss << "- found: ";
            else
                ss << "updated schema: ";
            ss << sysUser->name << "." << sysObj->name << " (dataobj: " << dec << sysTab->dataObj << ", obj: " << dec << sysObj->obj << ", columns: " << dec << maxSegCol << ")";
            if (sysTab->isClustered())
                ss << ", part of cluster";
            if (sysTab->isPartitioned())
                ss << ", partitioned(" << dec << partitions << ")";
            if (sysTab->isDependencies())
                ss << ", row dependencies";
            if (sysTab->isRowMovement())
                ss << ", row movement enabled";

            if ((oracleAnalyzer->disableChecks & DISABLE_CHECK_SUPPLEMENTAL_LOG) == 0 && (options & OPTIONS_SCHEMA_TABLE) == 0) {
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
                        ss << ", supplemental log missing, try: ALTER TABLE " << sysUser->name << "." << sysObj->name << " ADD SUPPLEMENTAL LOG GROUP GRP" << dec << sysObj->obj << " (" << keysStr << ") ALWAYS;";
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
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SchemaElement) << " bytes memory (for: schema element)");
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
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysCCol) << " bytes memory (for: SysCCol)");
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
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysCDef) << " bytes memory (for: SysCDef)");
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

        SysCol* sysCol = new SysCol(rowId, obj, col, segCol, intCol, name, type, length, precision, scale, charsetForm, charsetId,
                null_, property1, property2, false);
        if (sysCol == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysCol) << " bytes memory (for: SysCol)");
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
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysDeferredStg) << " bytes memory (for: SysDeferredStg)");
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
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysECol) << " bytes memory (for: SysECol)");
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
                TRACE(TRACE2_SYSTEM, "SYSTEM: disabling single option for object " << name << " (owner " << dec << owner << ")");
            }
            return false;
        }

        SysObj* sysObj = new SysObj(rowId, owner, obj, dataObj, type, name, flags1, flags2, single, false);
        if (sysObj == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysObj) << " bytes memory (for: SysObj)");
        }
        sysObjMapRowId[rowId] = sysObj;
        sysObjMapObj[obj] = sysObj;

        return true;
    }

    bool Schema::dictSysSegAdd(const char* rowIdStr, uint32_t file, uint32_t block, uint32_t ts, uint64_t spare11, uint64_t spare12) {
        RowId rowId(rowIdStr);
        if (sysSegMapRowId.find(rowId) != sysSegMapRowId.end())
            return false;

        SysSeg* sysSeg = new SysSeg(rowId, file, block, ts, spare11, spare12, false);
        if (sysSeg == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysSeg) << " bytes memory (for: SysSeg)");
        }
        sysSegMapRowId[rowId] = sysSeg;
        SysSegKey sysSegKey(file, block, ts);
        sysSegMapKey[sysSegKey] = sysSeg;

        return true;
    }

    bool Schema::dictSysTabAdd(const char* rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, uint32_t ts, uint32_t file, uint32_t block,
            typeCOL cluCols, uint64_t flags1, uint64_t flags2, uint64_t property1, uint64_t property2) {
        RowId rowId(rowIdStr);
        if (sysTabMapRowId.find(rowId) != sysTabMapRowId.end())
            return false;

        SysTab* sysTab = new SysTab(rowId, obj, dataObj, ts, file, block, cluCols, flags1, flags2, property1, property2, false);
        if (sysTab == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysTab) << " bytes memory (for: SysTab)");
        }
        sysTabMapRowId[rowId] = sysTab;
        sysTabMapObj[obj] = sysTab;
        if (file != 0 || block != 0) {
            SysTabKey sysTabKey(file, block, ts);
            sysTabMapKey[sysTabKey] = sysTab;
        }

        return true;
    }

    bool Schema::dictSysTabComPartAdd(const char* rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo) {
        RowId rowId(rowIdStr);
        if (sysTabComPartMapRowId.find(rowId) != sysTabComPartMapRowId.end())
            return false;

        SysTabComPart* sysTabComPart = new SysTabComPart(rowId, obj, dataObj, bo, false);
        if (sysTabComPart == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysTabComPart) << " bytes memory (for: SysTabComPart)");
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
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysTabPart) << " bytes memory (for: SysTabPart)");
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
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysTabSubPart) << " bytes memory (for: SysTabSubPart)");
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
                    TRACE(TRACE2_SYSTEM, "SYSTEM: disabling single option for user " << name << " (" << dec << user << ")");
                }
                return true;
            }

            return false;
        }

        SysUser* sysUser = new SysUser(rowId, user, name, spare11, spare12, single, false);
        if (sysUser == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysUser) << " bytes memory (for: SysUser)");
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
