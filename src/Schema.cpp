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
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <sys/stat.h>

#include "ConfigurationException.h"
#include "OracleAnalyzer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "Reader.h"
#include "RowId.h"
#include "RuntimeException.h"
#include "Schema.h"
#include "SchemaElement.h"

using namespace rapidjson;
using namespace std;

extern const Value& getJSONfieldV(string &fileName, const Value& value, const char* field);
extern const Value& getJSONfieldD(string &fileName, const Document& document, const char* field);

namespace OpenLogReplicator {
    Schema::Schema() :
		object(nullptr) {
    }

    Schema::~Schema() {
        if (object != nullptr) {
            delete object;
            object = nullptr;
        }

        partitionMap.clear();

        for (auto it : objectMap) {
            OracleObject *objectTmp = it.second;
            delete objectTmp;
        }
        objectMap.clear();

        for (auto it : sysTabSubPartMapRowId) {
            SysTabSubPart *sysTabSubPart = it.second;
            delete sysTabSubPart;
        }
        sysTabSubPartMapRowId.clear();
        sysTabSubPartMapKey.clear();

        for (auto it : sysTabComPartMapRowId) {
            SysTabComPart *sysTabComPart = it.second;
            delete sysTabComPart;
        }
        sysTabComPartMapRowId.clear();
        sysTabComPartMapKey.clear();

        for (auto it : sysTabPartMapRowId) {
            SysTabPart *sysTabPart = it.second;
            delete sysTabPart;
        }
        sysTabPartMapRowId.clear();
        sysTabPartMapKey.clear();

        for (auto it : sysTabMapRowId) {
            SysTab *sysTab = it.second;
            delete sysTab;
        }
        sysTabMapRowId.clear();
        sysTabMapObj.clear();

        for (auto it : sysSegMapRowId) {
            SysSeg *sysSeg = it.second;
            delete sysSeg;
        }
        sysSegMapRowId.clear();
        sysSegMapKey.clear();

        for (auto it : sysEColMapRowId) {
            SysECol *sysECol = it.second;
            delete sysECol;
        }
        sysEColMapRowId.clear();
        sysEColMapKey.clear();

        for (auto it : sysDeferredStgMapRowId) {
            SysDeferredStg *sysDeferredStg = it.second;
            delete sysDeferredStg;
        }
        sysDeferredStgMapRowId.clear();
        sysDeferredStgMapObj.clear();

        for (auto it : sysColMapRowId) {
            SysCol *sysCol = it.second;
            delete sysCol;
        }
        sysColMapRowId.clear();
        sysColMapKey.clear();
        sysColMapSeg.clear();

        for (auto it : sysCDefMapRowId) {
            SysCDef *sysCDef = it.second;
            delete sysCDef;
        }
        sysCDefMapRowId.clear();
        sysCDefMapCDef.clear();
        sysCDefMapKey.clear();

        for (auto it : sysCColMapRowId) {
            SysCCol *sysCCol = it.second;
            delete sysCCol;
        }
        sysCColMapRowId.clear();
        sysCColMapKey.clear();

        for (auto it : sysColMapRowId) {
            SysCol *sysCol = it.second;
            delete sysCol;
        }
        sysColMapRowId.clear();

        for (auto it : sysObjMapRowId) {
            SysObj *sysObj = it.second;
            delete sysObj;
        }
        sysObjMapRowId.clear();
        sysObjMapObj.clear();

        for (auto it : sysUserMapRowId) {
            SysUser *sysUser = it.second;
            delete sysUser;
        }
        sysUserMapRowId.clear();
        sysUserMapUser.clear();

        for (SchemaElement *element : elements) {
            delete element;
        }
        elements.clear();
    }

    bool Schema::readSchema(OracleAnalyzer *oracleAnalyzer) {
        ifstream infile;
        string fileName = oracleAnalyzer->database + "-schema.json";
        infile.open(fileName.c_str(), ios::in);

        if (!infile.is_open()) {
            INFO("missing schema for " << oracleAnalyzer->database);
            return false;
        }
        INFO("reading schema for " << oracleAnalyzer->database << " (old style)");

        string schemaJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (schemaJSON.length() == 0 || document.Parse(schemaJSON.c_str()).HasParseError()) {
            RUNTIME_FAIL("parsing " << fileName << " at offset: " << document.GetErrorOffset() <<
                    ", message: " << GetParseError_En(document.GetParseError()));
        }

        if ((oracleAnalyzer->flags & REDO_FLAGS_EXPERIMENTAL_DDL) == 0) {
            const Value& databaseJSON = getJSONfieldD(fileName, document, "database");
            oracleAnalyzer->database = databaseJSON.GetString();

            const Value& bigEndianJSON = getJSONfieldD(fileName, document, "big-endian");
            bool isBigEndian = bigEndianJSON.GetUint64();
            if (isBigEndian)
                oracleAnalyzer->setBigEndian();

            const Value& resetlogsJSON = getJSONfieldD(fileName, document, "resetlogs");
            oracleAnalyzer->resetlogs = resetlogsJSON.GetUint64();

            const Value& activationJSON = getJSONfieldD(fileName, document, "activation");
            oracleAnalyzer->activation = activationJSON.GetUint64();

            const Value& databaseContextJSON = getJSONfieldD(fileName, document, "context");
            oracleAnalyzer->context = databaseContextJSON.GetString();

            const Value& conIdJSON = getJSONfieldD(fileName, document, "con-id");
            oracleAnalyzer->conId = conIdJSON.GetUint64();

            const Value& conNameJSON = getJSONfieldD(fileName, document, "con-name");
            oracleAnalyzer->conName = conNameJSON.GetString();

            const Value& dbBlockChecksumJSON = getJSONfieldD(fileName, document, "db-block-checksum");
            oracleAnalyzer->dbBlockChecksum = dbBlockChecksumJSON.GetString();

            const Value& dbRecoveryFileDestJSON = getJSONfieldD(fileName, document, "db-recovery-file-dest");
            oracleAnalyzer->dbRecoveryFileDest = dbRecoveryFileDestJSON.GetString();

            if (oracleAnalyzer->logArchiveFormat.length() == 0) {
                const Value& logArchiveFormatJSON = getJSONfieldD(fileName, document, "log-archive-format");
                oracleAnalyzer->logArchiveFormat = logArchiveFormatJSON.GetString();
            }

            const Value& logArchiveDestJSON = getJSONfieldD(fileName, document, "log-archive-dest");
            oracleAnalyzer->logArchiveDest = logArchiveDestJSON.GetString();

            const Value& nlsCharacterSetJSON = getJSONfieldD(fileName, document, "nls-character-set");
            oracleAnalyzer->nlsCharacterSet = nlsCharacterSetJSON.GetString();

            const Value& nlsNcharCharacterSetJSON = getJSONfieldD(fileName, document, "nls-nchar-character-set");
            oracleAnalyzer->nlsNcharCharacterSet = nlsNcharCharacterSetJSON.GetString();

            const Value& onlineRedo = getJSONfieldD(fileName, document, "online-redo");
            if (!onlineRedo.IsArray()) {
                CONFIG_FAIL("bad JSON in " << fileName << ", online-redo should be an array");
            }

            for (SizeType i = 0; i < onlineRedo.Size(); ++i) {
                const Value& groupJSON = getJSONfieldV(fileName, onlineRedo[i], "group");
                uint64_t group = groupJSON.GetInt64();

                const Value& path = onlineRedo[i]["path"];
                if (!path.IsArray()) {
                    CONFIG_FAIL("bad JSON, path-mapping should be array");
                }

                Reader *onlineReader = oracleAnalyzer->readerCreate(group);
                for (SizeType j = 0; j < path.Size(); ++j) {
                    const Value& pathVal = path[j];
                    onlineReader->paths.push_back(pathVal.GetString());
                }
            }

            if ((oracleAnalyzer->flags & REDO_FLAGS_ARCH_ONLY) == 0)
                oracleAnalyzer->checkOnlineRedoLogs();
            oracleAnalyzer->archReader = oracleAnalyzer->readerCreate(0);
        }

        const Value& schema = getJSONfieldD(fileName, document, "schema");
        if (!schema.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", schema should be an array");
        }

        for (SizeType i = 0; i < schema.Size(); ++i) {
            const Value& objJSON = getJSONfieldV(fileName, schema[i], "obj");
            typeOBJ obj = objJSON.GetInt64();
            typeDATAOBJ dataObj = 0;

            if (schema[i].HasMember("data-obj")) {
                const Value& dataObjJSON = getJSONfieldV(fileName, schema[i], "data-obj");
                dataObj = dataObjJSON.GetInt64();
            }

            const Value& cluColsJSON = getJSONfieldV(fileName, schema[i], "clu-cols");
            typeCOL cluCols = cluColsJSON.GetInt();

            const Value& totalPkJSON = getJSONfieldV(fileName, schema[i], "total-pk");
            typeCOL totalPk = totalPkJSON.GetInt();

            const Value& optionsJSON = getJSONfieldV(fileName, schema[i], "options");
            uint64_t options = optionsJSON.GetInt64();

            const Value& maxSegColJSON = getJSONfieldV(fileName, schema[i], "max-seg-col");
            typeCOL maxSegCol = maxSegColJSON.GetInt();

            const Value& ownerJSON = getJSONfieldV(fileName, schema[i], "owner");
            const char *owner = ownerJSON.GetString();

            const Value& nameJSON = getJSONfieldV(fileName, schema[i], "name");
            const char *name = nameJSON.GetString();

            object = new OracleObject(obj, dataObj, cluCols, options, owner, name);
            object->totalPk = totalPk;
            object->maxSegCol = maxSegCol;

            const Value& columns = getJSONfieldV(fileName, schema[i], "columns");
            if (!columns.IsArray()) {
                CONFIG_FAIL("bad JSON in " << fileName << ", columns should be an array");
            }

            for (SizeType j = 0; j < columns.Size(); ++j) {
                const Value& colNoJSON = getJSONfieldV(fileName, columns[j], "col-no");
                typeCOL colNo = colNoJSON.GetInt();

                const Value& guardSegNoJSON = getJSONfieldV(fileName, columns[j], "guard-seg-no");
                typeCOL guardSegNo = guardSegNoJSON.GetInt();

                const Value& segColNoJSON = getJSONfieldV(fileName, columns[j], "seg-col-no");
                typeCOL segColNo = segColNoJSON.GetInt();
                if (segColNo > 1000) {
                    CONFIG_FAIL("bad JSON in " << fileName << ", invalid seg-col-no value");
                }

                const Value& columnNameJSON = getJSONfieldV(fileName, columns[j], "name");
                const char* columnName = columnNameJSON.GetString();

                const Value& typeNoJSON = getJSONfieldV(fileName, columns[j], "type-no");
                typeTYPE typeNo = typeNoJSON.GetUint();

                const Value& lengthJSON = getJSONfieldV(fileName, columns[j], "length");
                uint64_t length = lengthJSON.GetUint64();

                const Value& precisionJSON = getJSONfieldV(fileName, columns[j], "precision");
                int64_t precision = precisionJSON.GetInt64();

                const Value& scaleJSON = getJSONfieldV(fileName, columns[j], "scale");
                int64_t scale = scaleJSON.GetInt64();

                const Value& numPkJSON = getJSONfieldV(fileName, columns[j], "num-pk");
                uint64_t numPk = numPkJSON.GetUint64();

                const Value& charsetIdJSON = getJSONfieldV(fileName, columns[j], "charset-id");
                uint64_t charsetId = charsetIdJSON.GetUint64();

                const Value& nullableJSON = getJSONfieldV(fileName, columns[j], "nullable");
                bool nullable = nullableJSON.GetUint64();

                const Value& invisibleJSON = getJSONfieldV(fileName, columns[j], "invisible");
                bool invisible = invisibleJSON.GetUint64();

                const Value& storedAsLobJSON = getJSONfieldV(fileName, columns[j], "stored-as-lob");
                bool storedAsLob = storedAsLobJSON.GetUint64();

                const Value& constraintJSON = getJSONfieldV(fileName, columns[j], "constraint");
                bool constraint = constraintJSON.GetUint64();

                const Value& addedJSON = getJSONfieldV(fileName, columns[j], "added");
                bool added = addedJSON.GetUint64();

                const Value& guardJSON = getJSONfieldV(fileName, columns[j], "guard");
                bool guard = guardJSON.GetUint64();

                OracleColumn *column = new OracleColumn(colNo, guardSegNo, segColNo, columnName, typeNo, length, precision, scale, numPk, charsetId,
                        nullable, invisible, storedAsLob, constraint, added, guard);

                if (column->guard)
                    object->guardSegNo = column->segColNo - 1;

                object->columns.push_back(column);
            }

            if (schema[i].HasMember("partitions")) {
                const Value& partitions = getJSONfieldV(fileName, schema[i], "partitions");
                if (!columns.IsArray()) {
                    CONFIG_FAIL("bad JSON in " << fileName << ", partitions should be an array");
                }

                for (SizeType j = 0; j < partitions.Size(); ++j) {
                    const Value& partitionObjJSON = getJSONfieldV(fileName, partitions[j], "obj");
                    uint64_t partitionObj = partitionObjJSON.GetUint64();

                    const Value& partitionDataObjJSON = getJSONfieldV(fileName, partitions[j], "data-obj");
                    uint64_t partitionDataObj = partitionDataObjJSON.GetUint64();

                    typeOBJ2 objx = (((typeOBJ2)partitionObj)<<32) | ((typeOBJ2)partitionDataObj);
                    object->partitions.push_back(objx);
                }
            }

            object->updatePK();
            addToDict(object);
            object = nullptr;
        }

        infile.close();

        return true;
    }

    void Schema::writeSchema(OracleAnalyzer *oracleAnalyzer) {
        INFO("writing schema information for " << oracleAnalyzer->database << " (old style)");

        string fileName = oracleAnalyzer->database + "-schema.json";
        ofstream outfile;
        outfile.open(fileName.c_str(), ios::out | ios::trunc);

        if (!outfile.is_open()) {
            RUNTIME_FAIL("writing schema data");
        }

        stringstream ss;
        bool hasPrev = false;
        ss << "{";

        if ((oracleAnalyzer->flags & REDO_FLAGS_EXPERIMENTAL_DDL) == 0) {
            ss << "\"database\":\"" << oracleAnalyzer->database << "\"," <<
                    "\"big-endian\":" << dec << oracleAnalyzer->isBigEndian << "," <<
                    "\"resetlogs\":" << dec << oracleAnalyzer->resetlogs << "," <<
                    "\"activation\":" << dec << oracleAnalyzer->activation << "," <<
                    "\"context\":\"" << oracleAnalyzer->context << "\"," <<
                    "\"con-id\":" << dec << oracleAnalyzer->conId << "," <<
                    "\"con-name\":\"" << oracleAnalyzer->conName << "\"," <<
                    "\"db-recovery-file-dest\":\"";
            writeEscapeValue(ss, oracleAnalyzer->dbRecoveryFileDest);
            ss << "\"," << "\"log-archive-dest\":\"";
            writeEscapeValue(ss, oracleAnalyzer->logArchiveDest);
            ss << "\"," << "\"db-block-checksum\":\"";
            writeEscapeValue(ss, oracleAnalyzer->dbBlockChecksum);
            ss << "\"," << "\"log-archive-format\":\"";
            writeEscapeValue(ss, oracleAnalyzer->logArchiveFormat);
            ss << "\"," << "\"nls-character-set\":\"";
            writeEscapeValue(ss, oracleAnalyzer->nlsCharacterSet);
            ss << "\"," << "\"nls-nchar-character-set\":\"";
            writeEscapeValue(ss, oracleAnalyzer->nlsNcharCharacterSet);

            ss << "\"," << "\"online-redo\":[";

            bool hasPrev2;
            for (Reader *reader : oracleAnalyzer->readers) {
                if (reader->group == 0)
                    continue;

                if (hasPrev)
                    ss << ",";
                else
                    hasPrev = true;

                hasPrev2 = false;
                ss << "{\"group\":" << reader->group << ",\"path\":[";
                for (string &path : reader->paths) {
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
            ss << "],";
        }

        hasPrev = false;
        ss << "\"schema\":[";
        for (auto it : objectMap) {
            OracleObject *objectTmp = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"obj\":" << dec << objectTmp->obj << "," <<
                    "\"data-obj\":" << dec << objectTmp->dataObj << "," <<
                    "\"clu-cols\":" << dec << objectTmp->cluCols << "," <<
                    "\"total-pk\":" << dec << objectTmp->totalPk << "," <<
                    "\"options\":" << dec << objectTmp->options << "," <<
                    "\"max-seg-col\":" << dec << objectTmp->maxSegCol << "," <<
                    "\"owner\":\"" << objectTmp->owner << "\"," <<
                    "\"name\":\"" << objectTmp->name << "\"," <<
                    "\"columns\":[";

            for (uint64_t i = 0; i < objectTmp->columns.size(); ++i) {
                if (objectTmp->columns[i] == nullptr)
                    continue;

                if (i > 0)
                    ss << ",";
                ss << "{\"col-no\":" << dec << objectTmp->columns[i]->colNo << "," <<
                        "\"guard-seg-no\":" << dec << objectTmp->columns[i]->guardSegNo << "," <<
                        "\"seg-col-no\":" << dec << objectTmp->columns[i]->segColNo << "," <<
                        "\"name\":\"" << objectTmp->columns[i]->name << "\"," <<
                        "\"type-no\":" << dec << objectTmp->columns[i]->typeNo << "," <<
                        "\"length\":" << dec << objectTmp->columns[i]->length << "," <<
                        "\"precision\":" << dec << objectTmp->columns[i]->precision << "," <<
                        "\"scale\":" << dec << objectTmp->columns[i]->scale << "," <<
                        "\"num-pk\":" << dec << objectTmp->columns[i]->numPk << "," <<
                        "\"charset-id\":" << dec << objectTmp->columns[i]->charsetId << "," <<
                        "\"nullable\":" << dec << objectTmp->columns[i]->nullable << "," <<
                        "\"invisible\":" << dec << objectTmp->columns[i]->invisible << "," <<
                        "\"stored-as-lob\":" << dec << objectTmp->columns[i]->storedAsLob << "," <<
                        "\"constraint\":" << dec << objectTmp->columns[i]->constraint << "," <<
                        "\"added\":" << dec << objectTmp->columns[i]->added << "," <<
                        "\"guard\":" << dec << objectTmp->columns[i]->guard << "}";
            }
            ss << "]";

            if (objectTmp->partitions.size() > 0) {
                ss << ",\"partitions\":[";
                for (uint64_t i = 0; i < objectTmp->partitions.size(); ++i) {
                    if (i > 0)
                        ss << ",";
                    typeOBJ partitionObj = objectTmp->partitions[i] >> 32;
                    typeDATAOBJ partitionDataObj = objectTmp->partitions[i] & 0xFFFFFFFF;
                    ss << "{\"obj\":" << dec << partitionObj << "," <<
                            "\"data-obj\":" << dec << partitionDataObj << "}";
                }
                ss << "]";
            }
            ss << "}";
        }

        ss << "]}";
        outfile << ss.rdbuf();
        outfile.close();
    }

    bool Schema::readSys(OracleAnalyzer *oracleAnalyzer) {
        if ((oracleAnalyzer->flags & REDO_FLAGS_EXPERIMENTAL_DDL) == 0)
            return false;

        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: searching for previous schema on: " << oracleAnalyzer->checkpointPath);
        DIR *dir;
        if ((dir = opendir(oracleAnalyzer->checkpointPath.c_str())) == nullptr) {
            RUNTIME_FAIL("can't access directory: " << oracleAnalyzer->checkpointPath);
        }

        string newLastCheckedDay;
        struct dirent *ent;
        typeSCN fileScnMax = 0;
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;

            struct stat fileStat;
            string fileName = ent->d_name;
            TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: found previous schema: " << oracleAnalyzer->checkpointPath << "/" << fileName);

            string fullName = oracleAnalyzer->checkpointPath + "/" + ent->d_name;
            if (stat(fullName.c_str(), &fileStat)) {
                WARNING("can't read file information for: " << fullName);
                continue;
            }

            if (S_ISDIR(fileStat.st_mode))
                continue;

            string prefix = oracleAnalyzer->database + "-schema-";
            if (fileName.length() < prefix.length() || fileName.substr(0, prefix.length()).compare(prefix) != 0)
                continue;

            string suffix = ".json";
            if (fileName.length() < suffix.length() || fileName.substr(fileName.length() - suffix.length(), fileName.length()).compare(suffix) != 0)
                continue;

            string fileScnStr = fileName.substr(prefix.length(), fileName.length() - suffix.length());
            typeSCN fileScn;
            try {
                fileScn = strtoull(fileScnStr.c_str(), nullptr, 10);
            } catch (exception &e) {
                //ignore other files
                continue;
            }
            if (fileScn < oracleAnalyzer->scn && fileScn > fileScnMax)
                fileScnMax = fileScn;
        }
        closedir(dir);

        //none found
        if (fileScnMax == 0)
            return false;

        oracleAnalyzer->schemaScn = fileScnMax;
        ifstream infile;
        string fileName = oracleAnalyzer->checkpointPath + "/" + oracleAnalyzer->database + "-schema-" + to_string(oracleAnalyzer->schemaScn) + ".json";
        infile.open(fileName.c_str(), ios::in);

        if (!infile.is_open()) {
            ERROR("error reading " << fileName);
        }
        INFO("reading schema for " << oracleAnalyzer->database << " for scn: " << fileScnMax);

        string schemaJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (schemaJSON.length() == 0 || document.Parse(schemaJSON.c_str()).HasParseError()) {
            RUNTIME_FAIL("parsing " << fileName << " at offset: " << document.GetErrorOffset() <<
                    ", message: " << GetParseError_En(document.GetParseError()));
        }

        const Value& databaseJSON = getJSONfieldD(fileName, document, "database");
        oracleAnalyzer->database = databaseJSON.GetString();

        const Value& bigEndianJSON = getJSONfieldD(fileName, document, "big-endian");
        bool isBigEndian = bigEndianJSON.GetUint64();
        if (isBigEndian)
            oracleAnalyzer->setBigEndian();

        const Value& resetlogsJSON = getJSONfieldD(fileName, document, "resetlogs");
        oracleAnalyzer->resetlogs = resetlogsJSON.GetUint64();

        const Value& activationJSON = getJSONfieldD(fileName, document, "activation");
        oracleAnalyzer->activation = activationJSON.GetUint64();

        const Value& databaseContextJSON = getJSONfieldD(fileName, document, "context");
        oracleAnalyzer->context = databaseContextJSON.GetString();

        const Value& conIdJSON = getJSONfieldD(fileName, document, "con-id");
        oracleAnalyzer->conId = conIdJSON.GetUint64();

        const Value& conNameJSON = getJSONfieldD(fileName, document, "con-name");
        oracleAnalyzer->conName = conNameJSON.GetString();

        const Value& dbRecoveryFileDestJSON = getJSONfieldD(fileName, document, "db-recovery-file-dest");
        oracleAnalyzer->dbRecoveryFileDest = dbRecoveryFileDestJSON.GetString();

        const Value& dbBlockChecksumJSON = getJSONfieldD(fileName, document, "db-block-checksum");
        oracleAnalyzer->dbBlockChecksum = dbBlockChecksumJSON.GetString();

        if (oracleAnalyzer->logArchiveFormat.length() == 0) {
            const Value& logArchiveFormatJSON = getJSONfieldD(fileName, document, "log-archive-format");
            oracleAnalyzer->logArchiveFormat = logArchiveFormatJSON.GetString();
        }

        const Value& logArchiveDestJSON = getJSONfieldD(fileName, document, "log-archive-dest");
        oracleAnalyzer->logArchiveDest = logArchiveDestJSON.GetString();

        const Value& nlsCharacterSetJSON = getJSONfieldD(fileName, document, "nls-character-set");
        oracleAnalyzer->nlsCharacterSet = nlsCharacterSetJSON.GetString();

        const Value& nlsNcharCharacterSetJSON = getJSONfieldD(fileName, document, "nls-nchar-character-set");
        oracleAnalyzer->nlsNcharCharacterSet = nlsNcharCharacterSetJSON.GetString();

        const Value& onlineRedo = getJSONfieldD(fileName, document, "online-redo");
        if (!onlineRedo.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", online-redo should be an array");
        }

        for (SizeType i = 0; i < onlineRedo.Size(); ++i) {
            const Value& groupJSON = getJSONfieldV(fileName, onlineRedo[i], "group");
            uint64_t group = groupJSON.GetInt64();

            const Value& path = onlineRedo[i]["path"];
            if (!path.IsArray()) {
                CONFIG_FAIL("bad JSON, path-mapping should be array");
            }

            Reader *onlineReader = oracleAnalyzer->readerCreate(group);
            for (SizeType j = 0; j < path.Size(); ++j) {
                const Value& pathVal = path[j];
                onlineReader->paths.push_back(pathVal.GetString());
            }
        }

        if ((oracleAnalyzer->flags & REDO_FLAGS_ARCH_ONLY) == 0)
            oracleAnalyzer->checkOnlineRedoLogs();
        oracleAnalyzer->archReader = oracleAnalyzer->readerCreate(0);

        //SYS.USER$
        const Value& sysUser = getJSONfieldD(fileName, document, "sys-user");
        if (!sysUser.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-user should be an array");
        }

        for (SizeType i = 0; i < sysUser.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysUser[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& userJSON = getJSONfieldV(fileName, sysUser[i], "user");
            typeUSER user = userJSON.GetUint();

            const Value& nameJSON = getJSONfieldV(fileName, sysUser[i], "name");
            const char *name = nameJSON.GetString();

            const Value& spare1JSON = getJSONfieldV(fileName, sysUser[i], "spare1");
            uint64_t spare1 = spare1JSON.GetUint64();

            dictSysUserAdd(rowId, user, name, spare1, false);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.USER$: " << dec << sysUser.Size());

        //SYS.OBJ$
        const Value& sysObj = getJSONfieldD(fileName, document, "sys-obj");
        if (!sysObj.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-obj should be an array");
        }

        for (SizeType i = 0; i < sysObj.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysObj[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& ownerJSON = getJSONfieldV(fileName, sysObj[i], "owner");
            typeUSER owner = ownerJSON.GetUint();

            const Value& objJSON = getJSONfieldV(fileName, sysObj[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& dataObjJSON = getJSONfieldV(fileName, sysObj[i], "data-obj");
            typeDATAOBJ dataObj = dataObjJSON.GetUint();

            const Value& typeJSON = getJSONfieldV(fileName, sysObj[i], "type");
            typeTYPE type = typeJSON.GetUint();

            const Value& nameJSON = getJSONfieldV(fileName, sysObj[i], "name");
            const char *name = nameJSON.GetString();

            const Value& flagsJSON = getJSONfieldV(fileName, sysObj[i], "flags");
            uint32_t flags = flagsJSON.GetUint();

            dictSysObjAdd(rowId, owner, obj, dataObj, type, name, flags);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.OBJ$: " << dec << sysObj.Size());

        //SYS.COL$
        const Value& sysCol = getJSONfieldD(fileName, document, "sys-col");
        if (!sysCol.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-col should be an array");
        }

        for (SizeType i = 0; i < sysCol.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysCol[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysCol[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& colJSON = getJSONfieldV(fileName, sysCol[i], "col");
            typeCOL col = colJSON.GetUint();

            const Value& segColJSON = getJSONfieldV(fileName, sysCol[i], "seg-col");
            typeCOL segCol = segColJSON.GetUint();

            const Value& intColJSON = getJSONfieldV(fileName, sysCol[i], "int-col");
            typeCOL intCol = intColJSON.GetUint();

            const Value& nameJSON = getJSONfieldV(fileName, sysCol[i], "name");
            const char *name = nameJSON.GetString();

            const Value& typeJSON = getJSONfieldV(fileName, sysCol[i], "type");
            typeTYPE type = typeJSON.GetUint();

            const Value& lengthJSON = getJSONfieldV(fileName, sysCol[i], "length");
            uint64_t length = lengthJSON.GetUint64();

            const Value& precisionJSON = getJSONfieldV(fileName, sysCol[i], "precision");
            uint64_t precision = precisionJSON.GetInt64();

            const Value& scaleJSON = getJSONfieldV(fileName, sysCol[i], "scale");
            uint64_t scale = scaleJSON.GetInt64();

            const Value& charsetFormJSON = getJSONfieldV(fileName, sysCol[i], "charset-form");
            uint64_t charsetForm = charsetFormJSON.GetUint64();

            const Value& charsetIdJSON = getJSONfieldV(fileName, sysCol[i], "charset-id");
            uint64_t charsetId = charsetIdJSON.GetUint64();

            const Value& nullJSON = getJSONfieldV(fileName, sysCol[i], "null");
            uint64_t null_ = nullJSON.GetInt64();

            const Value& propertyJSON = getJSONfieldV(fileName, sysCol[i], "property");
            if (!propertyJSON.IsArray() || propertyJSON.Size() < 2) {
                CONFIG_FAIL("bad JSON in " << fileName << ", property should be an array");
            }
            uint64_t property1 = propertyJSON[0].GetUint64();
            uint64_t property2 = propertyJSON[1].GetUint64();

            dictSysColAdd(rowId, obj, col, segCol, intCol, name, type, length, precision, scale, charsetForm, charsetId, null_, property1, property2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.COL$: " << dec << sysCol.Size());

        //SYS.CCOL$
        const Value& sysCCol = getJSONfieldD(fileName, document, "sys-ccol");
        if (!sysCCol.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-ccol should be an array");
        }

        for (SizeType i = 0; i < sysCCol.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysCCol[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& conJSON = getJSONfieldV(fileName, sysCCol[i], "con");
            typeCON con = conJSON.GetUint();

            const Value& intColJSON = getJSONfieldV(fileName, sysCCol[i], "int-col");
            typeCON intCol = intColJSON.GetUint();

            const Value& objJSON = getJSONfieldV(fileName, sysCCol[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& spare1JSON = getJSONfieldV(fileName, sysCCol[i], "spare1");
            uint64_t spare1 = spare1JSON.GetUint64();

            dictSysCColAdd(rowId, con, intCol, obj, spare1);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.CCOL$: " << dec << sysCCol.Size());

        //SYS.CDEF$
        const Value& sysCDef = getJSONfieldD(fileName, document, "sys-cdef");
        if (!sysCDef.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-cdef should be an array");
        }

        for (SizeType i = 0; i < sysCDef.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysCDef[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& conJSON = getJSONfieldV(fileName, sysCDef[i], "con");
            typeCON con = conJSON.GetUint();

            const Value& objJSON = getJSONfieldV(fileName, sysCDef[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& typeJSON = getJSONfieldV(fileName, sysCDef[i], "type");
            typeTYPE type = typeJSON.GetUint();

            dictSysCDefAdd(rowId, con, obj, type);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.CDEF$: " << dec << sysCDef.Size());

        //SYS.DEFERRED_STG$
        const Value& sysDeferredStg = getJSONfieldD(fileName, document, "sys-deferredstg");
        if (!sysDeferredStg.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-deferredstg should be an array");
        }

        for (SizeType i = 0; i < sysDeferredStg.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysDeferredStg[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysDeferredStg[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& flagsStgJSON = getJSONfieldV(fileName, sysDeferredStg[i], "flags-stg");
            uint64_t flagsStg = flagsStgJSON.GetUint64();

            dictSysDeferredStgAdd(rowId, obj, flagsStg);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.DEFERRED_STG$: " << dec << sysDeferredStg.Size());

        //SYS.ECOL$
        const Value& sysECol = getJSONfieldD(fileName, document, "sys-ecol");
        if (!sysECol.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-ecol should be an array");
        }

        for (SizeType i = 0; i < sysECol.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysECol[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysECol[i], "tab-obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& colNumJSON = getJSONfieldV(fileName, sysECol[i], "col-num");
            typeCON colNum = colNumJSON.GetUint();

            const Value& guardIdJSON = getJSONfieldV(fileName, sysECol[i], "guard-id");
            uint64_t guardId = guardIdJSON.GetUint();

            dictSysEColAdd(rowId, obj, colNum, guardId);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.ECOL$: " << dec << sysECol.Size());

        //SYS.SEG$
        const Value& sysSeg = getJSONfieldD(fileName, document, "sys-seg");
        if (!sysSeg.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-seg should be an array");
        }

        for (SizeType i = 0; i < sysSeg.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysSeg[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& fileJSON = getJSONfieldV(fileName, sysSeg[i], "file");
            uint32_t file = fileJSON.GetUint();

            const Value& blockJSON = getJSONfieldV(fileName, sysSeg[i], "block");
            uint32_t block = blockJSON.GetUint();

            const Value& tsJSON = getJSONfieldV(fileName, sysSeg[i], "ts");
            uint32_t ts = tsJSON.GetUint();

            const Value& spare1JSON = getJSONfieldV(fileName, sysSeg[i], "spare1");
            uint64_t spare1 = spare1JSON.GetUint();

            dictSysSegAdd(rowId, file, block, ts, spare1);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.SEG$: " << dec << sysSeg.Size());

        //SYS.TAB$
        const Value& sysTab = getJSONfieldD(fileName, document, "sys-tab");
        if (!sysTab.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-tab should be an array");
        }

        for (SizeType i = 0; i < sysTab.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysTab[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysTab[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& dataObjJSON = getJSONfieldV(fileName, sysTab[i], "data-obj");
            typeDATAOBJ dataObj = dataObjJSON.GetUint();

            const Value& tsJSON = getJSONfieldV(fileName, sysTab[i], "ts");
            uint32_t ts = tsJSON.GetUint();

            const Value& fileJSON = getJSONfieldV(fileName, sysTab[i], "file");
            uint32_t file = fileJSON.GetUint();

            const Value& blockJSON = getJSONfieldV(fileName, sysTab[i], "block");
            uint32_t block = blockJSON.GetUint();

            const Value& cluColsJSON = getJSONfieldV(fileName, sysTab[i], "clu-cols");
            typeCOL cluCols = cluColsJSON.GetInt();

            const Value& flagsJSON = getJSONfieldV(fileName, sysTab[i], "flags");
            uint64_t flags = flagsJSON.GetUint64();

            const Value& propertyJSON = getJSONfieldV(fileName, sysTab[i], "property");
            if (!propertyJSON.IsArray() || propertyJSON.Size() < 2) {
                CONFIG_FAIL("bad JSON in " << fileName << ", property should be an array");
            }
            uint64_t property1 = propertyJSON[0].GetUint64();
            uint64_t property2 = propertyJSON[1].GetUint64();

            dictSysTabAdd(rowId, obj, dataObj, ts, file, block, cluCols, flags, property1, property2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TAB$: " << dec << sysTab.Size());

        //SYS.TABPART$
        const Value& sysTabPart = getJSONfieldD(fileName, document, "sys-tabpart");
        if (!sysTabPart.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-tabpart should be an array");
        }

        for (SizeType i = 0; i < sysTabPart.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysTabPart[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysTabPart[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& dataObjJSON = getJSONfieldV(fileName, sysTabPart[i], "data-obj");
            typeDATAOBJ dataObj = dataObjJSON.GetUint();

            const Value& boJSON = getJSONfieldV(fileName, sysTabPart[i], "bo");
            typeOBJ bo = boJSON.GetUint();

            dictSysTabPartAdd(rowId, obj, dataObj, bo);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABPART$: " << dec << sysTabPart.Size());

        //SYS.TABCOMPART$
        const Value& sysTabComPart = getJSONfieldD(fileName, document, "sys-tabcompart");
        if (!sysTabComPart.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-tabcompart should be an array");
        }

        for (SizeType i = 0; i < sysTabComPart.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysTabComPart[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysTabComPart[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& dataObjJSON = getJSONfieldV(fileName, sysTabComPart[i], "data-obj");
            typeDATAOBJ dataObj = dataObjJSON.GetUint();

            const Value& boJSON = getJSONfieldV(fileName, sysTabComPart[i], "bo");
            typeOBJ bo = boJSON.GetUint();

            dictSysTabComPartAdd(rowId, obj, dataObj, bo);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABCOMPART$: " << dec << sysTabComPart.Size());

        //SYS.TABSUBPART$
        const Value& sysTabSubPart = getJSONfieldD(fileName, document, "sys-tabsubpart");
        if (!sysTabSubPart.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-tabsubpart should be an array");
        }

        for (SizeType i = 0; i < sysTabSubPart.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysTabSubPart[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysTabSubPart[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& dataObjJSON = getJSONfieldV(fileName, sysTabSubPart[i], "data-obj");
            typeDATAOBJ dataObj = dataObjJSON.GetUint();

            const Value& pObjJSON = getJSONfieldV(fileName, sysTabSubPart[i], "p-obj");
            typeOBJ pObj = pObjJSON.GetUint();

            dictSysTabComPartAdd(rowId, obj, dataObj, pObj);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABSUBPART$: " << dec << sysTabSubPart.Size());

        infile.close();

        return true;
    }

    void Schema::writeSys(OracleAnalyzer *oracleAnalyzer) {
        if ((oracleAnalyzer->flags & REDO_FLAGS_EXPERIMENTAL_DDL) == 0)
            return;

        string fileName = oracleAnalyzer->checkpointPath + "/" + oracleAnalyzer->database + "-schema-" + to_string(oracleAnalyzer->schemaScn) + ".json";
        ofstream outfile;
        outfile.open(fileName.c_str(), ios::out | ios::trunc);

        if (!outfile.is_open()) {
            RUNTIME_FAIL("writing schema data");
        }

        stringstream ss;
        ss << "{\"database\":\"" << oracleAnalyzer->database << "\"," <<
                "\"big-endian\":" << dec << oracleAnalyzer->isBigEndian << "," <<
                "\"resetlogs\":" << dec << oracleAnalyzer->resetlogs << "," <<
                "\"activation\":" << dec << oracleAnalyzer->activation << "," <<
                "\"context\":\"" << oracleAnalyzer->context << "\"," <<
                "\"con-id\":" << dec << oracleAnalyzer->conId << "," <<
                "\"con-name\":\"" << oracleAnalyzer->conName << "\"," <<
                "\"db-recovery-file-dest\":\"";
        writeEscapeValue(ss, oracleAnalyzer->dbRecoveryFileDest);
        ss << "\"," << "\"log-archive-dest\":\"";
        writeEscapeValue(ss, oracleAnalyzer->logArchiveDest);
        ss << "\"," << "\"db-block-checksum\":\"";
        writeEscapeValue(ss, oracleAnalyzer->dbBlockChecksum);
        ss << "\"," << "\"log-archive-format\":\"";
        writeEscapeValue(ss, oracleAnalyzer->logArchiveFormat);
        ss << "\"," << "\"nls-character-set\":\"";
        writeEscapeValue(ss, oracleAnalyzer->nlsCharacterSet);
        ss << "\"," << "\"nls-nchar-character-set\":\"";
        writeEscapeValue(ss, oracleAnalyzer->nlsNcharCharacterSet);

        ss << "\"," << "\"online-redo\":[";

        bool hasPrev = false, hasPrev2;
        for (Reader *reader : oracleAnalyzer->readers) {
            if (reader->group == 0)
                continue;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            hasPrev2 = false;
            ss << "{\"group\":" << reader->group << ",\"path\":[";
            for (string &path : reader->paths) {
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

        //SYS.USER$
        ss << "]," << endl << "\"sys-user\":[";
        hasPrev = false;
        for (auto it : sysUserMapRowId) {
            SysUser *sysUser = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysUser->rowId << "\"," <<
                    "\"user\":" << dec << sysUser->user << "," <<
                    "\"name\":\"" << sysUser->name << "\"," <<
                    "\"spare1\":" << dec << sysUser->spare1 << "}";
        }

        //SYS.OBJ$
        ss << "]," << endl << "\"sys-obj\":[";
        hasPrev = false;
        for (auto it : sysObjMapRowId) {
            SysObj *sysObj = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysObj->rowId << "\"," <<
                    "\"owner\":" << dec << sysObj->owner << "," <<
                    "\"obj\":" << dec << sysObj->obj << "," <<
                    "\"data-obj\":" << dec << sysObj->dataObj << "," <<
                    "\"type\":" << dec << sysObj->type << "," <<
                    "\"name\":\"" << sysObj->name << "\"," <<
                    "\"flags\":" << dec << sysObj->flags << "}";
        }

        //SYS.COL$
        ss << "]," << endl << "\"sys-col\":[";
        hasPrev = false;
        for (auto it : sysColMapRowId) {
            SysCol *sysCol = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysCol->rowId << "\"," <<
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
        }

        //SYS.CCOL$
        ss << "]," << endl << "\"sys-ccol\":[";
        hasPrev = false;
        for (auto it : sysCColMapRowId) {
            SysCCol *sysCCol = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysCCol->rowId << "\"," <<
                    "\"con\":" << dec << sysCCol->con << "," <<
                    "\"int-col\":" << dec << sysCCol->intCol << "," <<
                    "\"obj\":" << dec << sysCCol->obj << "," <<
                    "\"spare1\":" << dec << sysCCol->spare1 << "}";
        }

        //SYS.CDEF$
        ss << "]," << endl << "\"sys-cdef\":[";
        hasPrev = false;
        for (auto it : sysCDefMapRowId) {
            SysCDef *sysCDef = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysCDef->rowId << "\"," <<
                    "\"con\":" << dec << sysCDef->con << "," <<
                    "\"obj\":" << dec << sysCDef->obj << "," <<
                    "\"type\":" << dec << sysCDef->type << "}";
        }

        //SYS.DEFERRED_STG$
        ss << "]," << endl << "\"sys-deferredstg\":[";
        hasPrev = false;
        for (auto it : sysDeferredStgMapRowId) {
            SysDeferredStg *sysDeferredStg = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysDeferredStg->rowId << "\"," <<
                    "\"obj\":" << dec << sysDeferredStg->obj << "," <<
                    "\"flags-stg\":" << dec << sysDeferredStg->flagsStg << "}";
        }

        //SYS.ECOL$
        ss << "]," << endl << "\"sys-ecol\":[";
        hasPrev = false;
        for (auto it : sysEColMapRowId) {
            SysECol *sysECol = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysECol->rowId << "\"," <<
                    "\"tab-obj\":" << dec << sysECol->tabObj << "," <<
                    "\"col-num\":" << dec << sysECol->colNum << "," <<
                    "\"guard-id\":" << dec << sysECol->guardId << "}";
        }

        //SYS.SEG$
        ss << "]," << endl << "\"sys-seg\":[";
        hasPrev = false;
        for (auto it : sysSegMapRowId) {
            SysSeg *sysSeg = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysSeg->rowId << "\"," <<
                    "\"file\":" << dec << sysSeg->file << "," <<
                    "\"block\":" << dec << sysSeg->block << "," <<
                    "\"ts\":" << dec << sysSeg->ts << "," <<
                    "\"spare1\":" << dec << sysSeg->spare1 << "}";
        }

        //SYS.TAB$
        ss << "]," << endl << "\"sys-tab\":[";
        hasPrev = false;
        for (auto it : sysTabMapRowId) {
            SysTab *sysTab = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysTab->rowId << "\"," <<
                    "\"obj\":" << dec << sysTab->obj << "," <<
                    "\"data-obj\":" << dec << sysTab->dataObj << "," <<
                    "\"ts\":" << dec << sysTab->ts << "," <<
                    "\"file\":" << dec << sysTab->file << "," <<
                    "\"block\":" << dec << sysTab->block << "," <<
                    "\"clu-cols\":" << dec << sysTab->cluCols << "," <<
                    "\"flags\":" << dec << sysTab->flags << "," <<
                    "\"property\":" << dec << sysTab->property << "}";
        }

        //SYS.TABPART$
        ss << "]," << endl << "\"sys-tabpart\":[";
        hasPrev = false;
        for (auto it : sysTabPartMapRowId) {
            SysTabPart *sysTabPart = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysTabPart->rowId << "\"," <<
                    "\"obj\":" << dec << sysTabPart->obj << "," <<
                    "\"data-obj\":" << dec << sysTabPart->dataObj << "," <<
                    "\"bo\":" << dec << sysTabPart->bo << "}";
        }

        //SYS.TABCOMPART$
        ss << "]," << endl << "\"sys-tabcompart\":[";
        hasPrev = false;
        for (auto it : sysTabComPartMapRowId) {
            SysTabComPart *sysTabComPart = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysTabComPart->rowId << "\"," <<
                    "\"obj\":" << dec << sysTabComPart->obj << "," <<
                    "\"data-obj\":" << dec << sysTabComPart->dataObj << "," <<
                    "\"bo\":" << dec << sysTabComPart->bo << "}";
        }

        //SYS.TABSUBPART$
        ss << "]," << endl << "\"sys-tabsubpart\":[";
        hasPrev = false;
        for (auto it : sysTabSubPartMapRowId) {
            SysTabSubPart *sysTabSubPart = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"row-id\":\"" << sysTabSubPart->rowId << "\"," <<
                    "\"obj\":" << dec << sysTabSubPart->obj << "," <<
                    "\"data-obj\":" << dec << sysTabSubPart->dataObj << "," <<
                    "\"p-obj\":" << dec << sysTabSubPart->pObj << "}";
        }

        ss << "]}";
        outfile << ss.rdbuf();

        outfile.close();
    }

    void Schema::addToDict(OracleObject *object) {
        if (objectMap[object->obj] == nullptr) {
            objectMap[object->obj] = object;
        } else {
            CONFIG_FAIL("can't add object obj: " << dec << object->obj << ", dataObj: " << object->dataObj << " - another object with the same id");
        }

        if (partitionMap[object->obj] == nullptr) {
            partitionMap[object->obj] = object;
        } else {
            CONFIG_FAIL("can't add object obj: " << dec << object->obj << ", dataObj: " << object->dataObj << " - another object with the same id");
        }

        for (typeOBJ2 objx : object->partitions) {
            typeOBJ partitionObj = objx >> 32;
            typeDATAOBJ partitionDataObj = objx & 0xFFFFFFFF;

            if (partitionMap[partitionObj] == nullptr) {
                partitionMap[partitionObj] = object;
            } else {
                CONFIG_FAIL("can't add object obj: " << dec << partitionObj << ", dataObj: " << partitionDataObj << " - another object with the same id");
            }
        }
    }

    OracleObject *Schema::checkDict(typeOBJ obj, typeDATAOBJ dataObj) {
        auto it = partitionMap.find(obj);
        if (it == partitionMap.end())
            return nullptr;
        return (*it).second;
    }

    stringstream& Schema::writeEscapeValue(stringstream &ss, string &str) {
        const char *c_str = str.c_str();
        for (uint64_t i = 0; i < str.length(); ++i) {
            if (*c_str == '\t' || *c_str == '\r' || *c_str == '\n' || *c_str == '\b') {
                //skip
            } else if (*c_str == '"' || *c_str == '\\' || *c_str == '/') {
                ss << '\\' << *c_str;
            } else {
                ss << *c_str;
            }
            ++c_str;
        }
        return ss;
    }

    SchemaElement* Schema::addElement(void) {
        SchemaElement *element = new SchemaElement();
        if (element == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SchemaElement) << " bytes memory (for: schema element)");
        }
        elements.push_back(element);
        return element;
    }

    SchemaElement* Schema::addElement(const char *owner, const char *table, uint64_t options) {
        SchemaElement *element = new SchemaElement(owner, table, options);
        if (element == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SchemaElement) << " bytes memory (for: schema element)");
        }
        elements.push_back(element);
        return element;
    }

    bool Schema::dictSysUserAdd(const char *rowIdStr, typeUSER user, const char *name, uint64_t spare1, bool trackDDL) {
        RowId rowId(rowIdStr);
        SysUser *sysUser = sysUserMapRowId[rowId];
        if (sysUser != nullptr) {
            if (!sysUser->trackDDL) {
                if (trackDDL)
                    sysUser->trackDDL = true;
                return true;
            }

            return false;
        }

        sysUser = new SysUser(rowId, user, name, spare1, trackDDL);
        sysUserMapRowId[rowId] = sysUser;
        sysUserMapUser[user] = sysUser;

        return true;
    }

    bool Schema::dictSysObjAdd(const char *rowIdStr, typeUSER owner, typeOBJ obj, typeDATAOBJ dataObj, typeTYPE type, const char *name, uint32_t flags) {
        RowId rowId(rowIdStr);
        if (sysObjMapRowId[rowId] != nullptr)
            return false;

        SysObj *sysObj = new SysObj(rowId, owner, obj, dataObj, type, name, flags);
        sysObjMapRowId[rowId] = sysObj;
        sysObjMapObj[obj] = sysObj;

        return true;
    }

    bool Schema::dictSysColAdd(const char *rowIdStr, typeOBJ obj, typeCOL col, typeCOL segCol, typeCOL intCol, const char *name, typeTYPE type, uint64_t length,
            int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, int64_t null_, uint64_t property1, uint64_t property2) {
        RowId rowId(rowIdStr);
        if (sysColMapRowId[rowId] != nullptr)
            return false;

        SysCol *sysCol = new SysCol(rowId, obj, col, segCol, intCol, name, type, length, precision, scale, charsetForm, charsetId,
                null_, property1, property2);
        sysColMapRowId[rowId] = sysCol;
        SysColKey sysColKey(obj, intCol);
        sysColMapKey[sysColKey] = sysCol;
        SysColSeg sysColSeg(obj, segCol);
        sysColMapSeg[sysColSeg] = sysCol;

        return true;
    }

    bool Schema::dictSysCColAdd(const char *rowIdStr, typeCON con, typeCOL intCol, typeOBJ obj, uint64_t spare1) {
        RowId rowId(rowIdStr);
        if (sysCColMapRowId[rowId] != nullptr)
            return false;

        SysCCol *sysCCol = new SysCCol(rowId, con, intCol, obj, spare1);
        sysCColMapRowId[rowId] = sysCCol;
        SysCColKey sysCColKey(obj, intCol, con);
        sysCColMapKey[sysCColKey] = sysCCol;

        return true;
    }

    bool Schema::dictSysCDefAdd(const char *rowIdStr, typeCON con, typeOBJ obj, typeTYPE type) {
        RowId rowId(rowIdStr);
        if (sysCDefMapRowId[rowId] != nullptr)
            return false;

        SysCDef *sysCDef = new SysCDef(rowId, con, obj, type);
        sysCDefMapRowId[rowId] = sysCDef;
        sysCDefMapCDef[con] = sysCDef;
        SysCDefKey sysCDefKey(obj, con);
        sysCDefMapKey[sysCDefKey] = sysCDef;

        return true;
    }

    bool Schema::dictSysDeferredStgAdd(const char *rowIdStr, typeOBJ obj, uint64_t flagsStg) {
        RowId rowId(rowIdStr);
        if (sysDeferredStgMapRowId[rowId] != nullptr)
            return false;

        SysDeferredStg *sysDeferredStg = new SysDeferredStg(rowId, obj, flagsStg);
        sysDeferredStgMapRowId[rowId] = sysDeferredStg;
        sysDeferredStgMapObj[obj] = sysDeferredStg;

        return true;
    }

    bool Schema::dictSysEColAdd(const char *rowIdStr, typeOBJ tabObj, uint32_t colNum, uint32_t guardId) {
        RowId rowId(rowIdStr);
        if (sysEColMapRowId[rowId] != nullptr)
            return false;

        SysECol *sysECol = new SysECol(rowId, tabObj, colNum, guardId);
        sysEColMapRowId[rowId] = sysECol;
        SysEColKey sysEColKey(tabObj, colNum);
        sysEColMapKey[sysEColKey] = sysECol;

        return true;
    }

    bool Schema::dictSysSegAdd(const char *rowIdStr, uint32_t file, uint32_t block, uint32_t ts, uint64_t spare1) {
        RowId rowId(rowIdStr);
        if (sysSegMapRowId[rowId] != nullptr)
            return false;

        SysSeg *sysSeg = new SysSeg(rowId, file, block, ts, spare1);
        sysSegMapRowId[rowId] = sysSeg;
        SysSegKey sysSegKey(file, block, ts);
        sysSegMapKey[sysSegKey] = sysSeg;

        return true;
    }

    bool Schema::dictSysTabAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, uint32_t ts, uint32_t file, uint32_t block, typeCOL cluCols,
            uint64_t flags, uint64_t property1, uint64_t property2) {
        RowId rowId(rowIdStr);
        if (sysTabMapRowId[rowId] != nullptr)
            return false;

        SysTab *sysTab = new SysTab(rowId, obj, dataObj, ts, file, block, cluCols, flags, property1, property2);
        sysTabMapRowId[rowId] = sysTab;
        sysTabMapObj[obj] = sysTab;

        return true;
    }

    bool Schema::dictSysTabPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo) {
        RowId rowId(rowIdStr);
        if (sysTabPartMapRowId[rowId] != nullptr)
            return false;

        SysTabPart *sysTabPart = new SysTabPart(rowId, obj, dataObj, bo);
        sysTabPartMapRowId[rowId] = sysTabPart;
        SysTabPartKey sysTabPartKey(bo, obj);
        sysTabPartMapKey[sysTabPartKey] = sysTabPart;

        return true;
    }

    bool Schema::dictSysTabComPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo) {
        RowId rowId(rowIdStr);
        if (sysTabComPartMapRowId[rowId] != nullptr)
            return false;

        SysTabComPart *sysTabComPart = new SysTabComPart(rowId, obj, dataObj, bo);
        sysTabComPartMapRowId[rowId] = sysTabComPart;
        SysTabComPartKey sysTabComPartKey(bo, obj);
        sysTabComPartMapKey[sysTabComPartKey] = sysTabComPart;

        return true;
    }

    bool Schema::dictSysTabSubPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ pObj) {
        RowId rowId(rowIdStr);
        if (sysTabSubPartMapRowId[rowId] != nullptr)
            return false;

        SysTabSubPart *sysTabSubPart = new SysTabSubPart(rowId, obj, dataObj, pObj);
        sysTabSubPartMapRowId[rowId] = sysTabSubPart;
        SysTabSubPartKey sysTabSubPartKey(pObj, obj);
        sysTabSubPartMapKey[sysTabSubPartKey] = sysTabSubPart;

        return true;
    }
}
