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
#include <regex>
#include <sys/stat.h>

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

extern const Value& getJSONfieldV(string &fileName, const Value& value, const char* field);
extern const Value& getJSONfieldD(string &fileName, const Document& document, const char* field);

namespace OpenLogReplicator {
    Schema::Schema() :
        schemaObject(nullptr),
		sysCColKeyTouched(false),
		sysCDefKeyTouched(false),
		sysCDefConTouched(false),
		sysColKeyTouched(false),
		sysColSegTouched(false),
		sysDeferredStgObjTouched(false),
		sysEColKeyTouched(false),
		sysObjObjTouched(false),
		sysSegKeyTouched(false),
		sysTabObjTouched(false),
		sysTabKeyTouched(false),
		sysTabComPartKeyTouched(false),
        sysTabPartKeyTouched(false),
		sysTabSubPartKeyTouched(false),
		sysUserUserTouched(false),
		touched(false) {
    }

    Schema::~Schema() {
        if (schemaObject != nullptr) {
            delete schemaObject;
            schemaObject = nullptr;
        }

        partitionMap.clear();

        for (auto it : objectMap) {
            OracleObject *object = it.second;
            delete object;
        }
        objectMap.clear();

        for (auto it : sysCColMapRowId) {
            SysCCol *sysCCol = it.second;
            delete sysCCol;
        }
        sysCColMapRowId.clear();
        sysCColMapKey.clear();

        for (auto it : sysCDefMapRowId) {
            SysCDef *sysCDef = it.second;
            delete sysCDef;
        }
        sysCDefMapRowId.clear();
        sysCDefMapCon.clear();
        sysCDefMapKey.clear();

        for (auto it : sysColMapRowId) {
            SysCol *sysCol = it.second;
            delete sysCol;
        }
        sysColMapRowId.clear();
        sysColMapKey.clear();
        sysColMapSeg.clear();

        for (auto it : sysDeferredStgMapRowId) {
            SysDeferredStg *sysDeferredStg = it.second;
            delete sysDeferredStg;
        }
        sysDeferredStgMapRowId.clear();
        sysDeferredStgMapObj.clear();

        for (auto it : sysEColMapRowId) {
            SysECol *sysECol = it.second;
            delete sysECol;
        }
        sysEColMapRowId.clear();
        sysEColMapKey.clear();

        for (auto it : sysObjMapRowId) {
            SysObj *sysObj = it.second;
            delete sysObj;
        }
        sysObjMapRowId.clear();
        sysObjMapObj.clear();

        for (auto it : sysSegMapRowId) {
            SysSeg *sysSeg = it.second;
            delete sysSeg;
        }
        sysSegMapRowId.clear();
        sysSegMapKey.clear();

        for (auto it : sysTabMapRowId) {
            SysTab *sysTab = it.second;
            delete sysTab;
        }
        sysTabMapRowId.clear();
        sysTabMapObj.clear();
        sysTabMapKey.clear();

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

        for (auto it : sysTabSubPartMapRowId) {
            SysTabSubPart *sysTabSubPart = it.second;
            delete sysTabSubPart;
        }
        sysTabSubPartMapRowId.clear();
        sysTabSubPartMapKey.clear();

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

        string schemaStringJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (schemaStringJSON.length() == 0 || document.Parse(schemaStringJSON.c_str()).HasParseError()) {
            RUNTIME_FAIL("parsing " << fileName << " at offset: " << document.GetErrorOffset() <<
                    ", message: " << GetParseError_En(document.GetParseError()));
        }

        if ((oracleAnalyzer->flags & REDO_FLAGS_EXPERIMENTAL_DDL) == 0) {
            const Value& databaseJSON = getJSONfieldD(fileName, document, "database");
            oracleAnalyzer->database = databaseJSON.GetString();

            const Value& bigEndianJSON = getJSONfieldD(fileName, document, "big-endian");
            bool bigEndian = bigEndianJSON.GetUint();
            if (bigEndian)
                oracleAnalyzer->setBigEndian();

            const Value& resetlogsJSON = getJSONfieldD(fileName, document, "resetlogs");
            oracleAnalyzer->resetlogs = resetlogsJSON.GetUint();

            const Value& activationJSON = getJSONfieldD(fileName, document, "activation");
            oracleAnalyzer->activation = activationJSON.GetUint();

            const Value& databaseContextJSON = getJSONfieldD(fileName, document, "context");
            oracleAnalyzer->context = databaseContextJSON.GetString();

            const Value& conIdJSON = getJSONfieldD(fileName, document, "con-id");
            oracleAnalyzer->conId = conIdJSON.GetUint();

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

            const Value& onlineRedoJSON = getJSONfieldD(fileName, document, "online-redo");
            if (!onlineRedoJSON.IsArray()) {
                CONFIG_FAIL("bad JSON in " << fileName << ", online-redo should be an array");
            }

            for (SizeType i = 0; i < onlineRedoJSON.Size(); ++i) {
                const Value& groupJSON = getJSONfieldV(fileName, onlineRedoJSON[i], "group");
                int64_t group = groupJSON.GetInt64();

                const Value& path = onlineRedoJSON[i]["path"];
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

        const Value& schemaJSON = getJSONfieldD(fileName, document, "schema");
        if (!schemaJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", schema should be an array");
        }

        for (SizeType i = 0; i < schemaJSON.Size(); ++i) {
            const Value& objJSON = getJSONfieldV(fileName, schemaJSON[i], "obj");
            typeOBJ obj = objJSON.GetUint();
            typeDATAOBJ dataObj = 0;

            if (schemaJSON[i].HasMember("data-obj")) {
                const Value& dataObjJSON = getJSONfieldV(fileName, schemaJSON[i], "data-obj");
                dataObj = dataObjJSON.GetUint();
            }

            const Value& userJSON = getJSONfieldV(fileName, schemaJSON[i], "user");
            typeUSER user = userJSON.GetUint();

            const Value& cluColsJSON = getJSONfieldV(fileName, schemaJSON[i], "clu-cols");
            typeCOL cluCols = cluColsJSON.GetInt();

            const Value& totalPkJSON = getJSONfieldV(fileName, schemaJSON[i], "total-pk");
            typeCOL totalPk = totalPkJSON.GetInt();

            const Value& optionsJSON = getJSONfieldV(fileName, schemaJSON[i], "options");
            typeOPTIONS options = optionsJSON.GetUint();

            const Value& maxSegColJSON = getJSONfieldV(fileName, schemaJSON[i], "max-seg-col");
            typeCOL maxSegCol = maxSegColJSON.GetInt();

            const Value& ownerJSON = getJSONfieldV(fileName, schemaJSON[i], "owner");
            const char *owner = ownerJSON.GetString();

            const Value& nameJSON = getJSONfieldV(fileName, schemaJSON[i], "name");
            const char *name = nameJSON.GetString();

            schemaObject = new OracleObject(obj, dataObj, user, cluCols, options, owner, name);
            if (schemaObject == nullptr) {
                RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleObject) << " bytes memory (for: object creation)");
            }
            schemaObject->totalPk = totalPk;
            schemaObject->maxSegCol = maxSegCol;

            const Value& columnsJSON = getJSONfieldV(fileName, schemaJSON[i], "columns");
            if (!columnsJSON.IsArray()) {
                CONFIG_FAIL("bad JSON in " << fileName << ", columns should be an array");
            }

            for (SizeType j = 0; j < columnsJSON.Size(); ++j) {
                const Value& colNoJSON = getJSONfieldV(fileName, columnsJSON[j], "col-no");
                typeCOL colNo = colNoJSON.GetInt();

                const Value& segColNoJSON = getJSONfieldV(fileName, columnsJSON[j], "seg-col-no");
                typeCOL segColNo = segColNoJSON.GetInt();
                if (segColNo > 1000) {
                    CONFIG_FAIL("bad JSON in " << fileName << ", invalid seg-col-no value");
                }

                const Value& columnNameJSON = getJSONfieldV(fileName, columnsJSON[j], "name");
                const char* columnName = columnNameJSON.GetString();

                const Value& typeNoJSON = getJSONfieldV(fileName, columnsJSON[j], "type-no");
                typeTYPE typeNo = typeNoJSON.GetUint();

                const Value& lengthJSON = getJSONfieldV(fileName, columnsJSON[j], "length");
                uint64_t length = lengthJSON.GetUint64();

                typeCOL guardSegNo = -1;
                if (columnsJSON[j].HasMember("guard-seg-no")) {
                    const Value& guardSegNoJSON = getJSONfieldV(fileName, columnsJSON[j], "guard-seg-no");
                    guardSegNo = guardSegNoJSON.GetInt();
                }

                int64_t precision = -1;
                if (columnsJSON[j].HasMember("precision")) {
                    const Value& precisionJSON = getJSONfieldV(fileName, columnsJSON[j], "precision");
                    precision = precisionJSON.GetInt64();
                }

                int64_t scale =  -1;
                if (columnsJSON[j].HasMember("scale")) {
                    const Value& scaleJSON = getJSONfieldV(fileName, columnsJSON[j], "scale");
                    scale = scaleJSON.GetInt64();
                }

                uint64_t numPk = 0;
                if (columnsJSON[j].HasMember("num-pk")) {
                    const Value& numPkJSON = getJSONfieldV(fileName, columnsJSON[j], "num-pk");
                    numPk = numPkJSON.GetUint64();
                }

                uint64_t charsetId = 0;
                if (columnsJSON[j].HasMember("charset-id")) {
                    const Value& charsetIdJSON = getJSONfieldV(fileName, columnsJSON[j], "charset-id");
                    charsetId = charsetIdJSON.GetUint64();
                }

                bool nullable = false;
                if (columnsJSON[j].HasMember("nullable")) {
                    const Value& nullableJSON = getJSONfieldV(fileName, columnsJSON[j], "nullable");
                    nullable = nullableJSON.GetUint();
                }

                bool invisible = false;
                if (columnsJSON[j].HasMember("invisible")) {
                    const Value& invisibleJSON = getJSONfieldV(fileName, columnsJSON[j], "invisible");
                    invisible = invisibleJSON.GetUint();
                }

                bool storedAsLob = false;
                if (columnsJSON[j].HasMember("stored-as-lob")) {
                    const Value& storedAsLobJSON = getJSONfieldV(fileName, columnsJSON[j], "stored-as-lob");
                    storedAsLob = storedAsLobJSON.GetUint();
                }

                bool constraint = false;
                if (columnsJSON[j].HasMember("constraint")) {
                    const Value& constraintJSON = getJSONfieldV(fileName, columnsJSON[j], "constraint");
                    constraint = constraintJSON.GetUint();
                }

                bool added = false;
                if  (columnsJSON[j].HasMember("added")) {
                    const Value& addedJSON = getJSONfieldV(fileName, columnsJSON[j], "added");
                    added = addedJSON.GetUint();
                }

                bool guard = false;
                if (columnsJSON[j].HasMember("guard")) {
                    const Value& guardJSON = getJSONfieldV(fileName, columnsJSON[j], "guard");
                    guard = guardJSON.GetUint();
                }

                OracleColumn *column = new OracleColumn(colNo, guardSegNo, segColNo, columnName, typeNo, length, precision, scale, numPk, charsetId,
                        nullable, invisible, storedAsLob, constraint, added, guard);
                if (column == nullptr) {
                    RUNTIME_FAIL("couldn't allocate " << dec << sizeof(OracleColumn) << " bytes memory (for: column creation1)");
                }

                if (column->guard)
                    schemaObject->guardSegNo = column->segColNo - 1;

                schemaObject->columns.push_back(column);
            }

            if (schemaJSON[i].HasMember("partitions")) {
                const Value& partitionsJSON = getJSONfieldV(fileName, schemaJSON[i], "partitions");
                if (!partitionsJSON.IsArray()) {
                    CONFIG_FAIL("bad JSON in " << fileName << ", partitions should be an array");
                }

                for (SizeType j = 0; j < partitionsJSON.Size(); ++j) {
                    const Value& partitionObjJSON = getJSONfieldV(fileName, partitionsJSON[j], "obj");
                    typeOBJ partitionObj = partitionObjJSON.GetUint();

                    const Value& partitionDataObjJSON = getJSONfieldV(fileName, partitionsJSON[j], "data-obj");
                    typeOBJ partitionDataObj = partitionDataObjJSON.GetUint();

                    typeOBJ2 objx = (((typeOBJ2)partitionObj) << 32) | ((typeOBJ2)partitionDataObj);
                    schemaObject->partitions.push_back(objx);
                }
            }

            schemaObject->updatePK();
            addToDict(schemaObject);
            schemaObject = nullptr;
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
                    "\"big-endian\":" << dec << oracleAnalyzer->bigEndian << "," <<
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
            OracleObject *object = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"obj\":" << dec << object->obj << "," <<
                    "\"data-obj\":" << dec << object->dataObj << "," <<
                    "\"user\":" << dec << object->user << "," <<
                    "\"clu-cols\":" << dec << object->cluCols << "," <<
                    "\"total-pk\":" << dec << object->totalPk << "," <<
                    "\"options\":" << dec << (uint64_t)object->options << "," <<
                    "\"max-seg-col\":" << dec << object->maxSegCol << "," <<
                    "\"owner\":\"" << object->owner << "\"," <<
                    "\"name\":\"" << object->name << "\"," <<
                    "\"columns\":[";

            for (uint64_t i = 0; i < object->columns.size(); ++i) {
                if (object->columns[i] == nullptr)
                    continue;

                if (i > 0)
                    ss << ",";
                ss << "{\"col-no\":" << dec << object->columns[i]->colNo <<
                        ",\"seg-col-no\":" << dec << object->columns[i]->segColNo <<
                        ",\"name\":\"" << object->columns[i]->name << "\"" <<
                        ",\"type-no\":" << dec << object->columns[i]->typeNo <<
                        ",\"length\":" << dec << object->columns[i]->length;
                if (object->columns[i]->guardSegNo != -1)
                    ss << ",\"guard-seg-no\":" << dec << object->columns[i]->guardSegNo;
                if (object->columns[i]->precision != -1)
                    ss << ",\"precision\":" << dec << object->columns[i]->precision;
                if (object->columns[i]->scale != -1)
                    ss << ",\"scale\":" << dec << object->columns[i]->scale;
                if (object->columns[i]->numPk > 0)
                    ss << ",\"num-pk\":" << dec << object->columns[i]->numPk;
                if (object->columns[i]->charsetId != 0)
                    ss << ",\"charset-id\":" << dec << object->columns[i]->charsetId;
                if (object->columns[i]->nullable)
                    ss << ",\"nullable\":" << dec << object->columns[i]->nullable;
                if (object->columns[i]->invisible)
                    ss << ",\"invisible\":" << dec << object->columns[i]->invisible;
                if (object->columns[i]->storedAsLob)
                    ss << ",\"stored-as-lob\":" << dec << object->columns[i]->storedAsLob;
                if (object->columns[i]->constraint)
                    ss << ",\"constraint\":" << dec << object->columns[i]->constraint;
                if (object->columns[i]->added)
                    ss << ",\"added\":" << dec << object->columns[i]->added;
                if (object->columns[i]->guard)
                    ss << ",\"guard\":" << dec << object->columns[i]->guard;
                ss << "}";
            }
            ss << "]";

            if (object->partitions.size() > 0) {
                ss << ",\"partitions\":[";
                for (uint64_t i = 0; i < object->partitions.size(); ++i) {
                    if (i > 0)
                        ss << ",";
                    typeOBJ partitionObj = object->partitions[i] >> 32;
                    typeDATAOBJ partitionDataObj = object->partitions[i] & 0xFFFFFFFF;
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
        bool bigEndian = bigEndianJSON.GetUint();
        if (bigEndian)
            oracleAnalyzer->setBigEndian();

        const Value& resetlogsJSON = getJSONfieldD(fileName, document, "resetlogs");
        oracleAnalyzer->resetlogs = resetlogsJSON.GetUint();

        const Value& activationJSON = getJSONfieldD(fileName, document, "activation");
        oracleAnalyzer->activation = activationJSON.GetUint();

        const Value& databaseContextJSON = getJSONfieldD(fileName, document, "context");
        oracleAnalyzer->context = databaseContextJSON.GetString();

        const Value& conIdJSON = getJSONfieldD(fileName, document, "con-id");
        oracleAnalyzer->conId = conIdJSON.GetUint();

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

        const Value& onlineRedoJSON = getJSONfieldD(fileName, document, "online-redo");
        if (!onlineRedoJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", online-redo should be an array");
        }

        for (SizeType i = 0; i < onlineRedoJSON.Size(); ++i) {
            const Value& groupJSON = getJSONfieldV(fileName, onlineRedoJSON[i], "group");
            uint64_t group = groupJSON.GetInt64();

            const Value& path = onlineRedoJSON[i]["path"];
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
        const Value& sysUserJSON = getJSONfieldD(fileName, document, "sys-user");
        if (!sysUserJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-user should be an array");
        }

        for (SizeType i = 0; i < sysUserJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysUserJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& userJSON = getJSONfieldV(fileName, sysUserJSON[i], "user");
            typeUSER user = userJSON.GetUint();

            const Value& nameJSON = getJSONfieldV(fileName, sysUserJSON[i], "name");
            const char *name = nameJSON.GetString();

            const Value& spare1JSON = getJSONfieldV(fileName, sysUserJSON[i], "spare1");
            if (!spare1JSON.IsArray() || spare1JSON.Size() < 2) {
                CONFIG_FAIL("bad JSON in " << fileName << ", spare1 should be an array");
            }
            uint64_t spare11 = spare1JSON[0].GetUint64();
            uint64_t spare12 = spare1JSON[1].GetUint64();

            dictSysUserAdd(rowId, user, name, spare11, spare12, false);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.USER$: " << dec << sysUserJSON.Size());

        //SYS.OBJ$
        const Value& sysObjJSON = getJSONfieldD(fileName, document, "sys-obj");
        if (!sysObjJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-obj should be an array");
        }

        for (SizeType i = 0; i < sysObjJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysObjJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& ownerJSON = getJSONfieldV(fileName, sysObjJSON[i], "owner");
            typeUSER owner = ownerJSON.GetUint();

            const Value& objJSON = getJSONfieldV(fileName, sysObjJSON[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& dataObjJSON = getJSONfieldV(fileName, sysObjJSON[i], "data-obj");
            typeDATAOBJ dataObj = dataObjJSON.GetUint();

            const Value& typeJSON = getJSONfieldV(fileName, sysObjJSON[i], "type");
            typeTYPE type = typeJSON.GetUint();

            const Value& nameJSON = getJSONfieldV(fileName, sysObjJSON[i], "name");
            const char *name = nameJSON.GetString();

            const Value& flagsJSON = getJSONfieldV(fileName, sysObjJSON[i], "flags");
            if (!flagsJSON.IsArray() || flagsJSON.Size() < 2) {
                CONFIG_FAIL("bad JSON in " << fileName << ", flags should be an array");
            }
            uint64_t flags1 = flagsJSON[0].GetUint64();
            uint64_t flags2 = flagsJSON[1].GetUint64();

            dictSysObjAdd(rowId, owner, obj, dataObj, type, name, flags1, flags2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.OBJ$: " << dec << sysObjJSON.Size());

        //SYS.COL$
        const Value& sysColJSON = getJSONfieldD(fileName, document, "sys-col");
        if (!sysColJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-col should be an array");
        }

        for (SizeType i = 0; i < sysColJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysColJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysColJSON[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& colJSON = getJSONfieldV(fileName, sysColJSON[i], "col");
            typeCOL col = colJSON.GetInt();

            const Value& segColJSON = getJSONfieldV(fileName, sysColJSON[i], "seg-col");
            typeCOL segCol = segColJSON.GetInt();

            const Value& intColJSON = getJSONfieldV(fileName, sysColJSON[i], "int-col");
            typeCOL intCol = intColJSON.GetInt();

            const Value& nameJSON = getJSONfieldV(fileName, sysColJSON[i], "name");
            const char *name = nameJSON.GetString();

            const Value& typeJSON = getJSONfieldV(fileName, sysColJSON[i], "type");
            typeTYPE type = typeJSON.GetUint();

            const Value& lengthJSON = getJSONfieldV(fileName, sysColJSON[i], "length");
            uint64_t length = lengthJSON.GetUint64();

            const Value& precisionJSON = getJSONfieldV(fileName, sysColJSON[i], "precision");
            int64_t precision = precisionJSON.GetInt64();

            const Value& scaleJSON = getJSONfieldV(fileName, sysColJSON[i], "scale");
            int64_t scale = scaleJSON.GetInt64();

            const Value& charsetFormJSON = getJSONfieldV(fileName, sysColJSON[i], "charset-form");
            uint64_t charsetForm = charsetFormJSON.GetUint64();

            const Value& charsetIdJSON = getJSONfieldV(fileName, sysColJSON[i], "charset-id");
            uint64_t charsetId = charsetIdJSON.GetUint64();

            const Value& nullJSON = getJSONfieldV(fileName, sysColJSON[i], "null");
            int64_t null_ = nullJSON.GetInt64();

            const Value& propertyJSON = getJSONfieldV(fileName, sysColJSON[i], "property");
            if (!propertyJSON.IsArray() || propertyJSON.Size() < 2) {
                CONFIG_FAIL("bad JSON in " << fileName << ", property should be an array");
            }
            uint64_t property1 = propertyJSON[0].GetUint64();
            uint64_t property2 = propertyJSON[1].GetUint64();

            dictSysColAdd(rowId, obj, col, segCol, intCol, name, type, length, precision, scale, charsetForm, charsetId, null_, property1, property2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.COL$: " << dec << sysColJSON.Size());

        //SYS.CCOL$
        const Value& sysCColJSON = getJSONfieldD(fileName, document, "sys-ccol");
        if (!sysCColJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-ccol should be an array");
        }

        for (SizeType i = 0; i < sysCColJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysCColJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& conJSON = getJSONfieldV(fileName, sysCColJSON[i], "con");
            typeCON con = conJSON.GetUint();

            const Value& intColJSON = getJSONfieldV(fileName, sysCColJSON[i], "int-col");
            typeCON intCol = intColJSON.GetUint();

            const Value& objJSON = getJSONfieldV(fileName, sysCColJSON[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& spare1JSON = getJSONfieldV(fileName, sysCColJSON[i], "spare1");
            if (!spare1JSON.IsArray() || spare1JSON.Size() < 2) {
                CONFIG_FAIL("bad JSON in " << fileName << ", spare1 should be an array");
            }
            uint64_t spare11 = spare1JSON[0].GetUint64();
            uint64_t spare12 = spare1JSON[1].GetUint64();

            dictSysCColAdd(rowId, con, intCol, obj, spare11, spare12);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.CCOL$: " << dec << sysCColJSON.Size());

        //SYS.CDEF$
        const Value& sysCDefJSON = getJSONfieldD(fileName, document, "sys-cdef");
        if (!sysCDefJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-cdef should be an array");
        }

        for (SizeType i = 0; i < sysCDefJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysCDefJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& conJSON = getJSONfieldV(fileName, sysCDefJSON[i], "con");
            typeCON con = conJSON.GetUint();

            const Value& objJSON = getJSONfieldV(fileName, sysCDefJSON[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& typeJSON = getJSONfieldV(fileName, sysCDefJSON[i], "type");
            typeTYPE type = typeJSON.GetUint();

            dictSysCDefAdd(rowId, con, obj, type);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.CDEF$: " << dec << sysCDefJSON.Size());

        //SYS.DEFERRED_STG$
        const Value& sysDeferredStgJSON = getJSONfieldD(fileName, document, "sys-deferredstg");
        if (!sysDeferredStgJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-deferredstg should be an array");
        }

        for (SizeType i = 0; i < sysDeferredStgJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysDeferredStgJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysDeferredStgJSON[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& flagsStgJSON = getJSONfieldV(fileName, sysDeferredStgJSON[i], "flags-stg");
            if (!flagsStgJSON.IsArray() || flagsStgJSON.Size() < 2) {
                CONFIG_FAIL("bad JSON in " << fileName << ", flags-stg should be an array");
            }
            uint64_t flagsStg1 = flagsStgJSON[0].GetUint64();
            uint64_t flagsStg2 = flagsStgJSON[1].GetUint64();

            dictSysDeferredStgAdd(rowId, obj, flagsStg1, flagsStg2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.DEFERRED_STG$: " << dec << sysDeferredStgJSON.Size());

        //SYS.ECOL$
        const Value& sysEColJSON = getJSONfieldD(fileName, document, "sys-ecol");
        if (!sysEColJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-ecol should be an array");
        }

        for (SizeType i = 0; i < sysEColJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysEColJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysEColJSON[i], "tab-obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& colNumJSON = getJSONfieldV(fileName, sysEColJSON[i], "col-num");
            typeCOL colNum = colNumJSON.GetInt();

            const Value& guardIdJSON = getJSONfieldV(fileName, sysEColJSON[i], "guard-id");
            typeCOL guardId = guardIdJSON.GetInt();

            dictSysEColAdd(rowId, obj, colNum, guardId);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.ECOL$: " << dec << sysEColJSON.Size());

        //SYS.SEG$
        const Value& sysSegJSON = getJSONfieldD(fileName, document, "sys-seg");
        if (!sysSegJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-seg should be an array");
        }

        for (SizeType i = 0; i < sysSegJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysSegJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& fileJSON = getJSONfieldV(fileName, sysSegJSON[i], "file");
            uint32_t file = fileJSON.GetUint();

            const Value& blockJSON = getJSONfieldV(fileName, sysSegJSON[i], "block");
            uint32_t block = blockJSON.GetUint();

            const Value& tsJSON = getJSONfieldV(fileName, sysSegJSON[i], "ts");
            uint32_t ts = tsJSON.GetUint();

            const Value& spare1JSON = getJSONfieldV(fileName, sysSegJSON[i], "spare1");
            if (!spare1JSON.IsArray() || spare1JSON.Size() < 2) {
                CONFIG_FAIL("bad JSON in " << fileName << ", spare1 should be an array");
            }
            uint64_t spare11 = spare1JSON[0].GetUint64();
            uint64_t spare12 = spare1JSON[1].GetUint64();

            dictSysSegAdd(rowId, file, block, ts, spare11, spare12);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.SEG$: " << dec << sysSegJSON.Size());

        //SYS.TAB$
        const Value& sysTabJSON = getJSONfieldD(fileName, document, "sys-tab");
        if (!sysTabJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-tab should be an array");
        }

        for (SizeType i = 0; i < sysTabJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysTabJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysTabJSON[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& dataObjJSON = getJSONfieldV(fileName, sysTabJSON[i], "data-obj");
            typeDATAOBJ dataObj = dataObjJSON.GetUint();

            const Value& tsJSON = getJSONfieldV(fileName, sysTabJSON[i], "ts");
            uint32_t ts = tsJSON.GetUint();

            const Value& fileJSON = getJSONfieldV(fileName, sysTabJSON[i], "file");
            uint32_t file = fileJSON.GetUint();

            const Value& blockJSON = getJSONfieldV(fileName, sysTabJSON[i], "block");
            uint32_t block = blockJSON.GetUint();

            const Value& cluColsJSON = getJSONfieldV(fileName, sysTabJSON[i], "clu-cols");
            typeCOL cluCols = cluColsJSON.GetInt();

            const Value& flagsJSON = getJSONfieldV(fileName, sysTabJSON[i], "flags");
            if (!flagsJSON.IsArray() || flagsJSON.Size() < 2) {
                CONFIG_FAIL("bad JSON in " << fileName << ", flags should be an array");
            }
            uint64_t flags1 = flagsJSON[0].GetUint64();
            uint64_t flags2 = flagsJSON[1].GetUint64();

            const Value& propertyJSON = getJSONfieldV(fileName, sysTabJSON[i], "property");
            if (!propertyJSON.IsArray() || propertyJSON.Size() < 2) {
                CONFIG_FAIL("bad JSON in " << fileName << ", property should be an array");
            }
            uint64_t property1 = propertyJSON[0].GetUint64();
            uint64_t property2 = propertyJSON[1].GetUint64();

            dictSysTabAdd(rowId, obj, dataObj, ts, file, block, cluCols, flags1, flags2, property1, property2);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TAB$: " << dec << sysTabJSON.Size());

        //SYS.TABPART$
        const Value& sysTabPartJSON = getJSONfieldD(fileName, document, "sys-tabpart");
        if (!sysTabPartJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-tabpart should be an array");
        }

        for (SizeType i = 0; i < sysTabPartJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysTabPartJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysTabPartJSON[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& dataObjJSON = getJSONfieldV(fileName, sysTabPartJSON[i], "data-obj");
            typeDATAOBJ dataObj = dataObjJSON.GetUint();

            const Value& boJSON = getJSONfieldV(fileName, sysTabPartJSON[i], "bo");
            typeOBJ bo = boJSON.GetUint();

            dictSysTabPartAdd(rowId, obj, dataObj, bo);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABPART$: " << dec << sysTabPartJSON.Size());

        //SYS.TABCOMPART$
        const Value& sysTabComPartJSON = getJSONfieldD(fileName, document, "sys-tabcompart");
        if (!sysTabComPartJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-tabcompart should be an array");
        }

        for (SizeType i = 0; i < sysTabComPartJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysTabComPartJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysTabComPartJSON[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& dataObjJSON = getJSONfieldV(fileName, sysTabComPartJSON[i], "data-obj");
            typeDATAOBJ dataObj = dataObjJSON.GetUint();

            const Value& boJSON = getJSONfieldV(fileName, sysTabComPartJSON[i], "bo");
            typeOBJ bo = boJSON.GetUint();

            dictSysTabComPartAdd(rowId, obj, dataObj, bo);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABCOMPART$: " << dec << sysTabComPartJSON.Size());

        //SYS.TABSUBPART$
        const Value& sysTabSubPartJSON = getJSONfieldD(fileName, document, "sys-tabsubpart");
        if (!sysTabSubPartJSON.IsArray()) {
            CONFIG_FAIL("bad JSON in " << fileName << ", sys-tabsubpart should be an array");
        }

        for (SizeType i = 0; i < sysTabSubPartJSON.Size(); ++i) {
            const Value& rowIdJSON = getJSONfieldV(fileName, sysTabSubPartJSON[i], "row-id");
            const char *rowId = rowIdJSON.GetString();

            const Value& objJSON = getJSONfieldV(fileName, sysTabSubPartJSON[i], "obj");
            typeOBJ obj = objJSON.GetUint();

            const Value& dataObjJSON = getJSONfieldV(fileName, sysTabSubPartJSON[i], "data-obj");
            typeDATAOBJ dataObj = dataObjJSON.GetUint();

            const Value& pObjJSON = getJSONfieldV(fileName, sysTabSubPartJSON[i], "p-obj");
            typeOBJ pObj = pObjJSON.GetUint();

            dictSysTabComPartAdd(rowId, obj, dataObj, pObj);
        }
        TRACE(TRACE2_SCHEMA_LIST, "SCHEMA LIST: SYS.TABSUBPART$: " << dec << sysTabSubPartJSON.Size());

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
                "\"big-endian\":" << dec << oracleAnalyzer->bigEndian << "," <<
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

        ss << "\"," SCHEMA_ENDL << "\"online-redo\":[";

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
        ss << "]," SCHEMA_ENDL << "\"sys-user\":[";
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
        ss << "]," SCHEMA_ENDL << "\"sys-obj\":[";
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
        ss << "]," SCHEMA_ENDL << "\"sys-col\":[";
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
        ss << "]," SCHEMA_ENDL << "\"sys-ccol\":[";
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
        ss << "]," SCHEMA_ENDL << "\"sys-cdef\":[";
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
        ss << "]," SCHEMA_ENDL << "\"sys-deferredstg\":[";
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
        ss << "]," SCHEMA_ENDL << "\"sys-ecol\":[";
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
        ss << "]," SCHEMA_ENDL << "\"sys-seg\":[";
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
        ss << "]," SCHEMA_ENDL << "\"sys-tab\":[";
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
        ss << "]," SCHEMA_ENDL << "\"sys-tabpart\":[";
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
        ss << "]," SCHEMA_ENDL << "\"sys-tabcompart\":[";
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
        ss << "]," SCHEMA_ENDL << "\"sys-tabsubpart\":[";
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

    void Schema::removeFromDict(OracleObject *object) {
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

    OracleObject *Schema::checkDict(typeOBJ obj, typeDATAOBJ dataObj) {
        auto it = partitionMap.find(obj);
        if (it == partitionMap.end())
            return nullptr;
        return it->second;
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

    void Schema::refreshIndexes(void) {
        //SYS.CCOL$
        if (sysCColKeyTouched) {
            sysCColMapKey.clear();
            for (auto it : sysCColMapRowId) {
                SysCCol *sysCCol = it.second;
                SysCColKey sysCColKey(sysCCol->obj, sysCCol->intCol, sysCCol->con);
                sysCColMapKey[sysCColKey] = sysCCol;
            }
            sysCColKeyTouched = false;
        }

        //SYS.CDEF$

        if (sysCDefKeyTouched) {
            sysCDefMapKey.clear();
            for (auto it : sysCDefMapRowId) {
                SysCDef *sysCDef = it.second;
                SysCDefKey sysCDefKey(sysCDef->obj, sysCDef->con);
                sysCDefMapKey[sysCDefKey] = sysCDef;
            }
            sysCDefKeyTouched = false;
        }

        if (sysCDefConTouched) {
            sysCDefMapCon.clear();
            for (auto it : sysCDefMapRowId) {
                SysCDef *sysCDef = it.second;
                sysCDefMapCon[sysCDef->con] = sysCDef;
            }
            sysCDefConTouched = false;
        }

        //SYS.COL$
        if (sysColKeyTouched) {
            sysColMapKey.clear();
            for (auto it : sysColMapRowId) {
                SysCol *sysCol = it.second;
                SysColKey sysColKey(sysCol->obj, sysCol->intCol);
                sysColMapKey[sysColKey] = sysCol;
            }
            sysColKeyTouched = false;
        }

        if (sysColSegTouched) {
            sysColMapSeg.clear();
            for (auto it : sysColMapRowId) {
                SysCol *sysCol = it.second;
                SysColSeg sysColSeg(sysCol->obj, sysCol->segCol);
                sysColMapSeg[sysColSeg] = sysCol;
            }
            sysColSegTouched = false;
        }

        //SYS.DEFERRED_STG$
        if (sysDeferredStgObjTouched) {
            sysDeferredStgMapObj.clear();
            for (auto it : sysDeferredStgMapRowId) {
                SysDeferredStg *sysDeferredStg = it.second;
                sysDeferredStgMapObj[sysDeferredStg->obj] = sysDeferredStg;
            }
            sysDeferredStgObjTouched = false;
        }

        //SYS.ECOL$
        if (sysEColKeyTouched) {
            sysEColMapKey.clear();
            for (auto it : sysEColMapRowId) {
                SysECol *sysECol = it.second;
                SysEColKey sysEColKey(sysECol->tabObj, sysECol->colNum);
                sysEColMapKey[sysEColKey] = sysECol;
            }
            sysEColKeyTouched = false;
        }

        //SYS.OBJ$
        if (sysObjObjTouched) {
            sysObjMapObj.clear();
            for (auto it : sysObjMapRowId) {
                SysObj *sysObj = it.second;
                sysObjMapObj[sysObj->obj] = sysObj;
            }
            sysObjObjTouched = false;
        }

        //SYS.SEG$
        if (sysSegKeyTouched) {
            sysSegMapKey.clear();
            for (auto it : sysSegMapRowId) {
                SysSeg *sysSeg = it.second;
                SysSegKey sysSegKey(sysSeg->file, sysSeg->block, sysSeg->ts);
                sysSegMapKey[sysSegKey] = sysSeg;
            }
            sysSegKeyTouched = false;
        }

        //SYS.TAB$
        if (sysTabObjTouched) {
            sysTabMapObj.clear();
            for (auto it : sysTabMapRowId) {
                SysTab *sysTab = it.second;
                sysTabMapObj[sysTab->obj] = sysTab;
            }
            sysTabObjTouched = false;
        }

        if (sysTabKeyTouched) {
            sysTabMapKey.clear();
            for (auto it : sysTabMapRowId) {
                SysTab *sysTab = it.second;
                if (sysTab->file != 0 || sysTab->block != 0) {
                    SysTabKey sysTabKey(sysTab->file, sysTab->block, sysTab->ts);
                    sysTabMapKey[sysTabKey] = sysTab;
                }
            }
            sysTabKeyTouched = false;
        }

        //SYS.TABCOMPART$
        if (sysTabComPartKeyTouched) {
            sysTabComPartMapKey.clear();
            for (auto it : sysTabComPartMapRowId) {
                SysTabComPart *sysTabComPart = it.second;
                SysTabComPartKey sysTabComPartKey(sysTabComPart->bo, sysTabComPart->obj);
                sysTabComPartMapKey[sysTabComPartKey] = sysTabComPart;
            }
            sysTabComPartKeyTouched = false;
        }

        if (sysTabPartKeyTouched) {
            sysTabPartMapKey.clear();
            for (auto it : sysTabPartMapRowId) {
                SysTabPart *sysTabPart = it.second;
                SysTabPartKey sysTabPartKey(sysTabPart->bo, sysTabPart->obj);
                sysTabPartMapKey[sysTabPartKey] = sysTabPart;
            }
            sysTabPartKeyTouched = false;
        }

        //SYS.TABSUBPART$
        if (sysTabSubPartKeyTouched) {
            sysTabSubPartMapKey.clear();
            for (auto it : sysTabSubPartMapRowId) {
                SysTabSubPart *sysTabSubPart = it.second;
                SysTabSubPartKey sysTabSubPartKey(sysTabSubPart->pObj, sysTabSubPart->obj);
                sysTabSubPartMapKey[sysTabSubPartKey] = sysTabSubPart;
            }
            sysTabSubPartKeyTouched = false;
        }

        //SYS.USER$
        if (sysUserUserTouched) {
            sysUserMapUser.clear();
            for (auto it : sysUserMapRowId) {
                SysUser *sysUser = it.second;
                sysUserMapUser[sysUser->user] = sysUser;
            }
            sysUserUserTouched = false;
        }

        touched = false;
    }

    void Schema::rebuildMaps(OracleAnalyzer *oracleAnalyzer) {
        for (typeUSER user : usersTouched) {
            for (auto it = objectMap.cbegin(); it != objectMap.cend() ; ) {

                OracleObject *object = it->second;
                if (object->user == user) {
                    removeFromDict(object);
                    objectMap.erase(it++);
                    delete object;
                } else {
                    ++it;
                }
            }
        }
        usersTouched.clear();

        for (typeOBJ obj : objectsTouched) {
            auto objectMapIt = objectMap.find(obj);
            if (objectMapIt != objectMap.end()) {
                OracleObject *object = objectMapIt->second;
                removeFromDict(object);
                delete object;
            }
        }
        objectsTouched.clear();

        for (SchemaElement *element : elements)
            buildMaps(element->owner, element->table, element->keys, element->keysStr, element->options, oracleAnalyzer, false);
    }

    void Schema::buildMaps(string &owner, string &table, vector<string> &keys, string &keysStr, typeOPTIONS options, OracleAnalyzer *oracleAnalyzer, bool output) {
        uint64_t tabCnt = 0;
        regex regexOwner(owner), regexTable(table);

        for (auto itObj : sysObjMapRowId) {
            SysObj *sysObj = itObj.second;

            if (sysObj->isDropped() || !sysObj->isTable())
                continue;

            auto sysUserMapUserIt = sysUserMapUser.find(sysObj->owner);
            if (sysUserMapUserIt == sysUserMapUser.end())
                continue;
            SysUser *sysUser = sysUserMapUserIt->second;

            //table already added with another rule
            if (objectMap.find(sysObj->obj) != objectMap.end()) {
                DEBUG("- skipped: " << sysUser->name << "." << sysObj->name << " (obj: " << dec << sysObj->obj << ") - already added");
                continue;
            }

            auto sysTabMapObjIt = sysTabMapObj.find(sysObj->obj);
            if (sysTabMapObjIt == sysTabMapObj.end()) {
                RUNTIME_FAIL("inconsistent schema, missing SYS.OBJ$ OBJ: " << dec << sysObj->obj);
            }
            SysTab *sysTab = sysTabMapObjIt->second;

            if (!regex_match(sysUser->name, regexOwner) || !regex_match(sysObj->name, regexTable))
                continue;

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
                SysDeferredStg *sysDeferredStg = sysDeferredStgMapObj[sysObj->obj];
                if (sysDeferredStg != nullptr)
                    compressed = sysDeferredStg->isCompressed();
            } else {
                SysSegKey sysSegKey(sysTab->file, sysTab->block, sysTab->ts);
                SysSeg *sysSeg = sysSegMapKey[sysSegKey];
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

            if (sysTab->isPartitioned()) {
                SysTabPartKey sysTabPartKeyFirst(sysObj->obj, 0);
                for (auto itTabPart = sysTabPartMapKey.upper_bound(sysTabPartKeyFirst);
                        itTabPart != sysTabPartMapKey.end() && itTabPart->first.bo == sysObj->obj; ++itTabPart) {

                    SysTabPart *sysTabPart = itTabPart->second;
                    schemaObject->addPartition(sysTabPart->obj, sysTabPart->dataObj);
                }

                SysTabComPartKey sysTabComPartKeyFirst(sysObj->obj, 0);
                for (auto itTabComPart = sysTabComPartMapKey.upper_bound(sysTabComPartKeyFirst);
                        itTabComPart != sysTabComPartMapKey.end() && itTabComPart->first.bo == sysObj->obj; ++itTabComPart) {

                    SysTabSubPartKey sysTabSubPartKeyFirst(itTabComPart->second->obj, 0);
                    for (auto itTabSubPart = sysTabSubPartMapKey.upper_bound(sysTabSubPartKeyFirst);
                            itTabSubPart != sysTabSubPartMapKey.end() && itTabSubPart->first.pObj == itTabComPart->second->obj; ++itTabSubPart) {

                        SysTabSubPart *sysTabSubPart = itTabSubPart->second;
                        schemaObject->addPartition(itTabSubPart->second->obj, itTabSubPart->second->dataObj);
                    }
                }
            }

            if ((oracleAnalyzer->disableChecks & DISABLE_CHECK_SUPPLEMENTAL_LOG) == 0 && options == 0 && !oracleAnalyzer->suppLogDbAll &&
                    !sysUser->isSuppLogAll()) {

                SysCDefKey sysCDefKeyFirst(sysObj->obj, 0);
                for (auto itCDef = sysCDefMapKey.upper_bound(sysCDefKeyFirst);
                        itCDef != sysCDefMapKey.end() && itCDef->first.obj == sysObj->obj;
                        ++itCDef) {
                    SysCDef *sysCDef = itCDef->second;
                    if (sysCDef->isSupplementalLogPK())
                        suppLogTablePrimary = true;
                    else if (sysCDef->isSupplementalLogAll())
                        suppLogTableAll = true;
                }
            }

            SysColSeg sysColSegFirst(sysObj->obj, 0);
            for (auto itCol = sysColMapSeg.upper_bound(sysColSegFirst);
                    itCol != sysColMapSeg.end() && itCol->first.obj == sysObj->obj; ++itCol) {

                SysCol *sysCol = itCol->second;
                if (sysCol->segCol == 0)
                    continue;

                uint64_t charmapId = 0;
                typeCOL numPk = 0, numSup = 0, guardSegNo = -1;

                SysEColKey sysEColKey(sysObj->obj, sysCol->segCol);
                SysECol *sysECol = sysEColMapKey[sysEColKey];
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
                        WARNING("HINT: check in database for name: SELECT NLS_CHARSET_NAME(" << dec << charmapId << ") FROM DUAL;");
                        RUNTIME_FAIL("table " << sysUser->name << "." << sysObj->name << " - unsupported character set id: " << dec << charmapId <<
                                " for column: " << sysCol->name);
                    }
                }

                SysCColKey sysCColKeyFirst(sysObj->obj, sysCol->intCol, 0);
                for (auto itCCol = sysCColMapKey.upper_bound(sysCColKeyFirst);
                        itCCol != sysCColMapKey.end() && itCCol->first.obj == sysObj->obj && itCCol->first.intCol == sysCol->intCol;
                        ++itCCol) {
                    SysCCol *sysCCol = itCCol->second;

                    //count number of PK the column is part of
                    SysCDef* sysCDef = sysCDefMapCon[sysCCol->con];
                    if (sysCDef == nullptr) {
                        RUNTIME_FAIL("inconsistent schema, missing SYS.CDEF$ CON: " << dec << sysCCol->con);
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

                OracleColumn *column = new OracleColumn(sysCol->col, guardSegNo, sysCol->segCol, sysCol->name.c_str(), sysCol->type,
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
            ss << sysUser->name << "." << sysObj->name << " (dataobj: " << dec << sysTab->dataObj << ", obj: " << dec << sysObj->obj << ")";
            if (sysTab->isClustered())
                ss << ", part of cluster";
            if (sysTab->isPartitioned())
                ss << ", partitioned";
            if (sysTab->isDependencies())
                ss << ", row dependencies";
            if (sysTab->isRowMovement())
                ss << ", row movement enabled";

            if ((oracleAnalyzer->disableChecks & DISABLE_CHECK_SUPPLEMENTAL_LOG) == 0 && options == 0) {
                //use default primary key
                if (keys.size() == 0) {
                    if (totalPk == 0)
                        ss << " - primary key missing";
                    else if (!suppLogTablePrimary &&
                            !suppLogTableAll &&
                            !sysUser->isSuppLogPrimary() &&
                            !sysUser->isSuppLogAll() &&
                            !oracleAnalyzer->suppLogDbPrimary && !oracleAnalyzer->suppLogDbAll && supLogColMissing)
                        ss << " - supplemental log missing, try: ALTER TABLE " << sysUser->name << "." << sysObj->name << " ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;";
                //user defined primary key
                } else {
                    if (!suppLogTableAll &&
                            !sysUser->isSuppLogAll() &&
                            !oracleAnalyzer->suppLogDbAll &&
                            supLogColMissing)
                        ss << " - supplemental log missing, try: ALTER TABLE " << sysUser->name << "." << sysObj->name << " ADD SUPPLEMENTAL LOG GROUP GRP" << dec << sysObj->obj << " (" << keysStr << ") ALWAYS;";
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

    SchemaElement* Schema::addElement(const char *owner, const char *table, typeOPTIONS options) {
        SchemaElement *element = new SchemaElement(owner, table, options);
        if (element == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SchemaElement) << " bytes memory (for: schema element)");
        }
        elements.push_back(element);
        return element;
    }

    bool Schema::dictSysCColAdd(const char *rowIdStr, typeCON con, typeCOL intCol, typeOBJ obj, uint64_t spare11, uint64_t spare12) {
        RowId rowId(rowIdStr);
        if (sysCColMapRowId.find(rowId) != sysCColMapRowId.end())
            return false;

        SysCCol *sysCCol = new SysCCol(rowId, con, intCol, obj, spare11, spare12, false);
        if (sysCCol == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysCCol) << " bytes memory (for: SysCCol)");
        }
        sysCColMapRowId[rowId] = sysCCol;
        SysCColKey sysCColKey(obj, intCol, con);
        sysCColMapKey[sysCColKey] = sysCCol;

        return true;
    }

    bool Schema::dictSysCDefAdd(const char *rowIdStr, typeCON con, typeOBJ obj, typeTYPE type) {
        RowId rowId(rowIdStr);
        if (sysCDefMapRowId.find(rowId) != sysCDefMapRowId.end())
            return false;

        SysCDef *sysCDef = new SysCDef(rowId, con, obj, type, false);
        if (sysCDef == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysCDef) << " bytes memory (for: SysCDef)");
        }
        sysCDefMapRowId[rowId] = sysCDef;
        sysCDefMapCon[con] = sysCDef;
        SysCDefKey sysCDefKey(obj, con);
        sysCDefMapKey[sysCDefKey] = sysCDef;

        return true;
    }

    bool Schema::dictSysColAdd(const char *rowIdStr, typeOBJ obj, typeCOL col, typeCOL segCol, typeCOL intCol, const char *name, typeTYPE type, uint64_t length,
            int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, bool null_, uint64_t property1, uint64_t property2) {
        RowId rowId(rowIdStr);
        if (sysColMapRowId.find(rowId) != sysColMapRowId.end())
            return false;

        SysCol *sysCol = new SysCol(rowId, obj, col, segCol, intCol, name, type, length, precision, scale, charsetForm, charsetId,
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

    bool Schema::dictSysDeferredStgAdd(const char *rowIdStr, typeOBJ obj, uint64_t flagsStg1, uint64_t flagsStg2) {
        RowId rowId(rowIdStr);
        if (sysDeferredStgMapRowId.find(rowId) != sysDeferredStgMapRowId.end())
            return false;

        SysDeferredStg *sysDeferredStg = new SysDeferredStg(rowId, obj, flagsStg1, flagsStg2, false);
        if (sysDeferredStg == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysDeferredStg) << " bytes memory (for: SysDeferredStg)");
        }
        sysDeferredStgMapRowId[rowId] = sysDeferredStg;
        sysDeferredStgMapObj[obj] = sysDeferredStg;

        return true;
    }

    bool Schema::dictSysEColAdd(const char *rowIdStr, typeOBJ tabObj, typeCOL colNum, typeCOL guardId) {
        RowId rowId(rowIdStr);
        if (sysEColMapRowId.find(rowId) != sysEColMapRowId.end())
            return false;

        SysECol *sysECol = new SysECol(rowId, tabObj, colNum, guardId, false);
        if (sysECol == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysECol) << " bytes memory (for: SysECol)");
        }
        sysEColMapRowId[rowId] = sysECol;
        SysEColKey sysEColKey(tabObj, colNum);
        sysEColMapKey[sysEColKey] = sysECol;

        return true;
    }

    bool Schema::dictSysObjAdd(const char *rowIdStr, typeUSER owner, typeOBJ obj, typeDATAOBJ dataObj, typeTYPE type, const char *name,
            uint64_t flags1, uint64_t flags2) {
        RowId rowId(rowIdStr);
        if (sysObjMapRowId.find(rowId) != sysObjMapRowId.end())
            return false;

        SysObj *sysObj = new SysObj(rowId, owner, obj, dataObj, type, name, flags1, flags2, false);
        if (sysObj == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysObj) << " bytes memory (for: SysObj)");
        }
        sysObjMapRowId[rowId] = sysObj;
        sysObjMapObj[obj] = sysObj;

        return true;
    }

    bool Schema::dictSysSegAdd(const char *rowIdStr, uint32_t file, uint32_t block, uint32_t ts, uint64_t spare11, uint64_t spare12) {
        RowId rowId(rowIdStr);
        if (sysSegMapRowId.find(rowId) != sysSegMapRowId.end())
            return false;

        SysSeg *sysSeg = new SysSeg(rowId, file, block, ts, spare11, spare12, false);
        if (sysSeg == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysSeg) << " bytes memory (for: SysSeg)");
        }
        sysSegMapRowId[rowId] = sysSeg;
        SysSegKey sysSegKey(file, block, ts);
        sysSegMapKey[sysSegKey] = sysSeg;

        return true;
    }

    bool Schema::dictSysTabAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, uint32_t ts, uint32_t file, uint32_t block,
            typeCOL cluCols, uint64_t flags1, uint64_t flags2, uint64_t property1, uint64_t property2) {
        RowId rowId(rowIdStr);
        if (sysTabMapRowId.find(rowId) != sysTabMapRowId.end())
            return false;

        SysTab *sysTab = new SysTab(rowId, obj, dataObj, ts, file, block, cluCols, flags1, flags2, property1, property2, false);
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

    bool Schema::dictSysTabComPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo) {
        RowId rowId(rowIdStr);
        if (sysTabComPartMapRowId.find(rowId) != sysTabComPartMapRowId.end())
            return false;

        SysTabComPart *sysTabComPart = new SysTabComPart(rowId, obj, dataObj, bo, false);
        if (sysTabComPart == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysTabComPart) << " bytes memory (for: SysTabComPart)");
        }
        sysTabComPartMapRowId[rowId] = sysTabComPart;
        SysTabComPartKey sysTabComPartKey(bo, obj);
        sysTabComPartMapKey[sysTabComPartKey] = sysTabComPart;

        return true;
    }

    bool Schema::dictSysTabPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo) {
        RowId rowId(rowIdStr);
        if (sysTabPartMapRowId.find(rowId) != sysTabPartMapRowId.end())
            return false;

        SysTabPart *sysTabPart = new SysTabPart(rowId, obj, dataObj, bo, false);
        if (sysTabPart == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysTabPart) << " bytes memory (for: SysTabPart)");
        }
        sysTabPartMapRowId[rowId] = sysTabPart;
        SysTabPartKey sysTabPartKey(bo, obj);
        sysTabPartMapKey[sysTabPartKey] = sysTabPart;

        return true;
    }

    bool Schema::dictSysTabSubPartAdd(const char *rowIdStr, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ pObj) {
        RowId rowId(rowIdStr);
        if (sysTabSubPartMapRowId.find(rowId) != sysTabSubPartMapRowId.end())
            return false;

        SysTabSubPart *sysTabSubPart = new SysTabSubPart(rowId, obj, dataObj, pObj, false);
        if (sysTabSubPart == nullptr) {
            RUNTIME_FAIL("couldn't allocate " << dec << sizeof(class SysTabSubPart) << " bytes memory (for: SysTabSubPart)");
        }
        sysTabSubPartMapRowId[rowId] = sysTabSubPart;
        SysTabSubPartKey sysTabSubPartKey(pObj, obj);
        sysTabSubPartMapKey[sysTabSubPartKey] = sysTabSubPart;

        return true;
    }

    bool Schema::dictSysUserAdd(const char *rowIdStr, typeUSER user, const char *name, uint64_t spare11, uint64_t spare12, bool trackDDL) {
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

        sysUser = new SysUser(rowId, user, name, spare11, spare12, trackDDL, false);
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

    void Schema::touchUser(typeUSER user) {
        if (user == 0)
            return;

        if (usersTouched.find(user) == usersTouched.end())
            usersTouched.insert(user);
    }
}
