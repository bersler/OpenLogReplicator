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

#include <rapidjson/document.h>

#include "ConfigurationException.h"
#include "OracleAnalyzer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "Reader.h"
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
        INFO("reading schema for " << oracleAnalyzer->database);

        string schemaJSON((istreambuf_iterator<char>(infile)), istreambuf_iterator<char>());
        Document document;

        if (schemaJSON.length() == 0 || document.Parse(schemaJSON.c_str()).HasParseError()) {
            RUNTIME_FAIL("parsing of <database>-schema.json");
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
            CONFIG_FAIL("bad JSON in <database>-schema.json, online-redo should be an array");
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

        const Value& schema = getJSONfieldD(fileName, document, "schema");
        if (!schema.IsArray()) {
            CONFIG_FAIL("bad JSON in <database>-schema.json, schema should be an array");
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
                CONFIG_FAIL("bad JSON in <database>-schema.json, columns should be an array");
            }

            for (SizeType j = 0; j < columns.Size(); ++j) {
                const Value& colNoJSON = getJSONfieldV(fileName, columns[j], "col-no");
                typeCOL colNo = colNoJSON.GetInt();

                const Value& guardSegNoJSON = getJSONfieldV(fileName, columns[j], "guard-seg-no");
                typeCOL guardSegNo = guardSegNoJSON.GetInt();

                const Value& segColNoJSON = getJSONfieldV(fileName, columns[j], "seg-col-no");
                typeCOL segColNo = segColNoJSON.GetInt();
                if (segColNo > 1000) {
                    CONFIG_FAIL("bad JSON in <database>-schema.json, invalid seg-col-no value");
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
                    CONFIG_FAIL("bad JSON in <database>-schema.json, partitions should be an array");
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
        INFO("writing schema information for " << oracleAnalyzer->database);

        string fileName = oracleAnalyzer->database + "-schema.json";
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
        ss << "]," << "\"schema\":[";

        hasPrev = false;
        for (auto it : objectMap) {
            OracleObject *objectTmp = it.second;

            if (hasPrev)
                ss << ",";
            else
                hasPrev = true;

            ss << "{\"obj\":" << dec << objectTmp->obj << "," <<
                    "\"dataObj\":" << dec << objectTmp->dataObj << "," <<
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
                    typeOBJ partitionObjn = objectTmp->partitions[i] >> 32;
                    typeOBJ partitionObjd = objectTmp->partitions[i] & 0xFFFFFFFF;
                    ss << "{\"obj\":" << dec << partitionObjn << "," <<
                            "\"dataObj\":" << dec << partitionObjd << "}";
                }
                ss << "]";
            }
            ss << "}";
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
}
