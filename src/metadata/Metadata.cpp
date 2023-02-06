/* Base class for handling of metadata
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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
#include <unistd.h>

#include "../common/Ctx.h"
#include "../common/DataException.h"
#include "../common/OracleIncarnation.h"
#include "../common/RuntimeException.h"
#include "../common/SysCCol.h"
#include "../common/SysCDef.h"
#include "../common/SysCol.h"
#include "../common/SysDeferredStg.h"
#include "../common/SysECol.h"
#include "../common/SysObj.h"
#include "../common/SysTab.h"
#include "../common/SysTabComPart.h"
#include "../common/SysTabPart.h"
#include "../common/SysTabSubPart.h"
#include "../common/SysUser.h"
#include "../common/typeRowId.h"
#include "../locales/CharacterSet.h"
#include "../locales/Locales.h"
#include "../state/StateDisk.h"
#include "RedoLog.h"
#include "Metadata.h"
#include "Schema.h"
#include "SchemaElement.h"
#include "SerializerJson.h"

namespace OpenLogReplicator {
    Metadata::Metadata(Ctx* newCtx, Locales* newLocales, const char* newDatabase, typeConId newConId, typeScn newStartScn, typeSeq newStartSequence,
                       const char* newStartTime, int64_t newStartTimeRel) :
            schema(new Schema(newCtx, newLocales)),
            ctx(newCtx),
            locales(newLocales),
            state(nullptr),
            stateDisk(nullptr),
            serializer(nullptr),
            status(METADATA_STATUS_INITIALIZE),
            database(newDatabase),
            startScn(newStartScn),
            startSequence(newStartSequence),
            startTime(newStartTime),
            startTimeRel(newStartTimeRel),
            onlineData(false),
            suppLogDbPrimary(false),
            suppLogDbAll(false),
            logArchiveFormatCustom(false),
            conId(newConId),
            logArchiveFormat("o1_mf_%t_%s_%h_.arc"),
            defaultCharacterMapId(0),
            defaultCharacterNcharMapId(0),
            sequence(ZERO_SEQ),
            offset(0),
            resetlogs(0),
            activation(0),
            checkpoints(0),
            firstDataScn(ZERO_SCN),
            firstSchemaScn(ZERO_SCN),
            checkpointScn(ZERO_SCN),
            firstScn(ZERO_SCN),
            nextScn(ZERO_SCN),
            checkpointTime(0),
            checkpointOffset(0),
            checkpointBytes(0),
            minSequence(ZERO_SEQ),
            minOffset(0),
            minXid(),
            schemaInterval(0),
            lastCheckpointScn(ZERO_SCN),
            lastSequence(ZERO_SEQ),
            lastCheckpointOffset(0),
            lastCheckpointTime(),
            lastCheckpointBytes(0),
            oracleIncarnationCurrent(nullptr) {
    }

    Metadata::~Metadata() {
        if (schema != nullptr) {
            delete schema;
            schema = nullptr;
        }

        if (serializer != nullptr) {
            delete serializer;
            serializer = nullptr;
        }

        if (state != nullptr) {
            delete state;
            state = nullptr;
        }
        if (stateDisk != nullptr) {
            delete stateDisk;
            stateDisk = nullptr;
        }

        purgeRedoLogs();

        for (SchemaElement* element : schemaElements)
            delete element;
        schemaElements.clear();
        users.clear();

        for (OracleIncarnation* oi : oracleIncarnations)
            delete oi;
        oracleIncarnations.clear();
        oracleIncarnationCurrent = nullptr;
    }

    void Metadata::setNlsCharset(const std::string& nlsCharset, const std::string& nlsNcharCharset) {
        for (auto characterMapIt: locales->characterMap) {
            if (nlsCharset == characterMapIt.second->name) {
                defaultCharacterMapId = characterMapIt.first;
                break;
            }
        }

        if (defaultCharacterMapId == 0)
            throw DataException("unsupported NLS_CHARACTERSET value");

        for (auto characterMapIt: locales->characterMap) {
            if (strcmp(nlsNcharCharset.c_str(), characterMapIt.second->name) == 0) {
                defaultCharacterNcharMapId = characterMapIt.first;
                break;
            }
        }

        if (defaultCharacterNcharMapId == 0)
            throw DataException("unsupported NLS_NCHAR_CHARACTERSET value");
    }

    void Metadata::purgeRedoLogs() {
        for (RedoLog* redoLog : redoLogs)
            delete redoLog;
        redoLogs.clear();
    }

    void Metadata::setResetlogs(typeResetlogs newResetlogs) {
        std::unique_lock<std::mutex> lck(mtx);
        resetlogs = newResetlogs;
        activation = 0;
    }

    void Metadata::setActivation(typeActivation newActivation) {
        std::unique_lock<std::mutex> lck(mtx);
        activation = newActivation;
    }

    void Metadata::setSeqOffset(typeSeq newSequence, uint64_t newOffset) {
        std::unique_lock<std::mutex> lck(mtx);
        sequence = newSequence;
        offset = newOffset;
    }

    void Metadata::initializeDisk(const char* path) {
        state = new StateDisk(path);
        stateDisk = new StateDisk("scripts");
        serializer = new SerializerJson();
    }

    bool Metadata::stateRead(const std::string& name, uint64_t maxSize, std::string& in) {
        try {
            return state->read(name, maxSize, in);
        } catch (DataException& ex) {
            WARNING(ex.msg);
        }
        return false;
    }

    bool Metadata::stateDiskRead(const std::string& name, uint64_t maxSize, std::string& in) {
        try {
            return stateDisk->read(name, maxSize, in);
        } catch (DataException& ex) {
            WARNING(ex.msg);
        }
        return false;
    }

    bool Metadata::stateWrite(const std::string& name, std::ostringstream& out) {
        try {
            state->write(name, out);
            return true;
        } catch (DataException& ex) {
            WARNING(ex.msg);
        }
        return false;
    }

    bool Metadata::stateDrop(const std::string& name) {
        try {
            state->drop(name);
            return true;
        } catch (DataException& ex) {
            WARNING(ex.msg);
        }
        return false;
    }

    SchemaElement* Metadata::addElement(const char* owner, const char* table, typeOptions options) {
        if (!Ctx::checkNameCase(owner))
            throw DataException("owner '" + std::string(owner) + "' contains lower case characters, value must be upper case");
        if (!Ctx::checkNameCase(table))
            throw DataException("table '" + std::string(table) + "' contains lower case characters, value must be upper case");
        auto element = new SchemaElement(owner, table, options);
        schemaElements.push_back(element);
        return element;
    }

    void Metadata::waitForReplication() {
        std::unique_lock<std::mutex> lck(mtx);
        if (status == METADATA_STATUS_INITIALIZE)
            condStartedReplication.wait(lck);
    }

    void Metadata::setStatusReplicate() {
        std::unique_lock<std::mutex> lck(mtx);
        status = METADATA_STATUS_REPLICATE;
        condStartedReplication.notify_all();
    }

    void Metadata::wakeUp() {
        std::unique_lock<std::mutex> lck(mtx);
        condStartedReplication.notify_all();
    }

    void Metadata::checkpoint(typeScn newCheckpointScn, typeTime newCheckpointTime, typeSeq newCheckpointSequence, uint64_t newCheckpointOffset,
                              uint64_t newCheckpointBytes, typeSeq newMinSequence, uint64_t newMinOffset, typeXid newMinXid) {
        std::unique_lock<std::mutex> lck(mtx);
        checkpointScn = newCheckpointScn;
        checkpointTime = newCheckpointTime;
        checkpointSequence = newCheckpointSequence;
        checkpointOffset = newCheckpointOffset;
        checkpointBytes += newCheckpointBytes;
        minSequence = newMinSequence;
        minOffset = newMinOffset;
        minXid = newMinXid;
    }

    void Metadata::writeCheckpoint(bool force) {
        std::ostringstream ss;

        {
            std::unique_lock<std::mutex> lck(mtx);
            if (!allowedCheckpoints)
                return;

            // Nothing processed so far
            if (checkpointScn == ZERO_SCN || lastCheckpointScn == checkpointScn)
                return;

            if (lastSequence == sequence && !force &&
                (checkpointTime.getVal() - lastCheckpointTime.getVal() < ctx->checkpointIntervalS) &&
                (checkpointBytes - lastCheckpointBytes) / 1024 / 1024 < ctx->checkpointIntervalMb )
                return;

            if (schema->scn == ZERO_SCN)
                schema->scn = checkpointScn;

            // Schema not changed
            bool storeSchema = true;
            if (schema->refScn != ZERO_SCN && schema->refScn >= schema->scn) {
                if (schemaInterval < ctx->schemaForceInterval) {
                    storeSchema = false;
                    ++schemaInterval;
                } else
                    schemaInterval = 0;
            } else
                schemaInterval = 0;

            serializer->serialize(this, ss, storeSchema);

            lastCheckpointScn = checkpointScn;
            lastSequence = sequence;
            lastCheckpointOffset = checkpointOffset;
            lastCheckpointTime = checkpointTime;
            lastCheckpointBytes = checkpointBytes;
            ++checkpoints;
            checkpointScnList.insert(checkpointScn);
            checkpointSchemaMap[checkpointScn] = storeSchema;
        }

        std::string checkpointName = database + "-chkpt-" + std::to_string(lastCheckpointScn);

        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: writing scn: " << std::dec << lastCheckpointScn << " time: " << lastCheckpointTime.getVal() << " seq: " <<
                lastSequence << " offset: " << lastCheckpointOffset)
        if (!stateWrite(checkpointName, ss)) {
            WARNING("error writing checkpoint to " << checkpointName)
        }
    }

    void Metadata::readCheckpoints() {
        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: searching for previous checkpoint information")

        std::set<std::string> namesList;
        state->list(namesList);

        for (std::string name : namesList) {
            std::string prefix(database + "-chkpt-");
            if (name.length() < prefix.length() || name.substr(0, prefix.length()).compare(prefix) != 0)
                continue;

            std::string scnStr(name.substr(prefix.length(), name.length()));
            typeScn scn;
            try {
                scn = strtoull(scnStr.c_str(), nullptr, 10);
            } catch (std::exception& e) {
                // Ignore other files
                continue;
            }

            TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: found: " << name << " scn: " << std::dec << scn)
            checkpointScnList.insert(scn);
            checkpointSchemaMap[scn] = true;
        }

        if (startScn != ZERO_SCN)
            firstDataScn = startScn;
        else
            firstDataScn = 0;

        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: scn: " << std::dec << firstDataScn)
        if (firstDataScn != ZERO_SCN && firstDataScn != 0) {
            std::set<typeScn>::iterator it = checkpointScnList.end();

            while (it != checkpointScnList.begin()) {
                --it;
                if (*it <= firstDataScn && (sequence == ZERO_SEQ || sequence == 0))
                    readCheckpoint(*it);
            }
        }
    }

    void Metadata::readCheckpoint(typeScn scn) {
        std::set<std::string> msgs;
        INFO("reading metadata for " << database << " for scn: " << scn);
        std::string ss;

        std::string name1(database + "-chkpt-" + std::to_string(scn));
        if (!stateRead(name1, CHECKPOINT_SCHEMA_FILE_MAX_SIZE, ss)) {
            sequence = ZERO_SEQ;
            return;
        }
        if (!serializer->deserialize(this, ss, name1, msgs, true, true)) {
            for (auto msg : msgs) {
                ERROR(msg);
            }
            return;
        }

        for (auto msg : msgs) {
            INFO("- found: " << msg);
        }
        msgs.clear();

        // Schema missing
        if (schema->scn == ZERO_SCN) {
            if (schema->refScn == ZERO_SCN) {
                ERROR("load checkpoint from " << name1 << ": SCN missing")
                return;
            }

            ss.clear();
            std::string name2(database + "-chkpt-" + std::to_string(schema->refScn));
            INFO("reading schema for " << database << " for scn: " << schema->refScn);

            if (!stateRead(name2, CHECKPOINT_SCHEMA_FILE_MAX_SIZE, ss))
                return;

            if (!serializer->deserialize(this, ss, name2, msgs, false, true)) {
                for (auto msg: msgs) {
                    ERROR(msg);
                }
                return;
            }

            for (auto msg: msgs) {
                INFO("- found: " << msg);
            }
        }

        if (schema->scn != ZERO_SCN)
            firstSchemaScn = schema->scn;
    }

    void Metadata::deleteOldCheckpoints() {
        std::set<typeScn> scnToDrop;

        if (FLAG(REDO_FLAGS_CHECKPOINT_KEEP))
            return;

        {
            std::unique_lock<std::mutex> lck(mtx);
            if (!allowedCheckpoints)
                return;

            if (checkpoints < ctx->checkpointKeep)
                return;

            bool foundSchema = false;
            uint64_t num = 0;
            std::set<typeScn>::iterator it = checkpointScnList.end();
            while (it != checkpointScnList.begin()) {
                --it;
                ++num;

                if (num < ctx->checkpointKeep)
                    continue;

                if (!foundSchema) {
                    if (checkpointSchemaMap[*it])
                        foundSchema = true;
                    continue;
                }

                scnToDrop.insert(*it);
            }
        }

        for (auto scn : scnToDrop) {
            std::string checkpointName = database + "-chkpt-" + std::to_string(scn);
            if (!stateDrop(checkpointName))
                break;
        }

        {
            std::unique_lock<std::mutex> lck(mtx);
            for (auto scn: scnToDrop) {
                checkpointScnList.erase(scn);
                checkpointSchemaMap.erase(scn);
            }
        }
    }

    void Metadata::loadAdaptiveSchema() {
        std::string ss;
        std::set<std::string> msgs;
        std::string name("base-" + ctx->versionStr);

        INFO("reading adaptive schema from: " << name + ".json");
        std::string nlsCharset = "AL32UTF8";
        std::string nlsNcharCharset = "AL16UTF16";
        setNlsCharset(nlsCharset, nlsNcharCharset);

        if (!stateDiskRead(name, CHECKPOINT_SCHEMA_FILE_MAX_SIZE, ss)) {
            ERROR("can't read file " << name)
            return;
        }

        if (!serializer->deserialize(this, ss, name, msgs, false, true)) {
            for (auto msg: msgs) {
                ERROR(msg);
            }
            return;
        }

        firstSchemaScn = 0;
        for (auto msg: msgs) {
            INFO("- found: " << msg);
        }
    }
}
