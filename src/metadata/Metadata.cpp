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

#include "../common/Ctx.h"
#include "../common/OracleIncarnation.h"
#include "../common/typeRowId.h"
#include "../common/exception/ConfigurationException.h"
#include "../common/exception/RuntimeException.h"
#include "../common/table/SysCCol.h"
#include "../common/table/SysCDef.h"
#include "../common/table/SysCol.h"
#include "../common/table/SysDeferredStg.h"
#include "../common/table/SysECol.h"
#include "../common/table/SysObj.h"
#include "../common/table/SysTab.h"
#include "../common/table/SysTabComPart.h"
#include "../common/table/SysTabPart.h"
#include "../common/table/SysTabSubPart.h"
#include "../common/table/SysUser.h"
#include "../locales/CharacterSet.h"
#include "../locales/Locales.h"
#include "../state/StateDisk.h"
#include "RedoLog.h"
#include "Metadata.h"
#include "Schema.h"
#include "SchemaElement.h"
#include "Serializer.h"

namespace OpenLogReplicator {
    Metadata::Metadata(Ctx* newCtx, Locales* newLocales, const char* newDatabase, typeConId newConId, typeScn newStartScn, typeSeq newStartSequence,
                       const char* newStartTime, uint64_t newStartTimeRel) :
            schema(new Schema(newCtx, newLocales)),
            ctx(newCtx),
            locales(newLocales),
            state(nullptr),
            stateDisk(nullptr),
            serializer(nullptr),
            status(METADATA_STATUS_READY),
            database(newDatabase),
            startScn(newStartScn),
            startSequence(newStartSequence),
            startTime(newStartTime),
            startTimeRel(newStartTimeRel),
            onlineData(false),
            suppLogDbPrimary(false),
            suppLogDbAll(false),
            logArchiveFormatCustom(false),
            allowedCheckpoints(false),
            bootFailsafe(false),
            conId(newConId),
            logArchiveFormat("o1_mf_%t_%s_%h_.arc"),
            defaultCharacterMapId(0),
            defaultCharacterNcharMapId(0),
            firstDataScn(ZERO_SCN),
            firstSchemaScn(ZERO_SCN),
            resetlogs(0),
            oracleIncarnationCurrent(nullptr),
            activation(0),
            sequence(ZERO_SEQ),
            lastSequence(ZERO_SEQ),
            offset(0),
            firstScn(ZERO_SCN),
            nextScn(ZERO_SCN),
            clientScn(ZERO_SCN),
            clientIdx(0),
            checkpoints(0),
            checkpointScn(ZERO_SCN),
            lastCheckpointScn(ZERO_SCN),
            checkpointTime(0),
            lastCheckpointTime(),
            checkpointSequence(ZERO_SEQ),
            checkpointOffset(0),
            lastCheckpointOffset(0),
            checkpointBytes(0),
            lastCheckpointBytes(0),
            minSequence(ZERO_SEQ),
            minOffset(0),
            minXid(),
            schemaInterval(0) {
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

        for (SchemaElement* element : newSchemaElements)
            delete element;
        newSchemaElements.clear();

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
            throw RuntimeException(10042, "unsupported NLS_CHARACTERSET value: " + nlsCharset);

        for (auto characterMapIt: locales->characterMap) {
            if (strcmp(nlsNcharCharset.c_str(), characterMapIt.second->name) == 0) {
                defaultCharacterNcharMapId = characterMapIt.first;
                break;
            }
        }

        if (defaultCharacterNcharMapId == 0)
            throw RuntimeException(10046, "unsupported NLS_NCHAR_CHARACTERSET value: " + nlsNcharCharset);
    }

    void Metadata::purgeRedoLogs() {
        for (RedoLog* redoLog : redoLogs)
            delete redoLog;
        redoLogs.clear();
    }

    void Metadata::setResetlogs(typeResetlogs newResetlogs) {
        resetlogs = newResetlogs;
        activation = 0;
    }

    void Metadata::setActivation(typeActivation newActivation) {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        activation = newActivation;
    }

    void Metadata::setFirstNextScn(typeScn newFirstScn, typeScn newNextScn) {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        firstScn = newFirstScn;
        nextScn = newNextScn;
    }

    void Metadata::setNextSequence() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        ++sequence;
    }

    void Metadata::setSeqOffset(typeSeq newSequence, uint64_t newOffset) {
        if (ctx->trace & TRACE_CHECKPOINT)
            ctx->logTrace(TRACE_CHECKPOINT, "setting sequence to: " + std::to_string(newSequence) + ", offset: " +
                          std::to_string(newOffset));

        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        sequence = newSequence;
        offset = newOffset;
    }

    bool Metadata::stateRead(const std::string& name, uint64_t maxSize, std::string& in) {
        try {
            return state->read(name, maxSize, in);
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
        }
        return false;
    }

    bool Metadata::stateDiskRead(const std::string& name, uint64_t maxSize, std::string& in) {
        try {
            return stateDisk->read(name, maxSize, in);
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
        }
        return false;
    }

    bool Metadata::stateWrite(const std::string& name, typeScn scn, std::ostringstream& out) {
        try {
            state->write(name, scn, out);
            return true;
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
        }
        return false;
    }

    bool Metadata::stateDrop(const std::string& name) {
        try {
            state->drop(name);
            return true;
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
        }
        return false;
    }

    SchemaElement* Metadata::addElement(const char* owner, const char* table, typeOptions options) {
        if (!Ctx::checkNameCase(owner))
            throw ConfigurationException(30003, "owner '" + std::string(owner) +
                                         "' contains lower case characters, value must be upper case");
        if (!Ctx::checkNameCase(table))
            throw ConfigurationException(30004, "table '" + std::string(table) +
                                         "' contains lower case characters, value must be upper case");
        auto element = new SchemaElement(owner, table, options);
        newSchemaElements.push_back(element);
        return element;
    }

    void Metadata::resetElements() {
        for (SchemaElement* element : newSchemaElements)
            delete element;
        newSchemaElements.clear();

        addElement("SYS", "CCOL\\$", OPTIONS_SYSTEM_TABLE | OPTIONS_SCHEMA_TABLE);
        addElement("SYS", "CDEF\\$", OPTIONS_SYSTEM_TABLE | OPTIONS_SCHEMA_TABLE);
        addElement("SYS", "COL\\$", OPTIONS_SYSTEM_TABLE | OPTIONS_SCHEMA_TABLE);
        addElement("SYS", "DEFERRED_STG\\$", OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "ECOL\\$", OPTIONS_SYSTEM_TABLE | OPTIONS_SCHEMA_TABLE);
        addElement("SYS", "LOB\\$", OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "LOBCOMPPART\\$", OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "LOBFRAG\\$", OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "OBJ\\$", OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "TAB\\$", OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "TABPART\\$", OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "TABCOMPART\\$", OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "TABSUBPART\\$", OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "TS\\$", OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "USER\\$", OPTIONS_SYSTEM_TABLE);
        addElement("XDB", "XDB\\$TTSET", OPTIONS_SYSTEM_TABLE);
        addElement("XDB", "X\\$NM.*", OPTIONS_SYSTEM_TABLE);
        addElement("XDB", "X\\$PT.*", OPTIONS_SYSTEM_TABLE);
        addElement("XDB", "X\\$QN.*", OPTIONS_SYSTEM_TABLE);
    }

    void Metadata::commitElements() {
        std::unique_lock<std::mutex> lck(mtxSchema);

        for (SchemaElement* element : schemaElements)
            delete element;
        schemaElements.clear();

        for (SchemaElement* element : newSchemaElements)
            schemaElements.push_back(element);
        newSchemaElements.clear();
    }

    void Metadata::waitForWriter() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        if (status == METADATA_STATUS_READY) {
            if (ctx->trace & TRACE_SLEEP)
                ctx->logTrace(TRACE_SLEEP, "Metadata:waitForWriter");
            condReplicator.wait(lck);
        }
    }

    void Metadata::waitForReplicator() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        if (status == METADATA_STATUS_START) {
            if (ctx->trace & TRACE_SLEEP)
                ctx->logTrace(TRACE_SLEEP, "Metadata:waitForReplicator");
            condWriter.wait(lck);
        }
    }

    void Metadata::setStatusReady() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        status = METADATA_STATUS_READY;
        firstDataScn = ZERO_SCN;
        firstSchemaScn = ZERO_SCN;
        checkpointScn = ZERO_SCN;
        schema->scn = ZERO_SCN;
        condWriter.notify_all();
    }

    void Metadata::setStatusStart() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        status = METADATA_STATUS_START;
        condReplicator.notify_all();
    }

    void Metadata::setStatusReplicate() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        status = METADATA_STATUS_REPLICATE;
        condReplicator.notify_all();
        condWriter.notify_all();
    }

    void Metadata::wakeUp() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        condReplicator.notify_all();
        condWriter.notify_all();
    }

    void Metadata::checkpoint(typeScn newCheckpointScn, typeTime newCheckpointTime, typeSeq newCheckpointSequence, uint64_t newCheckpointOffset,
                              uint64_t newCheckpointBytes, typeSeq newMinSequence, uint64_t newMinOffset, typeXid newMinXid) {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

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
            std::unique_lock<std::mutex> lck(mtxCheckpoint);
            if (!allowedCheckpoints)
                return;

            // Nothing processed so far
            if (checkpointScn == ZERO_SCN || lastCheckpointScn == checkpointScn)
                return;

            if (lastSequence == sequence && !force &&
                (checkpointTime.getVal() - lastCheckpointTime.getVal() < ctx->checkpointIntervalS) &&
                (checkpointBytes - lastCheckpointBytes) / 1024 / 1024 < ctx->checkpointIntervalMb )
                return;

            // Schema did not change
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
            checkpointSchemaMap.insert_or_assign(checkpointScn, storeSchema);
        }

        std::string checkpointName = database + "-chkpt-" + std::to_string(lastCheckpointScn);

        if (ctx->trace & TRACE_CHECKPOINT)
            ctx->logTrace(TRACE_CHECKPOINT, "write scn: " + std::to_string(lastCheckpointScn) + " time: " +
                          std::to_string(lastCheckpointTime.getVal()) + " seq: " + std::to_string(lastSequence) + " offset: " +
                          std::to_string(lastCheckpointOffset) + " name: " + checkpointName);

        if (!stateWrite(checkpointName, lastCheckpointScn, ss))
            ctx->warning(60018, "file: " + checkpointName + " - couldn't write checkpoint");
    }

    void Metadata::readCheckpoints() {
        if (ctx->trace & TRACE_CHECKPOINT)
            ctx->logTrace(TRACE_CHECKPOINT, "searching for previous checkpoint information");

        std::set<std::string> namesList;
        state->list(namesList);

        for (const std::string& name : namesList) {
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

            if (ctx->trace & TRACE_CHECKPOINT)
                ctx->logTrace(TRACE_CHECKPOINT, "found: " + name + " scn: " + std::to_string(scn));

            checkpointScnList.insert(scn);
            checkpointSchemaMap.insert_or_assign(scn, true);
        }

        if (startScn != ZERO_SCN)
            firstDataScn = startScn;
        else
            firstDataScn = 0;

        if (ctx->trace & TRACE_CHECKPOINT)
            ctx->logTrace(TRACE_CHECKPOINT, "scn: " + std::to_string(firstDataScn));

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
        std::list<std::string> msgs;
        ctx->info(0, "reading metadata for " + database + " for scn: " + std::to_string(scn));
        std::string ss;

        std::string name1(database + "-chkpt-" + std::to_string(scn));
        if (!stateRead(name1, CHECKPOINT_SCHEMA_FILE_MAX_SIZE, ss)) {
            if (ctx->trace & TRACE_CHECKPOINT)
                ctx->logTrace(TRACE_CHECKPOINT, "no checkpoint file found, setting unknown sequence");

            sequence = ZERO_SEQ;
            return;
        }
        if (!serializer->deserialize(this, ss, name1, msgs, true, true)) {
            for (const auto& msg : msgs) {
                ctx->info(0, msg);
            }
            return;
        }

        for (const auto& msg : msgs) {
            ctx->info(0, "- found: " + msg);
        }
        msgs.clear();

        // Schema missing
        if (schema->scn == ZERO_SCN) {
            if (schema->refScn == ZERO_SCN) {
                ctx->warning(60019, "file: " + name1 + " - load checkpoint failed, reference SCN missing");
                return;
            }

            ss.clear();
            std::string name2(database + "-chkpt-" + std::to_string(schema->refScn));
            ctx->info(0, "reading schema for " + database + " for scn: " + std::to_string(schema->refScn));

            if (!stateRead(name2, CHECKPOINT_SCHEMA_FILE_MAX_SIZE, ss))
                return;

            if (!serializer->deserialize(this, ss, name2, msgs, false, true)) {
                for (const auto& msg: msgs) {
                    ctx->info(0, msg);
                }
                return;
            }

            for (const auto& msg: msgs) {
                ctx->info(0, "- found: " + msg);
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
            std::unique_lock<std::mutex> lck(mtxCheckpoint);

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
            std::unique_lock<std::mutex> lck(mtxCheckpoint);

            for (auto scn: scnToDrop) {
                checkpointScnList.erase(scn);
                checkpointSchemaMap.erase(scn);
            }
        }
    }

    void Metadata::loadAdaptiveSchema() {
        std::string ss;
        std::list<std::string> msgs;
        std::string name("base-" + ctx->versionStr);

        ctx->info(0, "reading adaptive schema from: " + name + ".json");
        std::string nlsCharset = "AL32UTF8";
        std::string nlsNcharCharset = "AL16UTF16";
        setNlsCharset(nlsCharset, nlsNcharCharset);

        if (!stateDiskRead(name, CHECKPOINT_SCHEMA_FILE_MAX_SIZE, ss)) {
            ctx->warning(60020, "file: " + name + " - load adaptive schema failed");
            return;
        }

        if (!serializer->deserialize(this, ss, name, msgs, false, true)) {
            for (const auto& msg: msgs) {
                ctx->info(0, msg);
            }
            return;
        }

        firstSchemaScn = 0;
        for (const auto& msg: msgs) {
            ctx->info(0, "- found: " + msg);
        }
    }

    void Metadata::allowCheckpoints() {
        if (ctx->trace & TRACE_CHECKPOINT)
            ctx->logTrace(TRACE_CHECKPOINT, "allowing checkpoints");

        std::unique_lock<std::mutex> lck(mtxCheckpoint);
        allowedCheckpoints = true;
    }

    bool Metadata::isNewData(typeScn scn, typeIdx idx) {
        if (clientScn == ZERO_SCN)
            return true;

        if (clientScn < scn)
            return true;

        if (clientScn == scn && clientIdx < idx)
            return true;

        return false;
    }
}
