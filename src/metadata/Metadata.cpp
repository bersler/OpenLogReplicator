/* Base class for handling of metadata
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

#include <vector>

#include "../common/Ctx.h"
#include "../common/OracleIncarnation.h"
#include "../common/typeRowId.h"
#include "../common/exception/ConfigurationException.h"
#include "../common/exception/RuntimeException.h"
#include "../common/OracleTable.h"
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
            status(STATUS_READY),
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
            dbTimezone(0),
            logArchiveFormat("o1_mf_%t_%s_%h_.arc"),
            defaultCharacterMapId(0),
            defaultCharacterNcharMapId(0),
            firstDataScn(Ctx::ZERO_SCN),
            firstSchemaScn(Ctx::ZERO_SCN),
            resetlogs(0),
            oracleIncarnationCurrent(nullptr),
            activation(0),
            sequence(Ctx::ZERO_SEQ),
            lastSequence(Ctx::ZERO_SEQ),
            offset(0),
            firstScn(Ctx::ZERO_SCN),
            nextScn(Ctx::ZERO_SCN),
            clientScn(Ctx::ZERO_SCN),
            clientIdx(0),
            checkpoints(0),
            checkpointScn(Ctx::ZERO_SCN),
            lastCheckpointScn(Ctx::ZERO_SCN),
            checkpointTime(0),
            lastCheckpointTime(),
            checkpointSequence(Ctx::ZERO_SEQ),
            checkpointOffset(0),
            lastCheckpointOffset(0),
            checkpointBytes(0),
            lastCheckpointBytes(0),
            minSequence(Ctx::ZERO_SEQ),
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

        for (SchemaElement* element: schemaElements)
            delete element;
        schemaElements.clear();

        for (SchemaElement* element: newSchemaElements)
            delete element;
        newSchemaElements.clear();

        users.clear();

        for (OracleIncarnation* oi: oracleIncarnations)
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

        if (unlikely(defaultCharacterMapId == 0))
            throw RuntimeException(10042, "unsupported NLS_CHARACTERSET value: " + nlsCharset);

        for (auto characterMapIt: locales->characterMap) {
            if (strcmp(nlsNcharCharset.c_str(), characterMapIt.second->name) == 0) {
                defaultCharacterNcharMapId = characterMapIt.first;
                break;
            }
        }

        if (unlikely(defaultCharacterNcharMapId == 0))
            throw RuntimeException(10046, "unsupported NLS_NCHAR_CHARACTERSET value: " + nlsNcharCharset);
    }

    void Metadata::purgeRedoLogs() {
        for (RedoLog* redoLog: redoLogs)
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
        if (unlikely(ctx->trace & Ctx::TRACE_CHECKPOINT))
            ctx->logTrace(Ctx::TRACE_CHECKPOINT, "setting sequence to: " + std::to_string(newSequence) + ", offset: " +
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

    bool Metadata::stateWrite(const std::string& name, typeScn scn, const std::ostringstream& out) {
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
        if (unlikely(!Ctx::checkNameCase(owner)))
            throw ConfigurationException(30003, "owner '" + std::string(owner) +
                                                "' contains lower case characters, value must be upper case");
        if (unlikely(!Ctx::checkNameCase(table)))
            throw ConfigurationException(30004, "table '" + std::string(table) +
                                                "' contains lower case characters, value must be upper case");
        auto element = new SchemaElement(owner, table, options);
        newSchemaElements.push_back(element);
        return element;
    }

    void Metadata::resetElements() {
        for (SchemaElement* element: newSchemaElements)
            delete element;
        newSchemaElements.clear();

        addElement("SYS", "CCOL\\$", OracleTable::OPTIONS_SYSTEM_TABLE | OracleTable::OPTIONS_SCHEMA_TABLE);
        addElement("SYS", "CDEF\\$", OracleTable::OPTIONS_SYSTEM_TABLE | OracleTable::OPTIONS_SCHEMA_TABLE);
        addElement("SYS", "COL\\$", OracleTable::OPTIONS_SYSTEM_TABLE | OracleTable::OPTIONS_SCHEMA_TABLE);
        addElement("SYS", "DEFERRED_STG\\$", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "ECOL\\$", OracleTable::OPTIONS_SYSTEM_TABLE | OracleTable::OPTIONS_SCHEMA_TABLE);
        addElement("SYS", "LOB\\$", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "LOBCOMPPART\\$", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "LOBFRAG\\$", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "OBJ\\$", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "TAB\\$", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "TABPART\\$", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "TABCOMPART\\$", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "TABSUBPART\\$", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "TS\\$", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("SYS", "USER\\$", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("XDB", "XDB\\$TTSET", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("XDB", "X\\$NM.*", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("XDB", "X\\$PT.*", OracleTable::OPTIONS_SYSTEM_TABLE);
        addElement("XDB", "X\\$QN.*", OracleTable::OPTIONS_SYSTEM_TABLE);
    }

    void Metadata::commitElements() {
        std::unique_lock<std::mutex> lck(mtxSchema);

        for (SchemaElement* element: schemaElements)
            delete element;
        schemaElements.clear();

        for (SchemaElement* element: newSchemaElements)
            schemaElements.push_back(element);
        newSchemaElements.clear();
    }

    void Metadata::waitForWriter() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        if (status == STATUS_READY) {
            if (unlikely(ctx->trace & Ctx::TRACE_SLEEP))
                ctx->logTrace(Ctx::TRACE_SLEEP, "Metadata:waitForWriter");
            condReplicator.wait(lck);
        }
    }

    void Metadata::waitForReplicator() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        if (status == STATUS_START) {
            if (unlikely(ctx->trace & Ctx::TRACE_SLEEP))
                ctx->logTrace(Ctx::TRACE_SLEEP, "Metadata:waitForReplicator");
            condWriter.wait(lck);
        }
    }

    void Metadata::setStatusReady() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        status = STATUS_READY;
        firstDataScn = Ctx::ZERO_SCN;
        firstSchemaScn = Ctx::ZERO_SCN;
        checkpointScn = Ctx::ZERO_SCN;
        schema->scn = Ctx::ZERO_SCN;
        condWriter.notify_all();
    }

    void Metadata::setStatusStart() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        status = STATUS_START;
        condReplicator.notify_all();
    }

    void Metadata::setStatusReplicate() {
        std::unique_lock<std::mutex> lck(mtxCheckpoint);

        status = STATUS_REPLICATE;
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
            if (checkpointScn == Ctx::ZERO_SCN || lastCheckpointScn == checkpointScn || checkpointSequence == Ctx::ZERO_SEQ)
                return;

            if (lastSequence == sequence && !force &&
                (static_cast<uint64_t>(checkpointTime.toEpoch(ctx->hostTimezone) - lastCheckpointTime.toEpoch(ctx->hostTimezone)) < ctx->checkpointIntervalS) &&
                (checkpointBytes - lastCheckpointBytes) / 1024 / 1024 < ctx->checkpointIntervalMb)
                return;

            // Schema did not change
            bool storeSchema = true;
            if (schema->refScn != Ctx::ZERO_SCN && schema->refScn >= schema->scn) {
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

        if (unlikely(ctx->trace & Ctx::TRACE_CHECKPOINT))
            ctx->logTrace(Ctx::TRACE_CHECKPOINT, "write scn: " + std::to_string(lastCheckpointScn) + " time: " +
                                                 std::to_string(lastCheckpointTime.getVal()) + " seq: " + std::to_string(lastSequence) + " offset: " +
                                                 std::to_string(lastCheckpointOffset) + " name: " + checkpointName);

        if (!stateWrite(checkpointName, lastCheckpointScn, ss))
            ctx->warning(60018, "file: " + checkpointName + " - couldn't write checkpoint");
    }

    void Metadata::readCheckpoints() {
        if (unlikely(ctx->trace & Ctx::TRACE_CHECKPOINT))
            ctx->logTrace(Ctx::TRACE_CHECKPOINT, "searching for previous checkpoint information");

        std::set<std::string> namesList;
        state->list(namesList);

        for (const std::string& name: namesList) {
            std::string prefix(database + "-chkpt-");
            if (name.length() < prefix.length() || name.substr(0, prefix.length()).compare(prefix) != 0)
                continue;

            std::string scnStr(name.substr(prefix.length(), name.length()));
            typeScn scn;
            try {
                scn = strtoull(scnStr.c_str(), nullptr, 10);
            } catch (const std::exception& e) {
                // Ignore other files
                continue;
            }

            if (unlikely(ctx->trace & Ctx::TRACE_CHECKPOINT))
                ctx->logTrace(Ctx::TRACE_CHECKPOINT, "found: " + name + " scn: " + std::to_string(scn));

            checkpointScnList.insert(scn);
            checkpointSchemaMap.insert_or_assign(scn, true);
        }

        if (startScn != Ctx::ZERO_SCN)
            firstDataScn = startScn;
        else
            firstDataScn = 0;

        if (unlikely(ctx->trace & Ctx::TRACE_CHECKPOINT))
            ctx->logTrace(Ctx::TRACE_CHECKPOINT, "scn: " + std::to_string(firstDataScn));

        if (firstDataScn != Ctx::ZERO_SCN && firstDataScn != 0) {
            std::set<typeScn>::iterator it = checkpointScnList.cend();

            while (it != checkpointScnList.cbegin()) {
                --it;
                if (*it <= firstDataScn && (sequence == Ctx::ZERO_SEQ || sequence == 0))
                    readCheckpoint(*it);
            }
        }
    }

    void Metadata::readCheckpoint(typeScn scn) {
        std::vector<std::string> msgs;
        ctx->info(0, "reading metadata for " + database + " for scn: " + std::to_string(scn));
        std::string ss;

        std::string name1(database + "-chkpt-" + std::to_string(scn));
        if (!stateRead(name1, CHECKPOINT_SCHEMA_FILE_MAX_SIZE, ss)) {
            if (unlikely(ctx->trace & Ctx::TRACE_CHECKPOINT))
                ctx->logTrace(Ctx::TRACE_CHECKPOINT, "no checkpoint file found, setting unknown sequence");

            sequence = Ctx::ZERO_SEQ;
            return;
        }
        if (!serializer->deserialize(this, ss, name1, msgs, true, true)) {
            for (const auto& msg: msgs) {
                ctx->info(0, msg);
            }
            return;
        }

        for (const auto& msg: msgs) {
            ctx->info(0, "- found: " + msg);
        }
        msgs.clear();

        // Schema missing
        if (schema->scn == Ctx::ZERO_SCN) {
            if (schema->refScn == Ctx::ZERO_SCN) {
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

        if (schema->scn != Ctx::ZERO_SCN)
            firstSchemaScn = schema->scn;
    }

    void Metadata::deleteOldCheckpoints() {
        std::set<typeScn> scnToDrop;

        if (ctx->flagsSet(Ctx::REDO_FLAGS_CHECKPOINT_KEEP))
            return;

        {
            std::unique_lock<std::mutex> lck(mtxCheckpoint);

            if (!allowedCheckpoints)
                return;

            if (checkpoints < ctx->checkpointKeep)
                return;

            bool foundSchema = false;
            uint64_t num = 0;
            std::set<typeScn>::iterator it = checkpointScnList.cend();
            while (it != checkpointScnList.cbegin()) {
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

        for (auto scn: scnToDrop) {
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
        std::vector<std::string> msgs;
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
        if (unlikely(ctx->trace & Ctx::TRACE_CHECKPOINT))
            ctx->logTrace(Ctx::TRACE_CHECKPOINT, "allowing checkpoints");

        std::unique_lock<std::mutex> lck(mtxCheckpoint);
        allowedCheckpoints = true;
    }

    bool Metadata::isNewData(typeScn scn, typeIdx idx) {
        if (clientScn == Ctx::ZERO_SCN)
            return true;

        if (clientScn < scn)
            return true;

        if (clientScn == scn && clientIdx < idx)
            return true;

        return false;
    }
}
