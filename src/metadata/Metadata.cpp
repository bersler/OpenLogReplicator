/* Base class for handling of metadata
   Copyright (C) 2018-2025 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "../common/DbIncarnation.h"
#include "../common/DbTable.h"
#include "../common/Thread.h"
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
    Metadata::Metadata(Ctx* newCtx, Locales* newLocales, std::string newDatabase, typeConId newConId, Scn newStartScn,
                       Seq newStartSequence, std::string newStartTime, uint64_t newStartTimeRel) :
            schema(new Schema(newCtx, newLocales)),
            ctx(newCtx),
            locales(newLocales),
            database(std::move(newDatabase)),
            startScn(newStartScn),
            startSequence(newStartSequence),
            startTime(std::move(newStartTime)),
            startTimeRel(newStartTimeRel),
            conId(newConId) {
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

        for (DbIncarnation* oi: dbIncarnations)
            delete oi;
        dbIncarnations.clear();
        dbIncarnationCurrent = nullptr;
    }

    void Metadata::setNlsCharset(const std::string& nlsCharset, const std::string& nlsNcharCharset) {
        for (const auto& [mapId, characterSet]: locales->characterMap) {
            if (nlsCharset == characterSet->name) {
                defaultCharacterMapId = mapId;
                break;
            }
        }

        if (unlikely(defaultCharacterMapId == 0))
            throw RuntimeException(10042, "unsupported NLS_CHARACTERSET value: " + nlsCharset);

        for (const auto& [characterNcharMapId, characterSet]: locales->characterMap) {
            if (nlsNcharCharset == characterSet->name) {
                defaultCharacterNcharMapId = characterNcharMapId;
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
        std::unique_lock<std::mutex> const lck(mtxCheckpoint);

        activation = newActivation;
    }

    void Metadata::setFirstNextScn(Scn newFirstScn, Scn newNextScn) {
        std::unique_lock<std::mutex> const lck(mtxCheckpoint);

        firstScn = newFirstScn;
        nextScn = newNextScn;
    }

    void Metadata::setNextSequence() {
        std::unique_lock<std::mutex> const lck(mtxCheckpoint);

        ++sequence;
    }

    void Metadata::setSeqFileOffset(Seq newSequence, FileOffset newFileOffset) {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
            ctx->logTrace(Ctx::TRACE::CHECKPOINT, "setting sequence to: " + newSequence.toString() + ", offset: " + newFileOffset.toString());

        std::unique_lock<std::mutex> const lck(mtxCheckpoint);

        sequence = newSequence;
        fileOffset = newFileOffset;
    }

    bool Metadata::stateRead(const std::string& name, uint64_t maxSize, std::string& in) const {
        try {
            return state->read(name, maxSize, in);
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
        }
        return false;
    }

    bool Metadata::stateDiskRead(const std::string& name, uint64_t maxSize, std::string& in) const {
        try {
            return stateDisk->read(name, maxSize, in);
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
        }
        return false;
    }

    bool Metadata::stateWrite(const std::string& name, Scn scn, const std::ostringstream& out) const {
        try {
            state->write(name, scn, out);
            return true;
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
        }
        return false;
    }

    bool Metadata::stateDrop(const std::string& name) const {
        try {
            state->drop(name);
            return true;
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
        }
        return false;
    }

    SchemaElement* Metadata::addElement(const std::string& owner, const std::string& table, DbTable::OPTIONS options1, DbTable::OPTIONS options2) {
        return addElement(owner, table, static_cast<DbTable::OPTIONS>(static_cast<uint>(options1) | static_cast<uint>(options2)));
    }

    SchemaElement* Metadata::addElement(const std::string& owner, const std::string& table, DbTable::OPTIONS options) {
        if (unlikely(!Data::checkNameCase(owner)))
            throw ConfigurationException(30003, "owner '" + owner + "' contains lower case characters, value must be upper case");
        if (unlikely(!Data::checkNameCase(table)))
            throw ConfigurationException(30004, "table '" + table + "' contains lower case characters, value must be upper case");
        auto* element = new SchemaElement(owner, table, options);
        newSchemaElements.push_back(element);
        return element;
    }

    void Metadata::resetElements() {
        for (SchemaElement* element: newSchemaElements)
            delete element;
        newSchemaElements.clear();

        static const std::vector<std::string> sysSchemaTables {"CCOL\\$", "CDEF\\$", "COL\\$", "ECOL\\$"};
        for (const auto& table : sysSchemaTables)
            addElement("SYS", table, DbTable::OPTIONS::SYSTEM_TABLE, DbTable::OPTIONS::SCHEMA_TABLE);

        static const std::vector<std::string> sysTables {"DEFERRED_STG\\$", "LOB\\$", "LOBCOMPPART\\$", "LOBFRAG\\$", "OBJ\\$", "TAB\\$", "TABPART\\$",
                                                         "TABCOMPART\\$", "TABSUBPART\\$", "TS\\$", "USER\\$"};
        for (const auto& table : sysTables)
            addElement("SYS", table, DbTable::OPTIONS::SYSTEM_TABLE);

        static const std::vector<std::string> xdbTables {"XDB\\$TTSET", "X\\$NM.*", "X\\$PT.*", "X\\$QN.*"};
        for (const auto& table : xdbTables)
            addElement("XDB", table, DbTable::OPTIONS::SYSTEM_TABLE);
    }

    void Metadata::commitElements() {
        std::unique_lock<std::mutex> const lck(mtxSchema);

        for (SchemaElement* element: schemaElements)
            delete element;
        schemaElements.clear();

        for (SchemaElement* element: newSchemaElements)
            schemaElements.push_back(element);
        newSchemaElements.clear();
    }

    void Metadata::buildMaps(std::vector<std::string>& msgs, std::unordered_map<typeObj, std::string>& tablesUpdated) {
        for (const SchemaElement* element: schemaElements) {
            if (ctx->isLogLevelAt(Ctx::LOG::DEBUG))
                msgs.push_back("- creating table schema for owner: " + element->owner + " table: " + element->table + " options: " +
                               std::to_string(static_cast<uint>(element->options)));

            schema->buildMaps(element->owner, element->table, element->keyList, element->key, element->tagType, element->tagList, element->tag,
                              element->condition, element->options, tablesUpdated, suppLogDbPrimary, suppLogDbAll, defaultCharacterMapId,
                              defaultCharacterNcharMapId);
        }
    }

    void Metadata::waitForWriter(Thread* t) {
        {
            t->contextSet(Thread::CONTEXT::CHKPT, Thread::REASON::CHKPT);
            std::unique_lock<std::mutex> lck(mtxCheckpoint);

            if (status == STATUS::READY) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                    ctx->logTrace(Ctx::TRACE::SLEEP, "Metadata:waitForWriter");
                t->contextSet(Thread::CONTEXT::WAIT, Thread::REASON::METADATA_WAIT_WRITER);
                condReplicator.wait(lck);
            }
        }
        t->contextSet(Thread::CONTEXT::CPU);
    }

    void Metadata::waitForReplicator(Thread* t) {
        {
            t->contextSet(Thread::CONTEXT::CHKPT, Thread::REASON::CHKPT);
            std::unique_lock<std::mutex> lck(mtxCheckpoint);

            if (status == STATUS::START) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                    ctx->logTrace(Ctx::TRACE::SLEEP, "Metadata:waitForReplicator");
                t->contextSet(Thread::CONTEXT::WAIT, Thread::REASON::METADATA_WAIT_FOR_REPLICATOR);
                condWriter.wait(lck);
            }
        }
        t->contextSet(Thread::CONTEXT::CPU);
    }

    void Metadata::setStatusReady(Thread* t) {
        {
            t->contextSet(Thread::CONTEXT::CHKPT, Thread::REASON::CHKPT);
            std::unique_lock<std::mutex> const lck(mtxCheckpoint);

            status = STATUS::READY;
            firstDataScn = Scn::none();
            firstSchemaScn = Scn::none();
            checkpointScn = Scn::none();
            schema->scn = Scn::none();
            condWriter.notify_all();
        }
        t->contextSet(Thread::CONTEXT::CPU);
    }

    void Metadata::setStatusStart(Thread* t) {
        {
            t->contextSet(Thread::CONTEXT::CHKPT, Thread::REASON::CHKPT);
            std::unique_lock<std::mutex> const lck(mtxCheckpoint);

            status = STATUS::START;
            condReplicator.notify_all();
        }
        t->contextSet(Thread::CONTEXT::CPU);
    }

    void Metadata::setStatusReplicate(Thread* t) {
        {
            t->contextSet(Thread::CONTEXT::CHKPT, Thread::REASON::CHKPT);
            std::unique_lock<std::mutex> const lck(mtxCheckpoint);

            status = STATUS::REPLICATE;
            condReplicator.notify_all();
            condWriter.notify_all();
        }
        t->contextSet(Thread::CONTEXT::CPU);
    }

    void Metadata::wakeUp(Thread* t) {
        {
            t->contextSet(Thread::CONTEXT::CHKPT, Thread::REASON::CHKPT);
            std::unique_lock<std::mutex> const lck(mtxCheckpoint);

            condReplicator.notify_all();
            condWriter.notify_all();
        }
        t->contextSet(Thread::CONTEXT::CPU);
    }

    void Metadata::checkpoint(Thread* t, Scn newCheckpointScn, Time newCheckpointTime, Seq newCheckpointSequence, FileOffset newCheckpointFileOffset,
                              uint64_t newCheckpointBytes, Seq newMinSequence, FileOffset newMinFileOffset, Xid newMinXid) {
        {
            t->contextSet(Thread::CONTEXT::CHKPT, Thread::REASON::CHKPT);
            std::unique_lock<std::mutex> const lck(mtxCheckpoint);

            checkpointScn = newCheckpointScn;
            checkpointTime = newCheckpointTime;
            checkpointSequence = newCheckpointSequence;
            checkpointFileOffset = newCheckpointFileOffset;
            checkpointBytes += newCheckpointBytes;
            minSequence = newMinSequence;
            minFileOffset = newMinFileOffset;
            minXid = newMinXid;
        }
        t->contextSet(Thread::CONTEXT::CPU);
    }

    void Metadata::writeCheckpoint(Thread* t, bool force) {
        std::ostringstream ss;

        {
            t->contextSet(Thread::CONTEXT::CHKPT, Thread::REASON::CHKPT);
            std::unique_lock<std::mutex> const lck(mtxCheckpoint);
            if (!allowedCheckpoints)
                return;

            // Nothing processed so far
            if (checkpointScn == Scn::none() || lastCheckpointScn == checkpointScn || checkpointSequence == Seq::none())
                return;

            if (lastSequence == sequence && !force &&
                (static_cast<uint64_t>(checkpointTime.toEpoch(ctx->hostTimezone) - lastCheckpointTime.toEpoch(ctx->hostTimezone)) < ctx->checkpointIntervalS) &&
                (checkpointBytes - lastCheckpointBytes) / 1024 / 1024 < ctx->checkpointIntervalMb)
                return;

            // Schema did not change
            bool storeSchema = true;
            if (schema->refScn != Scn::none() && schema->refScn >= schema->scn) {
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
            lastCheckpointFileOffset = checkpointFileOffset;
            lastCheckpointTime = checkpointTime;
            lastCheckpointBytes = checkpointBytes;
            ++checkpoints;
            checkpointScnList.insert(checkpointScn);
            checkpointSchemaMap.insert_or_assign(checkpointScn, storeSchema);
        }
        t->contextSet(Thread::CONTEXT::CPU);

        const std::string checkpointName = database + "-chkpt-" + lastCheckpointScn.toString();

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
            ctx->logTrace(Ctx::TRACE::CHECKPOINT, "write scn: " + lastCheckpointScn.toString() + " time: " +
                                                  std::to_string(lastCheckpointTime.getVal()) + " seq: " + lastSequence.toString() + " offset: " +
                                                  lastCheckpointFileOffset.toString() + " name: " + checkpointName);

        if (!stateWrite(checkpointName, lastCheckpointScn, ss))
            ctx->warning(60018, "file: " + checkpointName + " - couldn't write checkpoint");
    }

    void Metadata::readCheckpoints() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
            ctx->logTrace(Ctx::TRACE::CHECKPOINT, "searching for previous checkpoint information");

        std::set<std::string> namesList;
        state->list(namesList);

        for (const std::string& name: namesList) {
            const std::string prefix(database + "-chkpt-");
            if (name.length() < prefix.length() || name.substr(0, prefix.length()) != prefix)
                continue;

            const std::string scnStr(name.substr(prefix.length(), name.length()));
            Scn scn;
            try {
                scn = strtoull(scnStr.c_str(), nullptr, 10);
            } catch (const std::exception& e) {
                // Ignore other files
                continue;
            }

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
                ctx->logTrace(Ctx::TRACE::CHECKPOINT, "found: " + name + " scn: " + scn.toString());

            checkpointScnList.insert(scn);
            checkpointSchemaMap.insert_or_assign(scn, true);
        }

        if (startScn != Scn::none())
            firstDataScn = startScn;
        else
            firstDataScn = 0;

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
            ctx->logTrace(Ctx::TRACE::CHECKPOINT, "scn: " + firstDataScn.toString());

        if (firstDataScn != Scn::none() && firstDataScn.getData() != 0) {
            auto it = checkpointScnList.cend();

            while (it != checkpointScnList.cbegin()) {
                --it;
                if (*it <= firstDataScn && (sequence == Seq::none() || sequence == Seq::zero()))
                    readCheckpoint(*it);
            }
        }
    }

    void Metadata::readCheckpoint(Scn scn) {
        std::vector<std::string> msgs;
        std::unordered_map<typeObj, std::string> tablesUpdated;
        ctx->info(0, "reading metadata for " + database + " for scn: " + scn.toString());
        std::string ss;

        const std::string name1(database + "-chkpt-" + scn.toString());
        if (!stateRead(name1, CHECKPOINT_SCHEMA_FILE_MAX_SIZE, ss)) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
                ctx->logTrace(Ctx::TRACE::CHECKPOINT, "no checkpoint file found, setting unknown sequence");

            sequence = Seq::none();
            return;
        }
        if (!serializer->deserialize(this, ss, name1, msgs, tablesUpdated, true, true)) {
            for (const auto& [_, tableName]: tablesUpdated) {
                ctx->info(0, tableName);
            }
            return;
        }

        for (const auto& msg: msgs)
            ctx->info(0, msg);
        for (const auto& [_, tableName]: tablesUpdated) {
            ctx->info(0, "- found: " + tableName);
        }
        tablesUpdated.clear();

        // Schema missing
        if (schema->scn == Scn::none()) {
            if (schema->refScn == Scn::none()) {
                ctx->warning(60019, "file: " + name1 + " - load checkpoint failed, reference SCN missing");
                return;
            }

            ss.clear();
            const std::string name2(database + "-chkpt-" + schema->refScn.toString());
            ctx->info(0, "reading schema for " + database + " for scn: " + schema->refScn.toString());

            if (!stateRead(name2, CHECKPOINT_SCHEMA_FILE_MAX_SIZE, ss))
                return;

            if (!serializer->deserialize(this, ss, name2, msgs, tablesUpdated, false, true)) {
                for (const auto& msg: msgs) {
                    ctx->info(0, msg);
                }
                return;
            }

            for (const auto& msg: msgs) {
                ctx->info(0, msg);
            }
            for (const auto& [_, tableName] : tablesUpdated) {
                ctx->info(0, "- found: " + tableName);
            }
        }

        if (schema->scn != Scn::none())
            firstSchemaScn = schema->scn;
    }

    void Metadata::deleteOldCheckpoints(Thread* t) {
        std::set<Scn> scnToDrop;

        if (ctx->isFlagSet(Ctx::REDO_FLAGS::CHECKPOINT_KEEP))
            return;

        {
            t->contextSet(Thread::CONTEXT::CHKPT, Thread::REASON::CHKPT);
            std::unique_lock<std::mutex> const lck(mtxCheckpoint);

            if (!allowedCheckpoints) {
                t->contextSet(Thread::CONTEXT::CPU);
                return;
            }

            if (checkpoints < ctx->checkpointKeep) {
                t->contextSet(Thread::CONTEXT::CPU);
                return;
            }

            bool foundSchema = false;
            uint64_t num = 0;
            auto it = checkpointScnList.cend();
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
            const std::string checkpointName = database + "-chkpt-" + scn.toString();
            if (!stateDrop(checkpointName))
                break;
        }

        {
            std::unique_lock<std::mutex> const lck(mtxCheckpoint);

            for (auto scn: scnToDrop) {
                checkpointScnList.erase(scn);
                checkpointSchemaMap.erase(scn);
            }
        }
        t->contextSet(Thread::CONTEXT::CPU);
    }

    void Metadata::loadAdaptiveSchema() {
        std::string ss;
        std::vector<std::string> msgs;
        std::unordered_map<typeObj, std::string> tablesUpdated;
        const std::string name("base-" + ctx->versionStr);

        ctx->info(0, "reading adaptive schema from: " + name + ".json");
        const std::string nlsCharset = "AL32UTF8";
        const std::string nlsNcharCharset = "AL16UTF16";
        setNlsCharset(nlsCharset, nlsNcharCharset);

        if (!stateDiskRead(name, CHECKPOINT_SCHEMA_FILE_MAX_SIZE, ss)) {
            ctx->warning(60020, "file: " + name + " - load adaptive schema failed");
            return;
        }

        if (!serializer->deserialize(this, ss, name, msgs, tablesUpdated, false, true)) {
            for (const auto& msg: msgs) {
                ctx->info(0, msg);
            }
            return;
        }

        firstSchemaScn = 0;
        for (const auto& msg: msgs) {
            ctx->info(0, msg);
        }
        for (const auto& [_, tableName]: tablesUpdated) {
            ctx->info(0, "- found: " + tableName);
        }
    }

    void Metadata::allowCheckpoints() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
            ctx->logTrace(Ctx::TRACE::CHECKPOINT, "allowing checkpoints");

        std::unique_lock<std::mutex> const lck(mtxCheckpoint);
        allowedCheckpoints = true;
    }

    bool Metadata::isNewData(Scn scn, typeIdx idx) const {
        if (clientScn == Scn::none())
            return true;

        if (clientScn < scn)
            return true;

        if (clientScn == scn && clientIdx < idx)
            return true;

        return false;
    }
}
