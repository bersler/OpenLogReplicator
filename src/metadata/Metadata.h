/* Header for Metadata class
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

#ifndef METADATA_H_
#define METADATA_H_

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <set>
#include <unordered_map>
#include <vector>

#include "../common/typeTime.h"
#include "../common/typeXid.h"
#include "../common/Ctx.h"
#include "../common/DbTable.h"

namespace OpenLogReplicator {
    class Ctx;
    class DbIncarnation;
    class Locales;
    class RedoLog;
    class Schema;
    class SchemaElement;
    class Serializer;
    class State;
    class StateDisk;
    class Thread;

    class Metadata final {
    protected:
        std::condition_variable condReplicator;
        std::condition_variable condWriter;
        static constexpr uint64_t CHECKPOINT_SCHEMA_FILE_MAX_SIZE = 2147483648;

    public:
        enum class STATUS : unsigned char {
            READY, // Replication hasn't started yet. The metadata is not initialized, the starting point of replication is not defined yet
            START, // Replicator tries to start replication with given parameters.
            REPLICATE // Replication is running. The metadata is initialized, the starting point of replication is defined.
        };

        Schema* schema;
        Ctx* ctx;
        Locales* locales;
        State* state{nullptr};
        State* stateDisk{nullptr};
        Serializer* serializer{nullptr};
        std::atomic<STATUS> status{STATUS::READY};

        // Startup parameters
        std::string database;
        typeScn startScn;
        typeSeq startSequence;
        std::string startTime;
        uint64_t startTimeRel;

        // Database parameters
        bool onlineData{false};
        bool suppLogDbPrimary{false};
        bool suppLogDbAll{false};
        bool logArchiveFormatCustom{false};
        bool allowedCheckpoints{false};
        // The writer is controlling the boot parameters. If the data is not available on startup, don't fail immediately.
        bool bootFailsafe{false};
        typeConId conId;
        std::string conName;
        std::string context;
        std::string dbTimezoneStr;
        int64_t dbTimezone{0};
        std::string dbRecoveryFileDest;
        std::string logArchiveDest;
        std::string dbBlockChecksum;
        std::string nlsCharacterSet;
        std::string logArchiveFormat{"o1_mf_%t_%s_%h_.arc"};
        std::string nlsNcharCharacterSet;
        uint64_t defaultCharacterMapId{0};
        uint64_t defaultCharacterNcharMapId{0};
        typeScn firstDataScn{Ctx::ZERO_SCN};
        typeScn firstSchemaScn{Ctx::ZERO_SCN};
        std::set<RedoLog*> redoLogs;

        // Transaction schema consistency mutex
        std::mutex mtxTransaction;

        // Checkpoint information
        std::mutex mtxCheckpoint;
        typeResetlogs resetlogs{0};
        std::set<DbIncarnation*> dbIncarnations;
        DbIncarnation* dbIncarnationCurrent{nullptr};
        typeActivation activation{0};
        typeSeq sequence{Ctx::ZERO_SEQ};
        typeSeq lastSequence{Ctx::ZERO_SEQ};
        uint64_t offset{0};
        typeScn firstScn{Ctx::ZERO_SCN};
        typeScn nextScn{Ctx::ZERO_SCN};
        typeScn clientScn{Ctx::ZERO_SCN};
        typeIdx clientIdx{0};
        uint64_t checkpoints{0};
        typeScn checkpointScn{Ctx::ZERO_SCN};
        typeScn lastCheckpointScn{Ctx::ZERO_SCN};
        typeTime checkpointTime{0};
        typeTime lastCheckpointTime;
        typeSeq checkpointSequence{Ctx::ZERO_SEQ};
        uint64_t checkpointOffset{0};
        uint64_t lastCheckpointOffset{0};
        uint64_t checkpointBytes{0};
        uint64_t lastCheckpointBytes{0};
        typeSeq minSequence{Ctx::ZERO_SEQ};
        uint64_t minOffset{0};
        typeXid minXid;
        uint64_t schemaInterval{0};
        std::set<typeScn> checkpointScnList;
        std::unordered_map<typeScn, bool> checkpointSchemaMap;

        std::vector<SchemaElement*> newSchemaElements;

        // Schema information
        std::mutex mtxSchema;
        std::vector<SchemaElement*> schemaElements;
        std::set<std::string> users;

        Metadata(Ctx* newCtx, Locales* newLocales, const char* newDatabase, typeConId newConId, typeScn newStartScn, typeSeq newStartSequence,
                 const char* newStartTime, uint64_t newStartTimeRel);
        ~Metadata();

        void setNlsCharset(const std::string& nlsCharset, const std::string& nlsNcharCharset);
        void purgeRedoLogs();
        void setSeqOffset(typeSeq newSequence, uint64_t newOffset);
        void setResetlogs(typeResetlogs newResetlogs);
        void setActivation(typeActivation newActivation);
        void setFirstNextScn(typeScn newFirstScn, typeScn newNextScn);
        void setNextSequence();
        [[nodiscard]] bool stateRead(const std::string& name, uint64_t maxSize, std::string& in) const;
        [[nodiscard]] bool stateDiskRead(const std::string& name, uint64_t maxSize, std::string& in) const;
        [[nodiscard]] bool stateWrite(const std::string& name, typeScn scn, const std::ostringstream& out) const;
        [[nodiscard]] bool stateDrop(const std::string& name) const;
        SchemaElement* addElement(const char* owner, const char* table, DbTable::OPTIONS options1, DbTable::OPTIONS options2);
        SchemaElement* addElement(const char* owner, const char* table, DbTable::OPTIONS options);
        void resetElements();
        void commitElements();
        void buildMaps(std::vector<std::string>& msgs, std::unordered_map<typeObj, std::string>& tablesUpdated);

        void waitForWriter(Thread* t);
        void waitForReplicator(Thread* t);
        void setStatusReady(Thread* t);
        void setStatusStart(Thread* t);
        void setStatusReplicate(Thread* t);
        void wakeUp(Thread* t);
        void checkpoint(Thread* t, typeScn newCheckpointScn, typeTime newCheckpointTime, typeSeq newCheckpointSequence, uint64_t newCheckpointOffset,
                        uint64_t newCheckpointBytes, typeSeq newMinSequence, uint64_t newMinOffset, typeXid newMinXid);
        void writeCheckpoint(Thread* t, bool force);
        void readCheckpoints();
        void readCheckpoint(typeScn scn);
        void deleteOldCheckpoints(Thread* t);
        void loadAdaptiveSchema();
        void allowCheckpoints();
        bool isNewData(typeScn scn, typeIdx idx) const;
    };
}

#endif
