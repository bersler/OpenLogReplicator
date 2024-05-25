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

#ifndef METADATA_H_
#define METADATA_H_

namespace OpenLogReplicator {
    class Ctx;
    class Locales;
    class OracleIncarnation;
    class RedoLog;
    class Schema;
    class SchemaElement;
    class Serializer;
    class State;
    class StateDisk;

    class Metadata final {
    protected:
        std::condition_variable condReplicator;
        std::condition_variable condWriter;
        static constexpr uint64_t CHECKPOINT_SCHEMA_FILE_MAX_SIZE = 2147483648;

    public:
        // Replication hasn't started yet. The metadata is not initialized, the starting point of replication is not defined yet
        static constexpr uint64_t STATUS_READY = 0;
        // Replicator tries to start replication with given parameters.
        static constexpr uint64_t STATUS_START = 1;
        // Replication is running. The metadata is initialized, the starting point of replication is defined.
        static constexpr uint64_t STATUS_REPLICATE = 2;

        Schema* schema;
        Ctx* ctx;
        Locales* locales;
        State* state;
        State* stateDisk;
        Serializer* serializer;
        std::atomic<uint64_t> status;

        // Startup parameters
        std::string database;
        typeScn startScn;
        typeSeq startSequence;
        std::string startTime;
        uint64_t startTimeRel;

        // Database parameters
        bool onlineData;
        bool suppLogDbPrimary;
        bool suppLogDbAll;
        bool logArchiveFormatCustom;
        bool allowedCheckpoints;
        // The writer is controlling the boot parameters. If the data is not available on startup, don't fail immediately.
        bool bootFailsafe;
        typeConId conId;
        std::string conName;
        std::string context;
        std::string dbTimezoneStr;
        int64_t dbTimezone;
        std::string dbRecoveryFileDest;
        std::string logArchiveDest;
        std::string dbBlockChecksum;
        std::string nlsCharacterSet;
        std::string logArchiveFormat;
        std::string nlsNcharCharacterSet;
        uint64_t defaultCharacterMapId;
        uint64_t defaultCharacterNcharMapId;
        typeScn firstDataScn;
        typeScn firstSchemaScn;
        std::set<RedoLog*> redoLogs;

        // Transaction schema consistency mutex
        std::mutex mtxTransaction;

        // Checkpoint information
        std::mutex mtxCheckpoint;
        typeResetlogs resetlogs;
        std::set<OracleIncarnation*> oracleIncarnations;
        OracleIncarnation* oracleIncarnationCurrent;
        typeActivation activation;
        typeSeq sequence;
        typeSeq lastSequence;
        uint64_t offset;
        typeScn firstScn;
        typeScn nextScn;
        typeScn clientScn;
        typeIdx clientIdx;
        uint64_t checkpoints;
        typeScn checkpointScn;
        typeScn lastCheckpointScn;
        typeTime checkpointTime;
        typeTime lastCheckpointTime;
        typeSeq checkpointSequence;
        uint64_t checkpointOffset;
        uint64_t lastCheckpointOffset;
        uint64_t checkpointBytes;
        uint64_t lastCheckpointBytes;
        typeSeq minSequence;
        uint64_t minOffset;
        typeXid minXid;
        uint64_t schemaInterval;
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
        [[nodiscard]] bool stateRead(const std::string& name, uint64_t maxSize, std::string& in);
        [[nodiscard]] bool stateDiskRead(const std::string& name, uint64_t maxSize, std::string& in);
        [[nodiscard]] bool stateWrite(const std::string& name, typeScn scn, const std::ostringstream& out);
        [[nodiscard]] bool stateDrop(const std::string& name);
        SchemaElement* addElement(const char* owner, const char* table, typeOptions options);
        void resetElements();
        void commitElements();

        void waitForWriter();
        void waitForReplicator();
        void setStatusReady();
        void setStatusStart();
        void setStatusReplicate();
        void wakeUp();
        void checkpoint(typeScn newCheckpointScn, typeTime newCheckpointTime, typeSeq newCheckpointSequence, uint64_t newCheckpointOffset,
                        uint64_t newCheckpointBytes, typeSeq newMinSequence, uint64_t newMinOffset, typeXid newMinXid);
        void writeCheckpoint(bool force);
        void readCheckpoints();
        void readCheckpoint(typeScn scn);
        void deleteOldCheckpoints();
        void loadAdaptiveSchema();
        void allowCheckpoints();
        bool isNewData(typeScn scn, typeIdx idx);
    };
}

#endif
