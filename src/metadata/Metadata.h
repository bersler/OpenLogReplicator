/* Header for Metadata class
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#define METADATA_STATUS_INITIALIZE          0
#define METADATA_STATUS_REPLICATE           1

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

    class Metadata {
    protected:
        std::condition_variable condStartedReplication;

    public:
        Schema* schema;
        Ctx* ctx;
        Locales* locales;
        State* state;
        State* stateDisk;
        Serializer* serializer;
        std::atomic<uint64_t> status;
        std::mutex mtx;
        //startup parameters
        std::string database;
        typeScn startScn;
        typeSeq startSequence;
        std::string startTime;
        int64_t startTimeRel;
        //database parameters
        bool onlineData;
        bool suppLogDbPrimary;
        bool suppLogDbAll;
        typeConId conId;
        std::string conName;
        std::string context;
        std::string dbRecoveryFileDest;
        std::string logArchiveDest;
        std::string dbBlockChecksum;
        std::string nlsCharacterSet;
        std::string logArchiveFormat;
        std::string nlsNcharCharacterSet;
        uint64_t defaultCharacterMapId;
        uint64_t defaultCharacterNcharMapId;
        //read position
        typeSeq sequence;
        uint64_t offset;
        typeResetlogs resetlogs;
        typeActivation activation;
        uint64_t checkpoints;
        typeScn firstDataScn;
        typeScn firstSchemaScn;
        typeScn checkpointScn;
        typeScn firstScn;
        typeScn nextScn;
        typeTime checkpointTime;
        uint64_t checkpointOffset;
        uint64_t checkpointBytes;
        typeSeq minSequence;
        uint64_t minOffset;
        typeXid minXid;
        uint64_t schemaInterval;
        typeScn lastCheckpointScn;
        typeSeq lastSequence;
        uint64_t lastCheckpointOffset;
        typeTime lastCheckpointTime;
        uint64_t lastCheckpointBytes;
        //schema
        std::vector<SchemaElement*> schemaElements;
        std::set<std::string> users;
        std::set<RedoLog*> redoLogs;
        std::set<OracleIncarnation*> oracleIncarnations;
        OracleIncarnation* oracleIncarnationCurrent;
        std::set<typeScn> checkpointScnList;
        std::unordered_map<typeScn, bool> checkpointSchemaMap;

        Metadata(Ctx* newCtx, Locales* newLocales, const char* newDatabase, typeConId newConId, typeScn newStartScn, typeSeq newStartSequence,
                 const char* newStartTime, int64_t newStartTimeRel);
        ~Metadata();

        void setNlsCharset(std::string& nlsCharset, std::string& nlsNcharCharset);
        void purgeRedoLogs();
        void setSeqOffset(typeSeq newSequence, uint64_t newOffset);
        void setResetlogs(typeResetlogs newResetlogs);
        void setActivation(typeActivation newActivation);
        void initializeDisk(const char* path);
        [[nodiscard]] bool stateRead(std::string& name, uint64_t maxSize, std::string& in);
        [[nodiscard]] bool stateDiskRead(std::string& name, uint64_t maxSize, std::string& in);
        [[nodiscard]] bool stateWrite(std::string& name, std::stringstream& out);
        [[nodiscard]] bool stateDrop(std::string& name);
        SchemaElement* addElement(const char* owner, const char* table, typeOptions options);

        void waitForReplication();
        void setStatusReplicate();
        void wakeUp();
        void checkpoint(typeScn newCheckpointScn, typeTime newCheckpointTime, uint64_t newCheckpointOffset, uint64_t newCheckpointBytes,
                        typeSeq newMinSequence, uint64_t newMinOffset, typeXid newMinXid);
        void writeCheckpoint(bool force);
        void readCheckpoints();
        void readCheckpoint(typeScn scn);
        void deleteOldCheckpoints();
        void loadAdaptiveSchema();
    };
}

#endif
