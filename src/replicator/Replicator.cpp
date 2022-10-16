/* Thread reading Oracle Redo Logs using offline mode
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

#include <cerrno>
#include <dirent.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/ConfigurationException.h"
#include "../common/Ctx.h"
#include "../common/OracleIncarnation.h"
#include "../common/RedoLogException.h"
#include "../common/RuntimeException.h"
#include "../common/Timer.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "../parser/Parser.h"
#include "../parser/Transaction.h"
#include "../parser/TransactionBuffer.h"
#include "../reader/ReaderFilesystem.h"
#include "Replicator.h"

namespace OpenLogReplicator {
    Replicator::Replicator(Ctx* newCtx, void (*newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                           TransactionBuffer* newTransactionBuffer, std::string newAlias, const char* newDatabase) :
            Thread(newCtx, newAlias),
            archGetLog(newArchGetLog),
            builder(newBuilder),
            metadata(newMetadata),
            transactionBuffer(newTransactionBuffer),
            database(newDatabase),
            archReader(nullptr) {
    }

    Replicator::~Replicator() {
        readerDropAll();

        if (transactionBuffer != nullptr)
            transactionBuffer->purge();

        while (!archiveRedoQueue.empty()) {
            Parser* redoTmp = archiveRedoQueue.top();
            archiveRedoQueue.pop();
            delete redoTmp;
        }

        for (Parser* onlineRedo : onlineRedoSet)
            delete onlineRedo;
        onlineRedoSet.clear();

        pathMapping.clear();
        redoLogsBatch.clear();
    }

    void Replicator::initialize() {
    }

    void Replicator::cleanArchList() {
        while (!archiveRedoQueue.empty()) {
            Parser* parser = archiveRedoQueue.top();
            archiveRedoQueue.pop();
            delete parser;
        }
    }

    void Replicator::updateOnlineLogs() {
        for (Parser* onlineRedo : onlineRedoSet) {
            if (!onlineRedo->reader->updateRedoLog())
                throw RuntimeException("updating failed for " + onlineRedo->path);
            onlineRedo->sequence = onlineRedo->reader->getSequence();
            onlineRedo->firstScn = onlineRedo->reader->getFirstScn();
            onlineRedo->nextScn = onlineRedo->reader->getNextScn();
        }
    }

    void Replicator::readerDropAll(void) {
        bool wakingUp;
        for (;;) {
            wakingUp = false;
            for (Reader* reader : readers) {
                if (!reader->finished) {
                    reader->wakeUp();
                    wakingUp = true;
                }
            }
            if (!wakingUp)
                break;
            usleep(1000);
        }

        while (readers.size() > 0) {
            Reader* reader = *(readers.begin());
            ctx->finishThread(reader);
            readers.erase(reader);
            delete reader;
        }

        archReader = nullptr;
        readers.clear();
    }

    void Replicator::loadDatabaseMetadata() {
        archReader = readerCreate(0);
    }

    void Replicator::positionReader() {
        if (metadata->startSequence != ZERO_SEQ)
            metadata->setSeqOffset(metadata->startSequence, 0);
        else
            metadata->setSeqOffset(0, 0);
    }

    void Replicator::verifySchema(typeScn currentScn __attribute__((unused))) {
        // Nothing for offline mode
    }

    void Replicator::createSchema() {
        if (FLAG(REDO_FLAGS_SCHEMALESS))
            return;

        throw RuntimeException("schema file missing");
    }

    void Replicator::updateOnlineRedoLogData() {
        // Nothing here
    }

    void Replicator::run() {
        TRACE(TRACE2_THREADS, "THREADS: replicator (" << std::hex << std::this_thread::get_id() << ") start")

        try {
            loadDatabaseMetadata();

            do {
                metadata->waitForReplication();
                if (ctx->softShutdown)
                    break;
                if (metadata->status == METADATA_STATUS_INITIALIZE)
                    continue;

                printStartMsg();
                if (ctx->softShutdown)
                    break;

                metadata->readCheckpoints();
                if (metadata->firstDataScn == ZERO_SCN || metadata->sequence == ZERO_SEQ)
                    positionReader();

                INFO("current resetlogs is: " << std::dec << metadata->resetlogs)
                INFO("first data SCN: " << std::dec << metadata->firstDataScn)
                if (metadata->firstSchemaScn != ZERO_SCN) {
                    INFO("first schema SCN: " << std::dec << metadata->firstSchemaScn)
                }

                // No schema available?
                if (metadata->schema->scn == ZERO_SCN)
                    createSchema();

                if (metadata->sequence == ZERO_SEQ)
                    throw RuntimeException("starting sequence is unknown, failing");

                if (metadata->firstDataScn == ZERO_SCN) {
                    INFO("last confirmed scn: <none>, starting sequence: " << std::dec << metadata->sequence << ", offset: " << metadata->offset)
                } else {
                    INFO("last confirmed scn: " << std::dec << metadata->firstDataScn << ", starting sequence: " << std::dec << metadata->sequence <<
                            ", offset: " << metadata->offset)
                }

                if ((metadata->dbBlockChecksum == "OFF" || metadata->dbBlockChecksum == "FALSE") && !DISABLE_CHECKS(DISABLE_CHECKS_BLOCK_SUM)) {
                    WARNING("HINT: set DB_BLOCK_CHECKSUM = TYPICAL on the database"
                            " or turn off consistency checking in OpenLogReplicator setting parameter disable-checks: " << std::dec <<
                            DISABLE_CHECKS_BLOCK_SUM << " for the reader")
                }
            } while (metadata->status == METADATA_STATUS_INITIALIZE);

            while (!ctx->softShutdown) {
                bool logsProcessed = false;

                logsProcessed |= processArchivedRedoLogs();
                if (ctx->softShutdown)
                    break;

                if (!continueWithOnline())
                    break;
                if (ctx->softShutdown)
                    break;

                if (!FLAG(REDO_FLAGS_ARCH_ONLY))
                    logsProcessed |= processOnlineRedoLogs();
                if (ctx->softShutdown)
                    break;

                if (!logsProcessed)
                    usleep(ctx->redoReadSleepUs);
            }
        } catch (ConfigurationException& ex) {
            ERROR(ex.msg);
            ctx->stopHard();
        } catch (RedoLogException& ex) {
            ERROR(ex.msg);
            ctx->stopHard();
        } catch (RuntimeException& ex) {
            ERROR(ex.msg);
            ctx->stopHard();
        } catch (std::bad_alloc& ex) {
            ERROR("memory allocation failed: " << ex.what())
            ctx->stopHard();
        }

        INFO("Oracle replicator for: " << database << " is shutting down")

        ctx->replicatorFinished = true;
        INFO("Oracle replicator for: " << database << " is shut down, allocated at most " << std::dec <<
                ctx->getMaxUsedMemory() << "MB memory, max disk read buffer: " << (ctx->buffersMaxUsed * MEMORY_CHUNK_SIZE_MB) << "MB")

        TRACE(TRACE2_THREADS, "THREADS: replicator (" << std::hex << std::this_thread::get_id() << ") stop")
    }

    Reader* Replicator::readerCreate(int64_t group) {
        for (Reader* reader : readers)
            if (reader->getGroup() == group)
                return reader;

        auto* readerFS = new ReaderFilesystem(ctx, alias + "-reader-" + std::to_string(group) , database, group,
                                              metadata->dbBlockChecksum != "OFF" && metadata->dbBlockChecksum != "FALSE");
        readers.insert(readerFS);
        readerFS->initialize();

        ctx->spawnThread(readerFS);
        return readerFS;
    }

    void Replicator::checkOnlineRedoLogs() {
        for (Parser* onlineRedo : onlineRedoSet)
            delete onlineRedo;
        onlineRedoSet.clear();

        for (Reader* reader : readers) {
            if (reader->getGroup() == 0)
                continue;

            bool foundPath = false;
            for (std::string& path : reader->paths) {
                reader->fileName = path;
                applyMapping(reader->fileName);
                if (reader->checkRedoLog()) {
                    foundPath = true;
                    auto* parser = new Parser(ctx, builder, metadata, transactionBuffer,
                                              reader->getGroup(), reader->fileName);

                    parser->reader = reader;
                    INFO("online redo log: " << reader->fileName)
                    onlineRedoSet.insert(parser);
                    break;
                }
            }

            if (!foundPath) {
                uint64_t badGroup = reader->getGroup();
                for (std::string& path : reader->paths) {
                    std::string pathMapped(path);
                    applyMapping(pathMapped);
                    ERROR("can't read: " << pathMapped)
                }
                throw RuntimeException("can't read any member of group " +std::to_string(badGroup));
            }
        }
    }

    // Format uses wildcards:
    // %s - sequence number
    // %S - sequence number zero filled
    // %t - thread id
    // %T - thread id zero filled
    // %r - resetlogs id
    // %a - activation id
    // %d - database id
    // %h - some hash
    uint64_t Replicator::getSequenceFromFileName(Replicator* replicator, const std::string& file) {
        Ctx* ctx = replicator->ctx;
        uint64_t sequence = 0;
        uint64_t i = 0;
        uint64_t j = 0;

        while (i < replicator->metadata->logArchiveFormat.length() && j < file.length()) {
            if (replicator->metadata->logArchiveFormat[i] == '%') {
                if (i + 1 >= replicator->metadata->logArchiveFormat.length()) {
                    WARNING("Error getting sequence from file: " << file << " log_archive_format: " << replicator->metadata->logArchiveFormat <<
                            " at position " << j << " format position " << i << ", found end after %")
                    return 0;
                }
                uint64_t digits = 0;
                if (replicator->metadata->logArchiveFormat[i + 1] == 's' || replicator->metadata->logArchiveFormat[i + 1] == 'S' ||
                        replicator->metadata->logArchiveFormat[i + 1] == 't' || replicator->metadata->logArchiveFormat[i + 1] == 'T' ||
                        replicator->metadata->logArchiveFormat[i + 1] == 'r' || replicator->metadata->logArchiveFormat[i + 1] == 'a' ||
                        replicator->metadata->logArchiveFormat[i + 1] == 'd') {
                    // Some [0-9]*
                    uint64_t number = 0;
                    while (j < file.length() && file[j] >= '0' && file[j] <= '9') {
                        number = number * 10 + (file[j] - '0');
                        ++j;
                        ++digits;
                    }

                    if (replicator->metadata->logArchiveFormat[i + 1] == 's' || replicator->metadata->logArchiveFormat[i + 1] == 'S')
                        sequence = number;
                    i += 2;
                } else if (replicator->metadata->logArchiveFormat[i + 1] == 'h') {
                    // Some [0-9a-z]*
                    while (j < file.length() && ((file[j] >= '0' && file[j] <= '9') || (file[j] >= 'a' && file[j] <= 'z'))) {
                        ++j;
                        ++digits;
                    }
                    i += 2;
                }

                if (digits == 0) {
                    WARNING("Error getting sequence from file: " << file << " log_archive_format: " << replicator->metadata->logArchiveFormat <<
                            " at position " << j << " format position " << i << ", found no number/hash")
                    return 0;
                }
            } else if (file[j] == replicator->metadata->logArchiveFormat[i]) {
                ++i;
                ++j;
            } else {
                WARNING("Error getting sequence from file: " << file << " log_archive_format: " << replicator->metadata->logArchiveFormat <<
                        " at position " << j << " format position " << i << ", found different values")
                return 0;
            }
        }

        if (i == replicator->metadata->logArchiveFormat.length() && j == file.length())
            return sequence;

        WARNING("Error getting sequence from file: " << file << " log_archive_format: " << replicator->metadata->logArchiveFormat <<
                " at position " << j << " format position " << i << ", found no sequence")
        return 0;
    }

    void Replicator::addPathMapping(const char* source, const char* target) {
        TRACE(TRACE2_FILE, "FILE: added mapping [" << source << "] -> [" << target << "]")
        std::string sourceMaping(source);
        std::string targetMapping(target);
        pathMapping.push_back(sourceMaping);
        pathMapping.push_back(targetMapping);
    }

    void Replicator::addRedoLogsBatch(const char* path) {
        redoLogsBatch.emplace_back(path);
    }

    void Replicator::applyMapping(std::string& path) {
        uint64_t sourceLength;
        uint64_t targetLength;
        uint64_t newPathLength = path.length();
        char pathBuffer[MAX_PATH_LENGTH];

        for (uint64_t i = 0; i < pathMapping.size() / 2; ++i) {
            sourceLength = pathMapping[i * 2].length();
            targetLength = pathMapping[i * 2 + 1].length();

            if (sourceLength <= newPathLength &&
                    newPathLength - sourceLength + targetLength < MAX_PATH_LENGTH - 1 &&
                    memcmp(path.c_str(), pathMapping[i * 2].c_str(), sourceLength) == 0) {

                memcpy((void*)pathBuffer, (void*)pathMapping[i * 2 + 1].c_str(), targetLength);
                memcpy((void*)(pathBuffer + targetLength), (void*)(path.c_str() + sourceLength), newPathLength - sourceLength);
                pathBuffer[newPathLength - sourceLength + targetLength] = 0;
                if (newPathLength - sourceLength + targetLength >= MAX_PATH_LENGTH)
                    throw RuntimeException("After mapping path length (" + std::to_string(newPathLength - sourceLength + targetLength) +
                            ") is too long for: " + pathBuffer);
                path.assign(pathBuffer);
                break;
            }
        }
    }

    bool Replicator::checkConnection() {
        return true;
    }

    void Replicator::goStandby() {
    }

    bool Replicator::continueWithOnline() {
        return true;
    }

    const char* Replicator::getModeName() const {
        return "offline";
    }

    void Replicator::archGetLogPath(Replicator* replicator) {
        Ctx* ctx = replicator->ctx;
        if (replicator->metadata->logArchiveFormat.length() == 0)
            throw RuntimeException("missing location of archived redo logs for offline mode");

        std::string mappedPath(replicator->metadata->dbRecoveryFileDest + "/" + replicator->metadata->context + "/archivelog");
        replicator->applyMapping(mappedPath);
        TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath)

        DIR* dir;
        if ((dir = opendir(mappedPath.c_str())) == nullptr)
            throw RuntimeException("can't access directory: " + mappedPath);

        std::string newLastCheckedDay;
        struct dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;

            struct stat fileStat;
            std::string mappedSubPath(mappedPath + "/" + ent->d_name);
            if (stat(mappedSubPath.c_str(), &fileStat)) {
                WARNING("Reading information for file: " << mappedSubPath << " - " << strerror(errno))
                continue;
            }

            if (!S_ISDIR(fileStat.st_mode))
                continue;

            // Skip earlier days
            if (replicator->lastCheckedDay == ent->d_name)
                continue;

            TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath << "/" << ent->d_name)

            std::string mappedPathWithFile(mappedPath + "/" + ent->d_name);
            DIR* dir2;
            if ((dir2 = opendir(mappedPathWithFile.c_str())) == nullptr) {
                closedir(dir);
                throw RuntimeException("can't access directory: " + mappedPathWithFile);
            }

            struct dirent* ent2;
            while ((ent2 = readdir(dir2)) != nullptr) {
                if (strcmp(ent2->d_name, ".") == 0 || strcmp(ent2->d_name, "..") == 0)
                    continue;

                std::string fileName(mappedPath + "/" + ent->d_name + "/" + ent2->d_name);
                TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << fileName)

                uint64_t sequence = getSequenceFromFileName(replicator, ent2->d_name);

                TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: found seq: " << sequence)

                if (sequence == 0 || sequence < replicator->metadata->sequence)
                    continue;

                auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                          replicator->transactionBuffer, 0, fileName);

                parser->firstScn = ZERO_SCN;
                parser->nextScn = ZERO_SCN;
                parser->sequence = sequence;
                replicator->archiveRedoQueue.push(parser);
            }
            closedir(dir2);

            if (newLastCheckedDay.length() == 0 || (newLastCheckedDay == ent->d_name))
                newLastCheckedDay = ent->d_name;
        }
        closedir(dir);

        if (newLastCheckedDay.length() != 0 && (replicator->lastCheckedDay.length() == 0 || (replicator->lastCheckedDay.length() > 0 &&
                replicator->lastCheckedDay.compare(newLastCheckedDay) < 0))) {
            TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: updating last checked day to: " << newLastCheckedDay)
            replicator->lastCheckedDay = newLastCheckedDay;
        }
    }

    void Replicator::archGetLogList(Replicator* replicator) {
        Ctx* ctx = replicator->ctx;

        uint64_t sequenceStart = ZERO_SEQ;
        for (std::string& mappedPath : replicator->redoLogsBatch) {
            TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath)

            struct stat fileStat;
            if (stat(mappedPath.c_str(), &fileStat)) {
                WARNING("Reading information for file: " << mappedPath << " - " << strerror(errno))
                continue;
            }

            // Single file
            if (!S_ISDIR(fileStat.st_mode)) {
                TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << mappedPath)

                // Getting file name from path
                const char* fileName = mappedPath.c_str();
                uint64_t j = mappedPath.length();
                while (j > 0) {
                    if (fileName[j - 1] == '/')
                        break;
                    --j;
                }
                uint64_t sequence = getSequenceFromFileName(replicator, fileName + j);

                TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: found seq: " << sequence)

                if (sequence == 0 || sequence < replicator->metadata->sequence)
                    continue;

                auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                          replicator->transactionBuffer, 0, mappedPath);
                parser->firstScn = ZERO_SCN;
                parser->nextScn = ZERO_SCN;
                parser->sequence = sequence;
                replicator->archiveRedoQueue.push(parser);
                if (sequenceStart == ZERO_SEQ || sequenceStart > sequence)
                    sequenceStart = sequence;

            // Dir, check all files
            } else {
                DIR* dir;
                if ((dir = opendir(mappedPath.c_str())) == nullptr)
                    throw RuntimeException("can't access directory: " + mappedPath);

                struct dirent* ent;
                while ((ent = readdir(dir)) != nullptr) {
                    if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                        continue;

                    std::string fileName(mappedPath + "/" + ent->d_name);
                    TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: checking path: " << fileName)

                    uint64_t sequence = getSequenceFromFileName(replicator, ent->d_name);

                    TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: found seq: " << sequence)

                    if (sequence == 0 || sequence < replicator->metadata->sequence)
                        continue;

                    auto* parser = new Parser(replicator->ctx,replicator->builder, replicator->metadata, replicator->transactionBuffer,
                                             0, fileName);
                    parser->firstScn = ZERO_SCN;
                    parser->nextScn = ZERO_SCN;
                    parser->sequence = sequence;
                    replicator->archiveRedoQueue.push(parser);
                }
                closedir(dir);
            }
        }

        if (sequenceStart != ZERO_SEQ && replicator->metadata->sequence == 0)
            replicator->metadata->setSeqOffset(sequenceStart, 0);
        replicator->redoLogsBatch.clear();
    }

    bool parserCompare::operator()(Parser* const& p1, Parser* const& p2) {
        return p1->sequence > p2->sequence;
    }

    bool parserCompareReverse::operator()(Parser* const& p1, Parser* const& p2) {
        return p1->sequence < p2->sequence;
    }

    void Replicator::updateResetlogs() {
        for (OracleIncarnation* oi : metadata->oracleIncarnations) {
            if (oi->resetlogs == metadata->resetlogs) {
                metadata->oracleIncarnationCurrent = oi;
                break;
            }
        }

        // Resetlogs is changed
        for (OracleIncarnation* oi : metadata->oracleIncarnations) {
            if (oi->resetlogsScn == metadata->nextScn &&
                    metadata->oracleIncarnationCurrent->resetlogs == metadata->resetlogs &&
                    oi->priorIncarnation == metadata->oracleIncarnationCurrent->incarnation) {
                INFO("new resetlogs detected: " << std::dec << oi->resetlogs)
                metadata->setResetlogs(oi->resetlogs);
                metadata->sequence = 0;
                metadata->offset = 0;
                return;
            }
        }

        if (metadata->oracleIncarnations.empty())
            return;

        if (metadata->oracleIncarnationCurrent == nullptr)
            throw RuntimeException("resetlogs (" + std::to_string(metadata->resetlogs) + ") not found in incarnation list");
    }

    void Replicator::wakeUp() {
        metadata->wakeUp();
    }

    void Replicator::printStartMsg() {
        std::string flagsStr;
        if (ctx->flags)
            flagsStr = " (flags: " + std::to_string(ctx->flags) + ")";

        std::string starting;
        if (metadata->startTime.length() > 0)
            starting = "time: " + metadata->startTime;
        else if (metadata->startTimeRel > 0)
            starting = "time-rel: " + std::to_string(metadata->startTimeRel);
        else if (metadata->startScn != ZERO_SCN)
            starting = "scn: " + std::to_string(metadata->startScn);
        else
            starting = "now";

        std::string startingSeq;
        if (metadata->startSequence != ZERO_SEQ)
            startingSeq = ", seq: " + std::to_string(metadata->startSequence);

        INFO("Oracle Replicator for " << database << " in " << getModeName() << " mode is starting" << flagsStr
                                      << " from " << starting << startingSeq)
    }

    bool Replicator::processArchivedRedoLogs() {
        uint64_t ret = REDO_OK;
        Parser* parser;
        bool logsProcessed = false;

        while (!ctx->softShutdown) {
            TRACE(TRACE2_REDO, "REDO: checking archived redo logs, seq: " << std::dec << metadata->sequence)
            updateResetlogs();
            archGetLog(this);

            if (archiveRedoQueue.empty()) {
                if (FLAG(REDO_FLAGS_ARCH_ONLY)) {
                    TRACE(TRACE2_ARCHIVE_LIST, "ARCHIVE LIST: archived redo log missing for seq: " << std::dec << metadata->sequence << ", sleeping")
                    usleep(ctx->archReadSleepUs);
                } else {
                    break;
                }
            }

            TRACE(TRACE2_REDO, "REDO: searching archived redo log for seq: " << std::dec << metadata->sequence)
            while (!archiveRedoQueue.empty() && !ctx->softShutdown) {
                parser = archiveRedoQueue.top();
                TRACE(TRACE2_REDO, "REDO: " << parser->path << " is seq: " << std::dec << parser->sequence << ", scn: " << std::dec << parser->firstScn)

                // When no metadata exists start processing from first file
                if (metadata->sequence == 0)
                    metadata->sequence = parser->sequence;

                // Skip older archived redo logs
                if (parser->sequence < metadata->sequence) {
                    archiveRedoQueue.pop();
                    delete parser;
                    continue;
                } else if (parser->sequence > metadata->sequence) {
                    WARNING("Couldn't find archive log for seq: " + std::to_string(metadata->sequence) + ", found: " +
                                           std::to_string(parser->sequence) + ", sleeping " << std::dec << ctx->archReadSleepUs << " us")
                    usleep(ctx->archReadSleepUs);
                    cleanArchList();
                    archGetLog(this);
                    continue;
                }

                logsProcessed = true;
                parser->reader = archReader;

                archReader->fileName = parser->path;
                uint64_t retry = ctx->archReadTries;

                while (true) {
                    if (archReader->checkRedoLog() && archReader->updateRedoLog()) {
                        break;
                    }

                    if (retry == 0)
                        throw RuntimeException("opening archived redo log: " + parser->path);

                    INFO("archived redo log " << parser->path << " is not ready for read, sleeping " << std::dec << ctx->archReadSleepUs << " us")
                    usleep(ctx->archReadSleepUs);
                    --retry;
                }

                ret = parser->parse();
                metadata->firstScn = parser->firstScn;
                metadata->nextScn = parser->nextScn;

                if (ctx->softShutdown)
                    break;

                if (ret != REDO_FINISHED) {
                    if  (ret == REDO_STOPPED) {
                        archiveRedoQueue.pop();
                        delete parser;
                        break;
                    }
                    throw RuntimeException(std::string("archive log processing returned: ") + Reader::REDO_CODE[ret] + " (code: " +
                            std::to_string(ret) + ")");
                }

                // verifySchema(metadata->nextScn);

                ++metadata->sequence;
                archiveRedoQueue.pop();
                delete parser;

                if (ctx->stopLogSwitches > 0) {
                    --ctx->stopLogSwitches;
                    if (ctx->stopLogSwitches == 0) {
                        INFO("shutdown started - exhausted number of log switches")
                        ctx->stopSoft();
                    }
                }
            }

            if (!logsProcessed)
                break;
        }

        return logsProcessed;
    }

    bool Replicator::processOnlineRedoLogs() {
        uint64_t ret = REDO_OK;
        Parser* parser;
        bool logsProcessed = false;

        TRACE(TRACE2_REDO, "REDO: checking online redo logs, seq: " << std::dec << metadata->sequence)
        updateResetlogs();
        updateOnlineLogs();

        while (!ctx->softShutdown) {
            parser = nullptr;
            TRACE(TRACE2_REDO, "REDO: searching online redo log for seq: " << std::dec << metadata->sequence)

            // Keep reading online redo logs while it is possible
            bool higher = false;
            clock_t beginTime = Timer::getTime();

            while (!ctx->softShutdown) {
                for (Parser* onlineRedo : onlineRedoSet) {
                    if (onlineRedo->reader->getSequence() > metadata->sequence)
                        higher = true;

                    if (onlineRedo->reader->getSequence() == metadata->sequence &&
                            (onlineRedo->reader->getNumBlocks() == ZERO_BLK || metadata->offset < onlineRedo->reader->getNumBlocks() *
                            onlineRedo->reader->getBlockSize())) {
                        parser = onlineRedo;
                    }

                    TRACE(TRACE2_REDO, "REDO: " << onlineRedo->path << " is seq: " << std::dec << onlineRedo->sequence <<
                            ", scn: " << std::dec << onlineRedo->firstScn << ", blocks: " << std::dec << onlineRedo->reader->getNumBlocks())
                }

                // All so far read, waiting for switch
                if (parser == nullptr && !higher) {
                    usleep(ctx->redoReadSleepUs);
                } else
                    break;

                if (ctx->softShutdown)
                    break;

                clock_t endTime = Timer::getTime();
                if (beginTime + (clock_t)ctx->refreshIntervalUs < endTime) {
                    updateOnlineRedoLogData();
                    updateOnlineLogs();
                    goStandby();
                    break;
                }

                updateOnlineLogs();
            }

            if (parser == nullptr)
                break;

            // If online redo log is overwritten - then switch to reading archive logs
            if (ctx->softShutdown)
                break;
            logsProcessed = true;

            ret = parser->parse();
            metadata->firstScn = parser->firstScn;
            metadata->nextScn = parser->nextScn;

            if (ctx->softShutdown)
                break;

            if (ret == REDO_FINISHED) {
                // verifySchema(metadata->nextScn);
                ++metadata->sequence;
            } else if (ret == REDO_STOPPED || ret == REDO_OK) {
                updateOnlineRedoLogData();
                updateOnlineLogs();
            } else if (ret == REDO_OVERWRITTEN) {
                INFO("online redo log has been overwritten by new ctx, continuing reading from archived redo log")
                break;
            } else {
                if (parser->group == 0) {
                    throw RuntimeException("read archived redo log (code: " + std::to_string(ret) + ")");
                } else {
                    throw RuntimeException("read online redo log (code: " + std::to_string(ret) + ")");
                }
            }

            if (ctx->stopLogSwitches > 0) {
                --ctx->stopLogSwitches;
                if (ctx->stopLogSwitches == 0) {
                    INFO("shutdown initiated by number of log switches")
                    ctx->stopSoft();
                }
            }
        }
        return logsProcessed;
    }
}
