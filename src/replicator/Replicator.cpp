/* Thread reading database redo Logs using offline mode
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

#include <cerrno>
#include <cstddef>
#include <dirent.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/Clock.h"
#include "../common/Ctx.h"
#include "../common/DbIncarnation.h"
#include "../common/exception/BootException.h"
#include "../common/exception/RedoLogException.h"
#include "../common/exception/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "../metadata/RedoLog.h"
#include "../metadata/Schema.h"
#include "../parser/Parser.h"
#include "../parser/Transaction.h"
#include "../reader/ReaderFilesystem.h"
#include "Replicator.h"

namespace OpenLogReplicator {
    Replicator::Replicator(Ctx* newCtx, void (* newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                           TransactionBuffer* newTransactionBuffer, const std::string& newAlias, const char* newDatabase) :
            Thread(newCtx, newAlias),
            archGetLog(newArchGetLog),
            builder(newBuilder),
            metadata(newMetadata),
            transactionBuffer(newTransactionBuffer),
            database(newDatabase) {
        ctx->parserThread = this;
    }

    Replicator::~Replicator() {
        readerDropAll();

        while (!archiveRedoQueue.empty()) {
            Parser* parser = archiveRedoQueue.top();
            archiveRedoQueue.pop();
            delete parser;
        }

        for (Parser* parser: onlineRedoSet)
            delete parser;
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
        for (Parser* onlineRedo: onlineRedoSet) {
            if (!onlineRedo->reader->updateRedoLog())
                throw RuntimeException(10039, "updating of online redo logs failed for " + onlineRedo->path);
            onlineRedo->sequence = onlineRedo->reader->getSequence();
            onlineRedo->firstScn = onlineRedo->reader->getFirstScn();
            onlineRedo->nextScn = onlineRedo->reader->getNextScn();
        }
    }

    void Replicator::readerDropAll() {
        for (;;) {
            bool wakingUp = false;
            for (Reader* reader: readers) {
                if (!reader->finished) {
                    reader->wakeUp();
                    wakingUp = true;
                }
            }
            if (!wakingUp)
                break;
            contextSet(CONTEXT::SLEEP);
            usleep(1000);
            contextSet(CONTEXT::CPU);
        }

        while (!readers.empty()) {
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
        if (metadata->startSequence != Ctx::ZERO_SEQ)
            metadata->setSeqOffset(metadata->startSequence, 0);
        else
            metadata->setSeqOffset(0, 0);
    }

    void Replicator::verifySchema(typeScn currentScn __attribute__((unused))) {
        // Nothing for offline mode
    }

    void Replicator::createSchema() {
        if (ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS)) {
            metadata->allowCheckpoints();
            return;
        }

        throw RuntimeException(10040, "schema file missing");
    }

    void Replicator::updateOnlineRedoLogData() {
        int64_t lastGroup = -1;
        Reader* onlineReader = nullptr;

        for (auto* redoLog: metadata->redoLogs) {
            if (redoLog->group != lastGroup || onlineReader == nullptr) {
                onlineReader = readerCreate(redoLog->group);
                onlineReader->paths.clear();
                lastGroup = redoLog->group;
            }
            onlineReader->paths.push_back(redoLog->path);
        }

        checkOnlineRedoLogs();
    }

    void Replicator::run() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "replicator (" + ss.str() + ") start");
        }

        try {
            metadata->waitForWriter(this);

            loadDatabaseMetadata();
            metadata->readCheckpoints();
            if (!ctx->isFlagSet(Ctx::REDO_FLAGS::ARCH_ONLY))
                updateOnlineRedoLogData();
            ctx->info(0, "timezone: " + Ctx::timezoneToString(-timezone) + ", db-timezone: " + Ctx::timezoneToString(metadata->dbTimezone) +
                         ", log-timezone: " + Ctx::timezoneToString(ctx->logTimezone) + ", host-timezone: " + Ctx::timezoneToString(ctx->hostTimezone));

            do {
                if (ctx->softShutdown)
                    break;
                metadata->waitForWriter(ctx->parserThread);

                if (metadata->status == Metadata::STATUS::READY)
                    continue;

                if (ctx->softShutdown)
                    break;
                try {
                    printStartMsg();
                    if (metadata->resetlogs != 0)
                        ctx->info(0, "current resetlogs is: " + std::to_string(metadata->resetlogs));
                    if (metadata->firstDataScn != Ctx::ZERO_SCN)
                        ctx->info(0, "first data SCN: " + std::to_string(metadata->firstDataScn));
                    if (metadata->firstSchemaScn != Ctx::ZERO_SCN)
                        ctx->info(0, "first schema SCN: " + std::to_string(metadata->firstSchemaScn));

                    if (metadata->firstDataScn == Ctx::ZERO_SCN || metadata->sequence == Ctx::ZERO_SEQ)
                        positionReader();

                    // No schema available?
                    if (metadata->schema->scn == Ctx::ZERO_SCN)
                        createSchema();
                    else
                        metadata->allowCheckpoints();
                    metadata->schema->updateXmlCtx();

                    if (metadata->sequence == Ctx::ZERO_SEQ)
                        throw BootException(10028, "starting sequence is unknown");

                    if (metadata->firstDataScn == Ctx::ZERO_SCN)
                        ctx->info(0, "last confirmed scn: <none>, starting sequence: " + std::to_string(metadata->sequence) + ", offset: " +
                                     std::to_string(metadata->offset));
                    else
                        ctx->info(0, "last confirmed scn: " + std::to_string(metadata->firstDataScn) + ", starting sequence: " +
                                     std::to_string(metadata->sequence) + ", offset: " + std::to_string(metadata->offset));

                    if ((metadata->dbBlockChecksum == "OFF" || metadata->dbBlockChecksum == "FALSE") &&
                            !ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::BLOCK_SUM)) {
                        ctx->hint("set DB_BLOCK_CHECKSUM = TYPICAL on the database or turn off consistency checking in OpenLogReplicator "
                                  "setting parameter disable-checks: " + std::to_string(static_cast<uint>(Ctx::DISABLE_CHECKS::BLOCK_SUM)) +
                                  " for the reader");
                    }

                } catch (BootException& ex) {
                    if (!metadata->bootFailsafe)
                        throw RuntimeException(ex.code, ex.msg);

                    ctx->error(ex.code, ex.msg);
                    ctx->info(0, "replication startup failed, waiting for further commands");
                    metadata->setStatusReady(this);
                    continue;
                }

                // Boot succeeded
                ctx->info(0, "resume writer");
                metadata->setStatusReplicate(this);
            } while (metadata->status != Metadata::STATUS::REPLICATE);

            while (!ctx->softShutdown) {
                bool logsProcessed = false;

                logsProcessed |= processArchivedRedoLogs();
                if (ctx->softShutdown)
                    break;

                if (!continueWithOnline())
                    break;
                if (ctx->softShutdown)
                    break;

                if (!ctx->isFlagSet(Ctx::REDO_FLAGS::ARCH_ONLY))
                    logsProcessed |= processOnlineRedoLogs();
                if (ctx->softShutdown)
                    break;

                if (!logsProcessed) {
                    contextSet(CONTEXT::SLEEP);
                    usleep(ctx->redoReadSleepUs);
                    contextSet(CONTEXT::CPU);
                }
            }
        } catch (DataException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (RedoLogException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (std::bad_alloc& ex) {
            ctx->error(10018, "memory allocation failed: " + std::string(ex.what()));
            ctx->stopHard();
        }

        ctx->info(0, "Replicator for: " + database + " is shutting down");
        transactionBuffer->purge();

        ctx->replicatorFinished = true;
        ctx->printMemoryUsageHWM();

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "replicator (" + ss.str() + ") stop");
        }
    }

    Reader* Replicator::readerCreate(int group) {
        for (Reader* reader: readers)
            if (reader->getGroup() == group)
                return reader;

        auto* readerFS = new ReaderFilesystem(ctx, alias + "-reader-" + std::to_string(group), database, group,
                                              metadata->dbBlockChecksum != "OFF" && metadata->dbBlockChecksum != "FALSE");
        readers.insert(readerFS);
        readerFS->initialize();

        ctx->spawnThread(readerFS);
        return readerFS;
    }

    void Replicator::checkOnlineRedoLogs() {
        for (Parser* onlineRedo: onlineRedoSet)
            delete onlineRedo;
        onlineRedoSet.clear();

        for (Reader* reader: readers) {
            if (reader->getGroup() == 0)
                continue;

            bool foundPath = false;
            for (const std::string& path: reader->paths) {
                reader->fileName = path;
                applyMapping(reader->fileName);
                if (reader->checkRedoLog()) {
                    foundPath = true;
                    auto* parser = new Parser(ctx, builder, metadata, transactionBuffer,
                                             reader->getGroup(), reader->fileName);

                    parser->reader = reader;
                    ctx->info(0, "online redo log: " + reader->fileName);
                    onlineRedoSet.insert(parser);
                    break;
                }
            }

            if (!foundPath) {
                const int64_t badGroup = reader->getGroup();
                for (const std::string& path: reader->paths) {
                    std::string pathMapped(path);
                    applyMapping(pathMapped);
                    ctx->hint("check mapping, failed to read: " + pathMapped);
                }
                throw RuntimeException(10027, "can't read any member of group " + std::to_string(badGroup));
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
    typeSeq Replicator::getSequenceFromFileName(Replicator* replicator, const std::string& file) {
        typeSeq sequence = 0;
        size_t i = 0;
        size_t j = 0;

        while (i < replicator->metadata->logArchiveFormat.length() && j < file.length()) {
            if (replicator->metadata->logArchiveFormat[i] == '%') {
                if (i + 1 >= replicator->metadata->logArchiveFormat.length()) {
                    replicator->ctx->warning(60028, "can't get sequence from file: " + file + " log_archive_format: " +
                                                    replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                                    std::to_string(i) + ", found end after %");
                    return 0;
                }
                uint digits = 0;
                if (replicator->metadata->logArchiveFormat[i + 1] == 's' || replicator->metadata->logArchiveFormat[i + 1] == 'S' ||
                    replicator->metadata->logArchiveFormat[i + 1] == 't' || replicator->metadata->logArchiveFormat[i + 1] == 'T' ||
                    replicator->metadata->logArchiveFormat[i + 1] == 'r' || replicator->metadata->logArchiveFormat[i + 1] == 'a' ||
                    replicator->metadata->logArchiveFormat[i + 1] == 'd') {
                    // Some [0-9]*
                    typeSeq number = 0;
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
                    replicator->ctx->warning(60028, "can't get sequence from file: " + file + " log_archive_format: " +
                                                    replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                                    std::to_string(i) + ", found no number/hash");
                    return 0;
                }
            } else if (file[j] == replicator->metadata->logArchiveFormat[i]) {
                ++i;
                ++j;
            } else {
                replicator->ctx->warning(60028, "can't get sequence from file: " + file + " log_archive_format: " +
                                                replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                                std::to_string(i) + ", found different values");
                return 0;
            }
        }

        if (i == replicator->metadata->logArchiveFormat.length() && j == file.length())
            return sequence;

        replicator->ctx->warning(60028, "error getting sequence from file: " + file + " log_archive_format: " +
                                        replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                        std::to_string(i) + ", found no sequence");
        return 0;
    }

    void Replicator::addPathMapping(const char* source, const char* target) {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
            ctx->logTrace(Ctx::TRACE::FILE, "added mapping [" + std::string(source) + "] -> [" + target + "]");
        const std::string sourceMapping(source);
        const std::string targetMapping(target);
        pathMapping.push_back(sourceMapping);
        pathMapping.push_back(targetMapping);
    }

    void Replicator::addRedoLogsBatch(const char* path) {
        redoLogsBatch.emplace_back(path);
    }

    void Replicator::applyMapping(std::string& path) {
        const size_t newPathLength = path.length();
        std::array<char, Ctx::MAX_PATH_LENGTH> pathBuffer {};

        for (size_t i = 0; i < pathMapping.size() / 2; ++i) {
            const uint64_t sourceLength = pathMapping[i * 2].length();
            const uint64_t targetLength = pathMapping[(i * 2) + 1].length();

            if (sourceLength <= newPathLength &&
                newPathLength - sourceLength + targetLength < Ctx::MAX_PATH_LENGTH - 1 &&
                memcmp(path.c_str(), pathMapping[i * 2].c_str(), sourceLength) == 0) {

                memcpy(reinterpret_cast<void*>(pathBuffer.data()),
                       reinterpret_cast<const void*>(pathMapping[(i * 2) + 1].c_str()), targetLength);
                memcpy(reinterpret_cast<void*>(pathBuffer.data() + targetLength),
                       reinterpret_cast<const void*>(path.c_str() + sourceLength), newPathLength - sourceLength);
                pathBuffer[newPathLength - sourceLength + targetLength] = 0;
                path.assign(pathBuffer.data());
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
        if (replicator->metadata->logArchiveFormat.empty())
            throw RuntimeException(10044, "missing location of archived redo logs for offline mode");

        std::string mappedPath(replicator->metadata->dbRecoveryFileDest + "/" + replicator->metadata->context + "/archivelog");
        replicator->applyMapping(mappedPath);
        if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
            replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath);

        DIR* dir = opendir(mappedPath.c_str());
        if (dir == nullptr)
            throw RuntimeException(10012, "directory: " + mappedPath + " - can't read");

        std::string newLastCheckedDay;
        const struct dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;

            struct stat fileStat{};
            const std::string mappedSubPath(mappedPath + "/" + ent->d_name);
            if (stat(mappedSubPath.c_str(), &fileStat) != 0) {
                replicator->ctx->warning(10003, "file: " + mappedSubPath + " - get metadata returned: " + strerror(errno));
                continue;
            }

            if (!S_ISDIR(fileStat.st_mode))
                continue;

            // Skip earlier days
            if (replicator->lastCheckedDay.empty() && replicator->lastCheckedDay == ent->d_name)
                continue;

            if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath + "/" + ent->d_name);

            const std::string mappedPathWithFile(mappedPath + "/" + ent->d_name);
            DIR* dir2 = opendir(mappedPathWithFile.c_str());
            if (dir2 == nullptr) {
                closedir(dir);
                throw RuntimeException(10012, "directory: " + mappedPathWithFile + " - can't read");
            }

            const struct dirent* ent2;
            while ((ent2 = readdir(dir2)) != nullptr) {
                if (strcmp(ent2->d_name, ".") == 0 || strcmp(ent2->d_name, "..") == 0)
                    continue;

                const std::string fileName(mappedPath + "/" + ent->d_name + "/" + ent2->d_name);
                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + fileName);

                const typeSeq sequence = getSequenceFromFileName(replicator, ent2->d_name);

                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "found seq: " + std::to_string(sequence));

                if (sequence == 0 || sequence < replicator->metadata->sequence)
                    continue;

                auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                          replicator->transactionBuffer, 0, fileName);

                parser->firstScn = Ctx::ZERO_SCN;
                parser->nextScn = Ctx::ZERO_SCN;
                parser->sequence = sequence;
                replicator->archiveRedoQueue.push(parser);
            }
            closedir(dir2);

            if (newLastCheckedDay.empty() || (newLastCheckedDay != ent->d_name))
                newLastCheckedDay = ent->d_name;
        }
        closedir(dir);

        if (!newLastCheckedDay.empty() || (!replicator->lastCheckedDay.empty() && replicator->lastCheckedDay.compare(newLastCheckedDay) < 0)) {
            if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "updating last checked day to: " + newLastCheckedDay);
            replicator->lastCheckedDay = newLastCheckedDay;
        }
    }

    void Replicator::archGetLogList(Replicator* replicator) {
        typeSeq sequenceStart = Ctx::ZERO_SEQ;
        for (const std::string& mappedPath: replicator->redoLogsBatch) {
            if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath);

            struct stat fileStat{};
            if (stat(mappedPath.c_str(), &fileStat) != 0) {
                replicator->ctx->warning(10003, "file: " + mappedPath + " - get metadata returned: " + strerror(errno));
                continue;
            }

            // Single file
            if (!S_ISDIR(fileStat.st_mode)) {
                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath);

                // Getting file name from the path
                const char* fileName = mappedPath.c_str();
                size_t j = mappedPath.length();
                while (j > 0) {
                    if (fileName[j - 1] == '/')
                        break;
                    --j;
                }
                const typeSeq sequence = getSequenceFromFileName(replicator, fileName + j);

                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "found seq: " + std::to_string(sequence));

                if (sequence == 0 || sequence < replicator->metadata->sequence)
                    continue;

                auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                          replicator->transactionBuffer, 0, mappedPath);
                parser->firstScn = Ctx::ZERO_SCN;
                parser->nextScn = Ctx::ZERO_SCN;
                parser->sequence = sequence;
                replicator->archiveRedoQueue.push(parser);
                if (sequenceStart == Ctx::ZERO_SEQ || sequenceStart > sequence)
                    sequenceStart = sequence;

            } else {
                // Dir, check all files
                DIR* dir = opendir(mappedPath.c_str());
                if (dir == nullptr)
                    throw RuntimeException(10012, "directory: " + mappedPath + " - can't read");

                const struct dirent* ent;
                while ((ent = readdir(dir)) != nullptr) {
                    if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                        continue;

                    const std::string fileName(mappedPath + "/" + ent->d_name);
                    if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                        replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + fileName);

                    const typeSeq sequence = getSequenceFromFileName(replicator, ent->d_name);

                    if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                        replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "found seq: " + std::to_string(sequence));

                    if (sequence == 0 || sequence < replicator->metadata->sequence)
                        continue;

                    auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                             replicator->transactionBuffer, 0, fileName);
                    parser->firstScn = Ctx::ZERO_SCN;
                    parser->nextScn = Ctx::ZERO_SCN;
                    parser->sequence = sequence;
                    replicator->archiveRedoQueue.push(parser);
                }
                closedir(dir);
            }
        }

        if (sequenceStart != Ctx::ZERO_SEQ && replicator->metadata->sequence == 0)
            replicator->metadata->setSeqOffset(sequenceStart, 0);
        replicator->redoLogsBatch.clear();
    }

    bool parserCompare::operator()(const Parser* p1, const Parser* p2) {
        return p1->sequence > p2->sequence;
    }

    void Replicator::updateResetlogs() {
        contextSet(CONTEXT::MUTEX, REASON::REPLICATOR_UPDATE);
        std::unique_lock<std::mutex> const lck(metadata->mtxCheckpoint);

        for (DbIncarnation* oi: metadata->dbIncarnations) {
            if (oi->resetlogs == metadata->resetlogs) {
                metadata->dbIncarnationCurrent = oi;
                break;
            }
        }

        // Resetlogs is changed
        for (const DbIncarnation* oi: metadata->dbIncarnations) {
            if (oi->resetlogsScn == metadata->nextScn &&
                metadata->dbIncarnationCurrent->resetlogs == metadata->resetlogs &&
                oi->priorIncarnation == metadata->dbIncarnationCurrent->incarnation) {
                ctx->info(0, "new resetlogs detected: " + std::to_string(oi->resetlogs));
                metadata->setResetlogs(oi->resetlogs);
                metadata->sequence = 0;
                metadata->offset = 0;
                contextSet(CONTEXT::CPU);
                return;
            }
        }

        if (metadata->dbIncarnations.empty()) {
            contextSet(CONTEXT::CPU);
            return;
        }

        if (metadata->dbIncarnationCurrent == nullptr) {
            contextSet(CONTEXT::CPU);
            throw RuntimeException(10045, "resetlogs (" + std::to_string(metadata->resetlogs) + ") not found in incarnation list");
        }
        contextSet(CONTEXT::CPU);
    }

    void Replicator::wakeUp() {
        metadata->wakeUp(this);
    }

    void Replicator::printStartMsg() const {
        std::string flagsStr;
        if (ctx->flags != 0)
            flagsStr = " (flags: " + std::to_string(ctx->flags) + ")";

        std::string starting;
        if (!metadata->startTime.empty())
            starting = "time: " + metadata->startTime;
        else if (metadata->startTimeRel > 0)
            starting = "time-rel: " + std::to_string(metadata->startTimeRel);
        else if (metadata->startScn != Ctx::ZERO_SCN)
            starting = "scn: " + std::to_string(metadata->startScn);
        else
            starting = "NOW";

        std::string startingSeq;
        if (metadata->startSequence != Ctx::ZERO_SEQ)
            startingSeq = ", seq: " + std::to_string(metadata->startSequence);

        ctx->info(0, "Replicator for " + database + " in " + getModeName() + " mode is starting" + flagsStr + " from " + starting +
                     startingSeq);
    }

    bool Replicator::processArchivedRedoLogs() {
        Reader::REDO_CODE ret;
        Parser* parser;
        bool logsProcessed = false;

        while (!ctx->softShutdown) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                ctx->logTrace(Ctx::TRACE::REDO, "checking archived redo logs, seq: " + std::to_string(metadata->sequence));
            updateResetlogs();
            archGetLog(this);

            if (archiveRedoQueue.empty()) {
                if (ctx->isFlagSet(Ctx::REDO_FLAGS::ARCH_ONLY)) {
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                        ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "archived redo log missing for seq: " + std::to_string(metadata->sequence) +
                                                                ", sleeping");
                    contextSet(CONTEXT::SLEEP);
                    usleep(ctx->archReadSleepUs);
                    contextSet(CONTEXT::CPU);
                } else {
                    break;
                }
            }

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
                std::ostringstream ss;
                ss << std::this_thread::get_id();
                ctx->logTrace(Ctx::TRACE::REDO, "searching archived redo log for seq: " + std::to_string(metadata->sequence));
            }
            while (!archiveRedoQueue.empty() && !ctx->softShutdown) {
                parser = archiveRedoQueue.top();
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                    ctx->logTrace(Ctx::TRACE::REDO, parser->path + " is seq: " + std::to_string(parser->sequence) + ", scn: " +
                                                    std::to_string(parser->firstScn));

                // When no metadata exists, start processing from the first file
                if (metadata->sequence == 0) {
                    contextSet(CONTEXT::MUTEX, REASON::REPLICATOR_ARCH);
                    std::unique_lock<std::mutex> const lck(metadata->mtxCheckpoint);
                    metadata->sequence = parser->sequence;
                    contextSet(CONTEXT::CPU);
                }

                // Skip older archived redo logs
                if (parser->sequence < metadata->sequence) {
                    archiveRedoQueue.pop();
                    delete parser;
                    continue;
                }

                if (parser->sequence > metadata->sequence) {
                    ctx->warning(60027, "couldn't find archive log for seq: " + std::to_string(metadata->sequence) + ", found: " +
                                        std::to_string(parser->sequence) + ", sleeping " + std::to_string(ctx->archReadSleepUs) + " us");
                    contextSet(CONTEXT::SLEEP);
                    usleep(ctx->archReadSleepUs);
                    contextSet(CONTEXT::CPU);
                    cleanArchList();
                    archGetLog(this);
                    continue;
                }

                logsProcessed = true;
                parser->reader = archReader;

                archReader->fileName = parser->path;
                uint retry = ctx->archReadTries;

                while (true) {
                    if (archReader->checkRedoLog() && archReader->updateRedoLog()) {
                        break;
                    }

                    if (retry == 0)
                        throw RuntimeException(10009, "file: " + parser->path + " - failed to open after " +
                                                      std::to_string(ctx->archReadTries) + " tries");

                    ctx->info(0, "archived redo log " + parser->path + " is not ready for read, sleeping " +
                                 std::to_string(ctx->archReadSleepUs) + " us");
                    contextSet(CONTEXT::SLEEP);
                    usleep(ctx->archReadSleepUs);
                    contextSet(CONTEXT::CPU);
                    --retry;
                }

                ret = parser->parse();
                metadata->firstScn = parser->firstScn;
                metadata->nextScn = parser->nextScn;

                if (ctx->softShutdown)
                    break;

                if (ret != Reader::REDO_CODE::FINISHED) {
                    if (ret == Reader::REDO_CODE::STOPPED) {
                        archiveRedoQueue.pop();
                        delete parser;
                        break;
                    }
                    throw RuntimeException(10047, "archive log processing returned: " + std::string(Reader::REDO_MSG[static_cast<uint>(ret)]) + ", code: " +
                                                  std::to_string(static_cast<uint>(ret)));
                }

                // verifySchema(metadata->nextScn);

                ++metadata->sequence;
                archiveRedoQueue.pop();
                delete parser;

                if (ctx->stopLogSwitches > 0) {
                    --ctx->stopLogSwitches;
                    if (ctx->stopLogSwitches == 0) {
                        ctx->info(0, "shutdown started - exhausted number of log switches");
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
        Parser* parser;
        bool logsProcessed = false;

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
            ctx->logTrace(Ctx::TRACE::REDO, "checking online redo logs, seq: " + std::to_string(metadata->sequence));
        updateResetlogs();
        updateOnlineLogs();

        while (!ctx->softShutdown) {
            parser = nullptr;
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                ctx->logTrace(Ctx::TRACE::REDO, "searching online redo log for seq: " + std::to_string(metadata->sequence));

            // Keep reading online redo logs while it is possible
            bool higher = false;
            const time_ut beginTime = ctx->clock->getTimeUt();

            while (!ctx->softShutdown) {
                for (Parser* onlineRedo: onlineRedoSet) {
                    if (onlineRedo->reader->getSequence() > metadata->sequence)
                        higher = true;

                    if (onlineRedo->reader->getSequence() == metadata->sequence &&
                            (onlineRedo->reader->getNumBlocks() == Ctx::ZERO_BLK || metadata->offset <
                            static_cast<uint64_t>(onlineRedo->reader->getNumBlocks() * static_cast<uint64_t>(onlineRedo->reader->getBlockSize())))) {
                        parser = onlineRedo;
                    }

                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO) && ctx->logLevel >= Ctx::LOG::DEBUG))
                        ctx->logTrace(Ctx::TRACE::REDO, onlineRedo->path + " is seq: " + std::to_string(onlineRedo->sequence) +
                                                        ", scn: " + std::to_string(onlineRedo->firstScn) + ", blocks: " +
                                                        std::to_string(onlineRedo->reader->getNumBlocks()));
                }

                // All so far read, waiting for switch
                if (parser == nullptr && !higher) {
                    contextSet(CONTEXT::SLEEP);
                    usleep(ctx->redoReadSleepUs);
                    contextSet(CONTEXT::CPU);
                } else
                    break;

                if (ctx->softShutdown)
                    break;

                const time_ut endTime = ctx->clock->getTimeUt();
                if (beginTime + static_cast<time_ut>(ctx->refreshIntervalUs) < endTime) {
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                        ctx->logTrace(Ctx::TRACE::REDO, "refresh interval reached, checking online redo logs again");

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

            const Reader::REDO_CODE ret = parser->parse();
            metadata->setFirstNextScn(parser->firstScn, parser->nextScn);

            if (ctx->softShutdown)
                break;

            if (ret == Reader::REDO_CODE::FINISHED) {
                // verifySchema(metadata->nextScn);
                metadata->setNextSequence();
            } else if (ret == Reader::REDO_CODE::STOPPED || ret == Reader::REDO_CODE::OK) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                    ctx->logTrace(Ctx::TRACE::REDO, "updating redo log files, return code: " + std::to_string(static_cast<uint>(ret)) + ", sequence: " +
                                                    std::to_string(metadata->sequence) + ", first scn: " + std::to_string(metadata->firstScn) + ", next scn: " +
                                                    std::to_string(metadata->nextScn));

                updateOnlineRedoLogData();
                updateOnlineLogs();
            } else if (ret == Reader::REDO_CODE::OVERWRITTEN) {
                ctx->info(0, "online redo log has been overwritten by new ctx, continuing reading from archived redo log");
                break;
            } else {
                if (parser->group == 0) {
                    throw RuntimeException(10048, "read archived redo log, code: " + std::to_string(static_cast<uint>(ret)));
                }                     throw RuntimeException(10049, "read online redo log, code: " + std::to_string(static_cast<uint>(ret)));

            }

            if (ctx->stopLogSwitches > 0) {
                --ctx->stopLogSwitches;
                if (ctx->stopLogSwitches == 0) {
                    ctx->info(0, "shutdown initiated by number of log switches");
                    ctx->stopSoft();
                }
            }
        }
        return logsProcessed;
    }
}
