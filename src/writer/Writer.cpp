/* Base class for thread to write output
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
<http:////www.gnu.org/licenses/>.  */

#include <fstream>
#include <thread>
#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/ConfigurationException.h"
#include "../common/Ctx.h"
#include "../common/NetworkException.h"
#include "../common/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "Writer.h"

namespace OpenLogReplicator {
    Writer::Writer(Ctx* newCtx, std::string newAlias, std::string& newDatabase, Builder* newBuilder, Metadata* newMetadata) :
            Thread(newCtx, newAlias),
            database(newDatabase),
            builder(newBuilder),
            metadata(newMetadata),
            checkpointScn(ZERO_SCN),
            checkpointTime(time(nullptr)),
            confirmedScn(ZERO_SCN),
            confirmedMessages(0),
            sentMessages(0),
            tmpQueueSize(0),
            maxQueueSize(0),
            queue(nullptr),
            streaming(false) {
    }

    Writer::~Writer() {
        if (queue != nullptr) {
            delete[] queue;
            queue = nullptr;
        }
    }

    void Writer::initialize() {
        if (queue != nullptr)
            return;
        queue = new BuilderMsg*[ctx->queueSize];
    }

    void Writer::createMessage(BuilderMsg* msg) {
        ++sentMessages;

        queue[tmpQueueSize++] = msg;
        if (tmpQueueSize > maxQueueSize)
            maxQueueSize = tmpQueueSize;
    }

    void Writer::sortQueue() {
        if (tmpQueueSize == 0)
            return;

        BuilderMsg** oldQueue = queue;
        queue = new BuilderMsg*[ctx->queueSize];
        uint64_t oldQueueSize = tmpQueueSize;

        for (uint64_t newId = 0 ; newId < tmpQueueSize; ++newId) {
            queue[newId] = oldQueue[0];
            uint64_t i = 0;
            --oldQueueSize;
            while (i < oldQueueSize) {
                if (i * 2 + 2 < oldQueueSize && oldQueue[i * 2 + 2]->id < oldQueue[oldQueueSize]->id) {
                    if (oldQueue[i * 2 + 1]->id < oldQueue[i * 2 + 2]->id) {
                        oldQueue[i] = oldQueue[i * 2 + 1];
                        i = i * 2 + 1;
                    } else {
                        oldQueue[i] = oldQueue[i * 2 + 2];
                        i = i * 2 + 2;
                    }
                } else if (i * 2 + 1 < oldQueueSize && oldQueue[i * 2 + 1]->id < oldQueue[oldQueueSize]->id) {
                    oldQueue[i] = oldQueue[i * 2 + 1];
                    i = i * 2 + 1;
                } else
                    break;
            }
            oldQueue[i] = oldQueue[oldQueueSize];
        }

        if (oldQueue != nullptr)
            delete[] oldQueue;
    }

    void Writer::confirmMessage(BuilderMsg* msg) {
        if (msg == nullptr) {
            if (tmpQueueSize == 0) {
                WARNING("trying to confirm empty message")
                return;
            }
            msg = queue[0];
        }

        msg->flags |= OUTPUT_BUFFER_CONFIRMED;
        if (msg->flags & OUTPUT_BUFFER_ALLOCATED) {
            delete[] msg->data;
            msg->flags &= ~OUTPUT_BUFFER_ALLOCATED;
        }
        ++confirmedMessages;

        uint64_t maxId = 0;
        {
            while (tmpQueueSize > 0 && (queue[0]->flags & OUTPUT_BUFFER_CONFIRMED) != 0) {
                maxId = queue[0]->queueId;
                confirmedScn = queue[0]->scn;

                if (--tmpQueueSize == 0)
                    break;

                uint64_t i = 0;
                while (i < tmpQueueSize) {
                    if (i * 2 + 2 < tmpQueueSize && queue[i * 2 + 2]->id < queue[tmpQueueSize]->id) {
                        if (queue[i * 2 + 1]->id < queue[i * 2 + 2]->id) {
                            queue[i] = queue[i * 2 + 1];
                            i = i * 2 + 1;
                        } else {
                            queue[i] = queue[i * 2 + 2];
                            i = i * 2 + 2;
                        }
                    } else if (i * 2 + 1 < tmpQueueSize && queue[i * 2 + 1]->id < queue[tmpQueueSize]->id) {
                        queue[i] = queue[i * 2 + 1];
                        i = i * 2 + 1;
                    } else
                        break;
                }
                queue[i] = queue[tmpQueueSize];
            }
        }

        builder->releaseBuffers(maxId);
    }

    void Writer::run() {
        TRACE(TRACE2_THREADS, "THREADS: WRITER (" << std::hex << std::this_thread::get_id() << ") START")

        INFO("writer is starting with " << getName())

        try {
            // External loop for client disconnection
            while (!ctx->hardShutdown) {
                try {
                    mainLoop();
                } catch (NetworkException& ex) {
                    WARNING(ex.msg)
                    streaming = false;
                    // Client got disconnected
                }

                if (ctx->softShutdown && ctx->replicatorFinished)
                    break;
            }
        } catch (ConfigurationException& ex) {
            ERROR(ex.msg)
            ctx->stopHard();
        } catch (RuntimeException& ex) {
            ERROR(ex.msg)
            ctx->stopHard();
        }

        INFO("writer is stopping: " << getName() << ", max queue size: " << std::dec << maxQueueSize)

        TRACE(TRACE2_THREADS, "THREADS: WRITER (" << std::hex << std::this_thread::get_id() << ") STOP")
    }

    void Writer::mainLoop() {
        // Client isConnected
        readCheckpoint();

        BuilderMsg* msg;
        BuilderQueue* curBuffer = builder->firstBuffer;
        uint64_t curLength = 0;
        uint64_t tmpLength = 0;
        tmpQueueSize = 0;

        // Start streaming
        while (!ctx->hardShutdown) {

            // Get message to send
            while (!ctx->hardShutdown) {
                // Check for client checkpoint
                pollQueue();
                writeCheckpoint(false);

                // Next buffer
                if (curBuffer->length == curLength && curBuffer->next != nullptr) {
                    curBuffer = curBuffer->next;
                    curLength = 0;
                }

                // Found something
                msg = (BuilderMsg *) (curBuffer->data + curLength);

                if (curBuffer->length > curLength + sizeof(struct BuilderMsg) && msg->length > 0) {
                    tmpLength = curBuffer->length;
                    break;
                }

                ctx->wakeAllOutOfMemory();
                if (ctx->softShutdown && ctx->replicatorFinished)
                    break;
                builder->sleepForWriterWork(tmpQueueSize, ctx->pollIntervalUs);
            }

            if (ctx->hardShutdown)
                break;

            // Send message
            while (curLength + sizeof(struct BuilderMsg) < tmpLength && !ctx->hardShutdown) {
                msg = (BuilderMsg*) (curBuffer->data + curLength);
                if (msg->length == 0)
                    break;

                // Queue is full
                pollQueue();
                while (tmpQueueSize >= ctx->queueSize && !ctx->hardShutdown) {
                    DEBUG("output queue is full (" << std::dec << tmpQueueSize << " schemaElements), sleeping " << std::dec << ctx->pollIntervalUs << "us")
                    usleep(ctx->pollIntervalUs);
                    pollQueue();
                }

                writeCheckpoint(false);
                if (ctx->hardShutdown)
                    break;

                // builder->firstBufferPos += OUTPUT_BUFFER_RECORD_HEADER_SIZE;
                uint64_t length8 = (msg->length + 7) & 0xFFFFFFFFFFFFFFF8;
                curLength += sizeof(struct BuilderMsg);

                // Message in one part - send directly from buffer
                if (curLength + length8 <= OUTPUT_BUFFER_DATA_SIZE) {
                    createMessage(msg);
                    sendMessage(msg);
                    curLength += length8;
                    msg = (BuilderMsg*) (curBuffer->data + curLength);

                    // Message in many parts - merge & copy
                } else {
                    msg->data = (uint8_t*)malloc(msg->length);
                    if (msg->data == nullptr)
                        throw RuntimeException("couldn't allocate " + std::to_string(msg->length) +
                                               " bytes memory (for: temporary buffer for JSON message)");
                    msg->flags |= OUTPUT_BUFFER_ALLOCATED;

                    uint64_t copied = 0;
                    while (msg->length - copied > 0) {
                        uint64_t toCopy = msg->length - copied;
                        if (toCopy > tmpLength - curLength) {
                            toCopy = tmpLength - curLength;
                            memcpy((void*)(msg->data + copied), (void*)(curBuffer->data + curLength), toCopy);
                            curBuffer = curBuffer->next;
                            tmpLength = OUTPUT_BUFFER_DATA_SIZE;
                            curLength = 0;
                        } else {
                            memcpy((void*)(msg->data + copied), (void*)(curBuffer->data + curLength), toCopy);
                            curLength += (toCopy + 7) & 0xFFFFFFFFFFFFFFF8;
                        }
                        copied += toCopy;
                    }

                    createMessage(msg);
                    sendMessage(msg);
                    pollQueue();
                    writeCheckpoint(false);
                    break;
                }
            }

            // All work done?
            if (ctx->softShutdown && ctx->replicatorFinished) {
                // Some data to send?
                if (curBuffer->length != curLength || curBuffer->next != nullptr)
                    continue;
                break;
            }
        }

        writeCheckpoint(true);
    }

    void Writer::writeCheckpoint(bool force) {
        // Nothing changed
        if (checkpointScn == confirmedScn || confirmedScn == ZERO_SCN)
            return;

        // Not yet
        time_t now = time(nullptr);
        uint64_t timeSinceCheckpoint = (now - checkpointTime);
        if (timeSinceCheckpoint < ctx->checkpointIntervalS && !force)
            return;

        TRACE(TRACE2_CHECKPOINT, "CHECKPOINT: writer checkpoint scn: " << std::dec << checkpointScn << " confirmed scn: " << confirmedScn)
        std::string name(database + "-chkpt");
        std::stringstream ss;
        ss << R"({"database":")" << database
                << R"(","scn":)" << std::dec << confirmedScn
                << R"(,"resetlogs":)" << std::dec << metadata->resetlogs
                << R"(,"activation":)" << std::dec << metadata->activation << "}";

        if (metadata->stateWrite(name, ss)) {
            checkpointScn = confirmedScn;
            checkpointTime = now;
        }
    }

    void Writer::readCheckpoint() {
        std::ifstream infile;
        std::string name(database + "-chkpt");

        // Checkpoint is present - read it
        std::string checkpoint;
        rapidjson::Document document;
        if (!metadata->stateRead(name, CHECKPOINT_FILE_MAX_SIZE, checkpoint)) {
            metadata->setStatusReplicate();
            return;
        }

        if (checkpoint.length() == 0 || document.Parse(checkpoint.c_str()).HasParseError())
            throw RuntimeException("parsing of: " + name + " at offset: " + std::to_string(document.GetErrorOffset()) +
                                   ", message: " + GetParseError_En(document.GetParseError()));

        const char* databaseJson = Ctx::getJsonFieldS(name, JSON_PARAMETER_LENGTH, document, "database");
        if (database != databaseJson)
            throw RuntimeException("parsing of: " + name + " - invalid database name: " + databaseJson);

        metadata->setResetlogs(Ctx::getJsonFieldU32(name, document, "resetlogs"));
        metadata->setActivation(Ctx::getJsonFieldU32(name, document, "activation"));

        // Started earlier - continue work & ignore default startup parameters
        metadata->startScn = Ctx::getJsonFieldU64(name, document, "scn");
        metadata->startSequence = ZERO_SEQ;
        metadata->startTime.clear();
        metadata->startTimeRel = 0;
        INFO("checkpoint - reading scn: " << std::dec << metadata->startScn)

        metadata->setStatusReplicate();
    }

    void Writer::wakeUp() {
        builder->wakeUp();
    }
}
