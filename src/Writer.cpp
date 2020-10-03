/* Base class for thread to write output
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <thread>

#include "ConfigurationException.h"
#include "OutputBuffer.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Writer.h"

using namespace std;

void stopMain();

namespace OpenLogReplicator {

    Writer::Writer(const char *alias, OracleAnalyser *oracleAnalyser, uint64_t maxMessageMb) :
        Thread(alias),
        outputBuffer(oracleAnalyser->outputBuffer),
        oracleAnalyser(oracleAnalyser),
        maxMessageMb(maxMessageMb) {

        msgBuffer = oracleAnalyser->getMemoryChunk("WRITER", false);
    }

    Writer::~Writer() {
        if (msgBuffer != nullptr) {
            oracleAnalyser->freeMemoryChunk("WRITER", msgBuffer, false);
            msgBuffer = nullptr;
        }
    }

    void *Writer::run(void) {
        TRACE(TRACE2_THREADS, "WRITER (" << hex << this_thread::get_id() << ") START");

        INFO("Writer is starting: " << getName());

        try {
            for (;;) {
                uint64_t length = 0, bufferEnd;

                //get new block to read
                {
                    unique_lock<mutex> lck(outputBuffer->mtx);
                    bufferEnd = *((uint64_t*)(outputBuffer->firstBuffer + OUTPUT_BUFFER_END));
                    length = *((uint64_t*)(outputBuffer->firstBuffer + outputBuffer->firstBufferPos));

                    while ((outputBuffer->firstBufferPos == bufferEnd || length == 0) && !shutdown) {
                        oracleAnalyser->waitingForWriter = false;
                        oracleAnalyser->memoryCond.notify_all();
                        outputBuffer->writersCond.wait(lck);
                        bufferEnd = *((uint64_t*)(outputBuffer->firstBuffer + OUTPUT_BUFFER_END));
                        length = *((uint64_t*)(outputBuffer->firstBuffer + outputBuffer->firstBufferPos));

                        if (!shutdown)
                            oracleAnalyser->waitingForWriter = true;
                    }
                }

                //all data sent & shutdown command
                if (outputBuffer->firstBufferPos == bufferEnd && shutdown)
                    break;

                while (outputBuffer->firstBufferPos < bufferEnd) {
                    length = *((uint64_t*)(outputBuffer->firstBuffer + outputBuffer->firstBufferPos));

                    if (length == 0)
                        break;

                    outputBuffer->firstBufferPos += OUTPUT_BUFFER_LENGTH_SIZE;
                    uint64_t leftLength = (length + 7) & 0xFFFFFFFFFFFFFFF8;

                    //message in one part - send directly from buffer
                    if (outputBuffer->firstBufferPos + leftLength < MEMORY_CHUNK_SIZE) {
                        sendMessage(outputBuffer->firstBuffer + outputBuffer->firstBufferPos, length, false);
                        outputBuffer->firstBufferPos += leftLength;

                    //message in many parts - copy
                    } else {
                        uint8_t *buffer;
                        bool dealloc = false;
                       if (leftLength <= MEMORY_CHUNK_SIZE) {
                            buffer = msgBuffer;
                        } else {
                            buffer = (uint8_t*)malloc(leftLength);
                            if (buffer == nullptr) {
                                RUNTIME_FAIL("couldn't allocate " << leftLength << " bytes memory (for: temporary buffer for JSON message)");
                            }
                            dealloc = true;
                        }

                        uint64_t targetPos = 0;

                        while (leftLength > 0) {
                            if (outputBuffer->firstBufferPos + leftLength >= MEMORY_CHUNK_SIZE) {
                                uint64_t tmpLength = (MEMORY_CHUNK_SIZE - outputBuffer->firstBufferPos);
                                memcpy(buffer + targetPos, outputBuffer->firstBuffer + outputBuffer->firstBufferPos, tmpLength);
                                leftLength -= tmpLength;
                                targetPos += tmpLength;

                                //switch to next
                                uint8_t* nextBuffer = *((uint8_t**)(outputBuffer->firstBuffer + OUTPUT_BUFFER_NEXT));
                                oracleAnalyser->freeMemoryChunk("KAFKA", outputBuffer->firstBuffer, true);
                                outputBuffer->firstBufferPos = OUTPUT_BUFFER_DATA;

                                {
                                    unique_lock<mutex> lck(outputBuffer->mtx);
                                    --outputBuffer->buffersAllocated;
                                    outputBuffer->firstBuffer = nextBuffer;
                                    oracleAnalyser->memoryCond.notify_all();
                                }
                            } else {
                                memcpy(buffer + targetPos, outputBuffer->firstBuffer + outputBuffer->firstBufferPos, leftLength);
                                outputBuffer->firstBufferPos += leftLength;
                                leftLength = 0;
                            }
                        }

                        sendMessage(buffer, length, dealloc);
                        break;
                    }
                }
            }

        } catch(ConfigurationException &ex) {
            stopMain();
        } catch(RuntimeException &ex) {
            stopMain();
        }

        INFO("Writer is stopping: " << getName());

        TRACE(TRACE2_THREADS, "WRITER (" << hex << this_thread::get_id() << ") STOP");
        return 0;
    }
}
