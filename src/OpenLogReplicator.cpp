/* Main class for the program
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

#include <algorithm>
#include <thread>
#include <execinfo.h>
#include <signal.h>
#include <unistd.h>
#include <rapidjson/document.h>

#include "OutputBuffer.h"
#include "ConfigurationException.h"
#include "OracleAnalyser.h"
#include "OutputBufferJson.h"
#include "OutputBufferJsonDbz.h"
#include "OutputBufferJsonTest.h"
#include "RuntimeException.h"
#include "WriterKafka.h"
#include "WriterFile.h"
#include "Writer.h"

using namespace std;
using namespace rapidjson;
using namespace OpenLogReplicator;

const Value& getJSONfield(string &fileName, const Value& value, const char* field) {
    if (!value.HasMember(field)) {
        CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
    }
    return value[field];
}

const Value& getJSONfield(string &fileName, const Document& document, const char* field) {
    if (!document.HasMember(field)) {
        CONFIG_FAIL("parsing " << fileName << ", field " << field << " not found");
    }
    return document[field];
}

mutex mainMtx;
condition_variable mainThread;
bool exitOnSignal = false;
uint64_t trace2 = 0;

void stopMain(void) {
    unique_lock<mutex> lck(mainMtx);

    TRACE_(TRACE2_THREADS, "MAIN (" << hex << this_thread::get_id() << ") STOP ALL");
    mainThread.notify_all();
}

void signalHandler(int s) {
    if (!exitOnSignal) {
        cerr << "Caught signal " << s << ", exiting" << endl;
        exitOnSignal = true;
        stopMain();
    }
}

void signalCrash(int sig) {
    void *array[32];
    size_t size = backtrace(array, 32);
    cerr << "Error: signal " << dec << sig << endl;
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    exit(1);
}

int main(int argc, char **argv) {
    signal(SIGINT, signalHandler);
    signal(SIGPIPE, signalHandler);
    signal(SIGSEGV, signalCrash);
    cerr << "OpenLogReplicator v." PROGRAM_VERSION " (C) 2018-2020 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information" << endl;
    list<OracleAnalyser *> analysers;
    list<Writer *> writers;
    list<OutputBuffer *> buffers;
    OracleAnalyser *oracleAnalyser = nullptr;
    Writer *writer = nullptr;

    try {
        string fileName = "OpenLogReplicator.json";
        ifstream config(fileName, ios::in);
        if (!config.is_open()) {
            CONFIG_FAIL("file OpenLogReplicator.json is missing");
        }

        string configJSON((istreambuf_iterator<char>(config)), istreambuf_iterator<char>());
        Document document;

        if (configJSON.length() == 0 || document.Parse(configJSON.c_str()).HasParseError()) {
            CONFIG_FAIL("parsing OpenLogReplicator.json");
        }

        const Value& versionJSON = getJSONfield(fileName, document, "version");
        if (strcmp(versionJSON.GetString(), PROGRAM_VERSION) != 0) {
            CONFIG_FAIL("bad JSON, incompatible \"version\" value, expected: " << PROGRAM_VERSION << ", got: " << versionJSON.GetString());
        }

        //optional
        uint64_t dumpRedoLog = 0;
        if (document.HasMember("dump-redo-log")) {
            const Value& dumpRedoLogJSON = document["dump-redo-log"];
            dumpRedoLog = dumpRedoLogJSON.GetUint64();
        }

        //optional
        uint64_t trace = 2;
        if (document.HasMember("trace")) {
            const Value& traceJSON = document["trace"];
            trace = traceJSON.GetUint64();
        }

        //optional
        if (document.HasMember("trace2")) {
            const Value& traceJSON = document["trace2"];
            trace2 = traceJSON.GetUint64();
        }
        TRACE_(TRACE2_THREADS, "THREAD: MAIN (" << hex << this_thread::get_id() << ") START");

        //optional
        uint64_t dumpRawData = 0;
        if (document.HasMember("dump-raw-data")) {
            const Value& dumpRawDataJSON = document["dump-raw-data"];
            dumpRawData = dumpRawDataJSON.GetUint64();
        }

        //iterate through sources
        const Value& sourcesJSON = getJSONfield(fileName, document, "sources");
        if (!sourcesJSON.IsArray()) {
            CONFIG_FAIL("bad JSON, \"sources\" should be an array");
        }

        for (SizeType i = 0; i < sourcesJSON.Size(); ++i) {
            const Value& sourceJSON = sourcesJSON[i];
            const Value& aliasJSON = getJSONfield(fileName, sourceJSON, "alias");
            cerr << "Adding source: " << aliasJSON.GetString() << endl;

            //optional
            uint64_t flags = 0;
            if (sourceJSON.HasMember("flags")) {
                const Value& flagsJSON = sourceJSON["flags"];
                flags = flagsJSON.GetUint64();
            }

            //optional
            uint64_t memoryMinMb = 32;
            if (sourceJSON.HasMember("memory-min-mb")) {
                const Value& memoryMinMbJSON = sourceJSON["memory-min-mb"];
                memoryMinMb = memoryMinMbJSON.GetUint64();
                memoryMinMb = (memoryMinMb / MEMORY_CHUNK_SIZE_MB) * MEMORY_CHUNK_SIZE_MB;
                if (memoryMinMb < MEMORY_CHUNK_MIN_MB) {
                    CONFIG_FAIL("bad JSON, \"memory-min-mb\" value must be at least " MEMORY_CHUNK_MIN_MB_CHR);
                }
            }

            //optional
            uint64_t memoryMaxMb = 1024;
            if (sourceJSON.HasMember("memory-max-mb")) {
                const Value& memoryMaxMbJSON = sourceJSON["memory-max-mb"];
                memoryMaxMb = memoryMaxMbJSON.GetUint64();
                memoryMaxMb = (memoryMaxMb / MEMORY_CHUNK_SIZE_MB) * MEMORY_CHUNK_SIZE_MB;
                if (memoryMaxMb < memoryMinMb) {
                    CONFIG_FAIL("bad JSON, \"memory-min-mb\" value can't be greater than \"memory-max-mb\" value");
                }
            }

            //optional
            uint64_t redoReadSleep = 10000;
            if (sourceJSON.HasMember("redo-read-sleep")) {
                const Value& redoReadSleepJSON = sourceJSON["redo-read-sleep"];
                redoReadSleep = redoReadSleepJSON.GetUint();
            }

            //optional
            uint64_t archReadSleep = 10000000;
            if (sourceJSON.HasMember("arch-read-sleep")) {
                const Value& archReadSleepJSON = sourceJSON["arch-read-sleep"];
                archReadSleep = archReadSleepJSON.GetUint();
            }

            //optional
            uint32_t checkpointInterval = 10;
            if (sourceJSON.HasMember("checkpoint-interval")) {
                const Value& checkpointIntervalJSON = sourceJSON["checkpoint-interval"];
                checkpointInterval = checkpointIntervalJSON.GetUint64();
            }

            uint64_t arch = ARCH_LOG_PATH;
            const Value& readerJSON = getJSONfield(fileName, sourceJSON, "reader");
            const Value& readerTypeJSON = getJSONfield(fileName, readerJSON, "type");
            uint64_t readerType = READER_ONLINE;
            if (strcmp(readerTypeJSON.GetString(), "online") == 0)
                readerType = READER_ONLINE;
            else if (strcmp(readerTypeJSON.GetString(), "offline") == 0)
                readerType = READER_OFFLINE;
            else if (strcmp(readerTypeJSON.GetString(), "asm") == 0)
                readerType = READER_ASM;
            else if (strcmp(readerTypeJSON.GetString(), "standby") == 0)
                readerType = READER_STANDBY;
            else if (strcmp(readerTypeJSON.GetString(), "batch") == 0) {
                 readerType = READER_BATCH;
                 flags |= REDO_FLAGS_ARCH_ONLY;
                 arch = ARCH_LOG_LIST;
            } else {
                CONFIG_FAIL("bad JSON, invalid \"format\" value: " << readerTypeJSON.GetString());
            }

#ifndef ONLINE_MODEIMPL_OCI
            if (readerType == READER_ONLINE || readerType == READER_ASM) {
                RUNTIME_FAIL("reader types \"online\", \"asm\" are not compiled, exiting");
            }
#endif /*ONLINE_MODEIMPL_OCI*/

            //optional
            if (sourceJSON.HasMember("arch")) {
                const Value& archJSON = sourceJSON["arch"];
                if (strcmp(archJSON.GetString(), "path") == 0)
                    arch = ARCH_LOG_PATH;
                else {
                    if (readerType == READER_BATCH) {
                        CONFIG_FAIL("bad JSON, invalid \"arch\" value: " << archJSON.GetString() << ", only \"disk\" can be used here");
                    }

                    if (strcmp(archJSON.GetString(), "online") == 0)
                        arch = ARCH_LOG_ONLINE;
                    else if (strcmp(archJSON.GetString(), "online-keep") == 0)
                        arch = ARCH_LOG_ONLINE_KEEP;
                    if (strcmp(archJSON.GetString(), "list") == 0) {
                        if (readerType != READER_OFFLINE) {
                            CONFIG_FAIL("bad JSON, invalid \"arch\" value: \"list\" mode is only valid for \"offline\" reader");
                        }
                    } else{
                        CONFIG_FAIL("bad JSON, invalid \"arch\" value: " << archJSON.GetString());
                    }
                }
            }

            //optional
            uint64_t disableChecks = 0;
            if (readerJSON.HasMember("disable-checks")) {
                const Value& disableChecksJSON = readerJSON["disable-checks"];
                disableChecks = disableChecksJSON.GetUint64();
            }

            const Value& nameJSON = getJSONfield(fileName, sourceJSON, "name");

            const char *user = "", *password = "", *server = "", *userASM = "", *passwordASM = "", *serverASM = "";
            if (readerType == READER_ONLINE || readerType == READER_ASM || readerType == READER_STANDBY) {
                const Value& userJSON = getJSONfield(fileName, readerJSON, "user");
                user = userJSON.GetString();
                const Value& passwordJSON = getJSONfield(fileName, readerJSON, "password");
                password = passwordJSON.GetString();
                const Value& serverJSON = getJSONfield(fileName, readerJSON, "server");
                server = serverJSON.GetString();
            }
            if (readerType == READER_ASM) {
                const Value& userASMJSON = getJSONfield(fileName, readerJSON, "user-asm");
                userASM = userASMJSON.GetString();
                const Value& passwordASMJSON = getJSONfield(fileName, readerJSON, "password-asm");
                passwordASM = passwordASMJSON.GetString();
                const Value& serverASMJSON = getJSONfield(fileName, readerJSON, "server-asm");
                serverASM = serverASMJSON.GetString();
            }

            //format
            const Value& formatJSON = getJSONfield(fileName, sourceJSON, "format");

            //optional
            uint64_t timestampFormat = 0;
            if (formatJSON.HasMember("timestamp-format")) {
                const Value& timestampFormatJSON = formatJSON["timestamp-format"];
                timestampFormat = timestampFormatJSON.GetUint64();
            }

            //optional
            uint64_t charFormat = 0;
            if (formatJSON.HasMember("char-format")) {
                const Value& charFormatJSON = formatJSON["char-format"];
                charFormat = charFormatJSON.GetUint64();
            }

            //optional
            uint64_t scnFormat = 0;
            if (formatJSON.HasMember("scn-format")) {
                const Value& scnFormatJSON = formatJSON["scn-format"];
                scnFormat = scnFormatJSON.GetUint64();
            }

            //optional
            uint64_t unknownFormat = 0;
            if (formatJSON.HasMember("unknown-format")) {
                const Value& unknownFormatJSON = formatJSON["unknown-format"];
                unknownFormat = unknownFormatJSON.GetUint64();
            }

            //optional
            uint64_t showColumns = 0;
            if (formatJSON.HasMember("show-columns")) {
                const Value& showColumnsJSON = formatJSON["show-columns"];
                showColumns = showColumnsJSON.GetUint64();
            }

            const Value& formatTypeJSON = getJSONfield(fileName, formatJSON, "type");

            OutputBuffer *outputBuffer = nullptr;
            if (strcmp("json", formatTypeJSON.GetString()) == 0) {
                outputBuffer = new OutputBufferJson(timestampFormat, charFormat, scnFormat, unknownFormat, showColumns);
            } else if (strcmp("json-dbz", formatTypeJSON.GetString()) == 0) {
                outputBuffer = new OutputBufferJsonDbz(timestampFormat, charFormat, scnFormat, unknownFormat, showColumns);
            } else if (strcmp("json-test", formatTypeJSON.GetString()) == 0) {
                outputBuffer = new OutputBufferJsonTest(timestampFormat, charFormat, scnFormat, unknownFormat, showColumns);
            } else {
                CONFIG_FAIL("bad JSON, invalid \"type\" value: " << formatTypeJSON.GetString());
            }

            if (outputBuffer == nullptr) {
                RUNTIME_FAIL("could not allocate " << dec << sizeof(OutputBuffer) << " bytes memory for (reason: command buffer)");
            }
            buffers.push_back(outputBuffer);

            oracleAnalyser = new OracleAnalyser(outputBuffer, aliasJSON.GetString(), nameJSON.GetString(), user, password, server, userASM,
                    passwordASM, serverASM, arch, trace, trace2, dumpRedoLog, dumpRawData, flags, readerType, disableChecks, redoReadSleep,
                    archReadSleep, checkpointInterval, memoryMinMb, memoryMaxMb);
            if (oracleAnalyser == nullptr) {
                RUNTIME_FAIL("could not allocate " << dec << sizeof(OracleAnalyser) << " bytes memory for (reason: oracle analyser)");
            }

            //optional
            if (readerType == READER_ONLINE || readerType == READER_OFFLINE || readerType == READER_STANDBY) {
                if (readerJSON.HasMember("path-mapping")) {
                    const Value& pathMapping = readerJSON["path-mapping"];
                    if (!pathMapping.IsArray()) {
                        CONFIG_FAIL("bad JSON, \"path-mapping\" should be array");
                    }
                    if ((pathMapping.Size() % 2) != 0) {
                        CONFIG_FAIL("bad JSON, \"path-mapping\" should contain even number of elements");
                    }

                    for (SizeType j = 0; j < pathMapping.Size() / 2; ++j) {
                        const Value& sourceMapping = pathMapping[j * 2];
                        const Value& targetMapping = pathMapping[j * 2 + 1];
                        oracleAnalyser->addPathMapping(sourceMapping.GetString(), targetMapping.GetString());
                    }
                }
            }

            if (readerType == READER_BATCH) {
                if (!readerJSON.HasMember("redo-logs")) {
                    CONFIG_FAIL("bad JSON, missing \"redo-logs\" element which is required in \"batch\" reader type");
                }

                const Value& redoLogsBatch = readerJSON["redo-logs"];
                if (!redoLogsBatch.IsArray()) {
                    CONFIG_FAIL("bad JSON, \"redo-logs\" field should be array");
                }

                for (SizeType j = 0; j < redoLogsBatch.Size(); ++j) {
                    const Value& path = redoLogsBatch[j];
                    oracleAnalyser->addRedoLogsBatch(path.GetString());
                }
            }

            outputBuffer->initialize(oracleAnalyser);

            if (readerType == READER_OFFLINE || readerType == READER_BATCH) {
                if (!oracleAnalyser->readSchema()) {
                    CONFIG_FAIL("bad JSON, can't read schema from <database>-schema.json");
                }
            } else {
                oracleAnalyser->initializeOnlineMode();

                string keysStr("");
                vector<string> keys;
                if (sourceJSON.HasMember("event-table")) {
                    const Value& eventtableJSON = sourceJSON["event-table"];
                    oracleAnalyser->addTable(eventtableJSON.GetString(), keys, keysStr, 1);
                }

                const Value& tablesJSON = getJSONfield(fileName, sourceJSON, "tables");
                if (!tablesJSON.IsArray()) {
                    CONFIG_FAIL("bad JSON, field \"tables\" should be array");
                }

                for (SizeType j = 0; j < tablesJSON.Size(); ++j) {
                    const Value& tableJSON = getJSONfield(fileName, tablesJSON[j], "table");

                    if (tablesJSON[j].HasMember("key")) {
                        const Value& key = tablesJSON[j]["key"];
                        keysStr = key.GetString();
                        stringstream keyStream(keysStr);

                        while (keyStream.good()) {
                            string keyCol, keyCol2;
                            getline(keyStream, keyCol, ',' );
                            keyCol.erase(remove(keyCol.begin(), keyCol.end(), ' '), keyCol.end());
                            transform(keyCol.begin(), keyCol.end(),keyCol.begin(), ::toupper);
                            keys.push_back(keyCol);
                        }
                    } else
                        keysStr = "";
                    oracleAnalyser->addTable(tableJSON.GetString(), keys, keysStr, 0);
                    keys.clear();
                }

                oracleAnalyser->writeSchema();
            }

            if (pthread_create(&oracleAnalyser->pthread, nullptr, &Thread::runStatic, (void*)oracleAnalyser)) {
                RUNTIME_FAIL("error spawning thread - oracle analyser");
            }

            analysers.push_back(oracleAnalyser);
            oracleAnalyser = nullptr;
        }

        //iterate through targets
        const Value& targetsJSON = getJSONfield(fileName, document, "targets");
        if (!targetsJSON.IsArray()) {
            CONFIG_FAIL("bad JSON, field \"targets\" should be array");
        }
        for (SizeType i = 0; i < targetsJSON.Size(); ++i) {
            const Value& targetJSON = targetsJSON[i];
            const Value& aliasJSON = getJSONfield(fileName, targetJSON, "alias");
            cerr << "Adding target: " << aliasJSON.GetString() << endl;

            const Value& sourceJSON = getJSONfield(fileName, targetJSON, "source");
            OracleAnalyser *oracleAnalyser = nullptr;
            for (OracleAnalyser *analyser : analysers)
                if (analyser->alias.compare(sourceJSON.GetString()) == 0)
                    oracleAnalyser = (OracleAnalyser*)analyser;
            if (oracleAnalyser == nullptr) {
                CONFIG_FAIL("bad JSON, could not find reader for \"source\" value: " << sourceJSON.GetString());
            }

            //writer
            const Value& writerJSON = getJSONfield(fileName, targetJSON, "writer");
            const Value& writerTypeJSON = getJSONfield(fileName, writerJSON, "type");

            //optional
            uint64_t shortMessage = 0;
            if (writerJSON.HasMember("short-message")) {
                const Value& shortMessageJSON = writerJSON["short-message"];
                shortMessage = shortMessageJSON.GetUint64();
            }

            if (strcmp(writerTypeJSON.GetString(), "file") == 0) {
                const char *name = "";
                if (writerJSON.HasMember("name")) {
                    const Value& nameJSON = writerJSON["name"];
                    name = nameJSON.GetString();
                }

                writer = new WriterFile(aliasJSON.GetString(), oracleAnalyser, name, shortMessage);
                if (writer == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(WriterFile) << " bytes memory for (reason: file writer)");
                }
            } else if (strcmp(writerTypeJSON.GetString(), "kafka") == 0) {
                uint64_t maxMessageMb = 100;
                if (writerJSON.HasMember("max-message-mb")) {
                    const Value& maxMessageMbJSON = writerJSON["max-message-mb"];
                    maxMessageMb = maxMessageMbJSON.GetUint64();
                    if (maxMessageMb < 1)
                        maxMessageMb = 1;
                    if (maxMessageMb > MAX_KAFKA_MESSAGE_MB)
                        maxMessageMb = MAX_KAFKA_MESSAGE_MB;
                }

                uint64_t maxMessages = 100000;
                if (writerJSON.HasMember("max-messages")) {
                    const Value& maxMessagesJSON = writerJSON["max-messages"];
                    maxMessages = maxMessagesJSON.GetUint64();
                    if (maxMessages < 1)
                        maxMessages = 1;
                    if (maxMessages > MAX_KAFKA_MAX_MESSAGES)
                        maxMessages = MAX_KAFKA_MAX_MESSAGES;
                }

                const Value& brokersJSON = getJSONfield(fileName, writerJSON, "brokers");
                const Value& topicJSON = getJSONfield(fileName, writerJSON, "topic");

                writer = new WriterKafka(aliasJSON.GetString(), oracleAnalyser, shortMessage, brokersJSON.GetString(),
                        topicJSON.GetString(), maxMessageMb, maxMessages);
                if (writer == nullptr) {
                    RUNTIME_FAIL("could not allocate " << dec << sizeof(WriterKafka) << " bytes memory for (reason: kafka writer)");
                }
            } else if (strcmp(writerTypeJSON.GetString(), "service") == 0) {
                CONFIG_FAIL("bad JSON: service writer module not yet implemented");
            } else {
                CONFIG_FAIL("bad JSON: invalid \"type\" value: " << writerTypeJSON.GetString());
            }

            oracleAnalyser->outputBuffer->setWriter(writer);
            if (pthread_create(&writer->pthread, nullptr, &Thread::runStatic, (void*)writer)) {
                RUNTIME_FAIL("error spawning thread - kafka writer");
            }

            writers.push_back(writer);
            writer = nullptr;
        }

        //sleep until killed
        {
            unique_lock<mutex> lck(mainMtx);
            mainThread.wait(lck);
        }

    } catch(ConfigurationException &ex) {
    } catch(RuntimeException &ex) {
    }

    if (oracleAnalyser != nullptr)
        analysers.push_back(oracleAnalyser);

    if (writer != nullptr)
        writers.push_back(writer);

    //shut down all analysers
    for (OracleAnalyser *analyser : analysers)
        analyser->stop();
    for (OracleAnalyser *analyser : analysers) {
        if (analyser->started)
            pthread_join(analyser->pthread, nullptr);
    }

    //shut down writers
    for (Writer *writer : writers)
        writer->stop();
    for (OutputBuffer *outputBuffer : buffers) {
        unique_lock<mutex> lck(outputBuffer->mtx);
        outputBuffer->writersCond.notify_all();
    }
    for (Writer *writer : writers) {
        if (writer->started)
            pthread_join(writer->pthread, nullptr);
        delete writer;
    }
    writers.clear();

    for (OutputBuffer *outputBuffer : buffers)
        delete outputBuffer;
    buffers.clear();

    for (OracleAnalyser *analyser : analysers)
        delete analyser;
    analysers.clear();

    TRACE_(TRACE2_THREADS, "MAIN (" << hex << this_thread::get_id() << ") STOP");
    return 0;
}
