/* Main class for the program
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <list>
#include <mutex>
#include <sstream>
#include <streambuf>
#include <string>
#include <vector>
#include <execinfo.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <rapidjson/document.h>

#include "CommandBuffer.h"
#include "ConfigurationException.h"
#include "KafkaWriter.h"
#include "MemoryException.h"
#include "OracleAnalyser.h"
#include "RuntimeException.h"

using namespace std;
using namespace rapidjson;
using namespace OpenLogReplicator;

const Value& getJSONfield(string &fileName, const Value& value, const char* field) {
    if (!value.HasMember(field)) {
        cerr << "ERROR: parsing " << fileName << ", field " << field << " not found" << endl;
        throw ConfigurationException("JSON format error");
    }
    return value[field];
}

const Value& getJSONfield(string &fileName, const Document& document, const char* field) {
    if (!document.HasMember(field)) {
        cerr << "ERROR: parsing " << fileName << ", field " << field << " not found" << endl;
        throw ConfigurationException("JSON format error");
    }
    return document[field];
}

mutex mainMtx;
condition_variable mainThread;
bool exitOnSignal = false;

void stopMain(void) {
    unique_lock<mutex> lck(mainMtx);
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
    cout << "Open Log Replicator v." PROGRAM_VERSION " (C) 2018-2020 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information" << endl;
    list<Thread *> analysers, writers;
    list<CommandBuffer *> buffers;
    OracleAnalyser *oracleAnalyser = nullptr;
    KafkaWriter *kafkaWriter = nullptr;

    try {
        string fileName = "OpenLogReplicator.json";
        ifstream config(fileName, ios::in);
        if (!config.is_open())
            throw ConfigurationException("file OpenLogReplicator.json is missing");

        string configJSON((istreambuf_iterator<char>(config)), istreambuf_iterator<char>());
        Document document;

        if (configJSON.length() == 0 || document.Parse(configJSON.c_str()).HasParseError())
            throw ConfigurationException("parsing OpenLogReplicator.json");

        const Value& version = getJSONfield(fileName, document, "version");
        if (strcmp(version.GetString(), PROGRAM_VERSION) != 0)
            throw ConfigurationException("bad JSON, incompatible version");

        //optional
        uint64_t dumpRedoLog = 0;
        if (document.HasMember("dump-redo-log")) {
            const Value& dumpRedoLogJSON = document["dump-redo-log"];
            dumpRedoLog = dumpRedoLogJSON.GetUint64();
        }

        //optional
        uint64_t trace = 0;
        if (document.HasMember("trace")) {
            const Value& traceJSON = document["trace"];
            trace = traceJSON.GetUint64();
        }

        //optional
        uint64_t trace2 = 0;
        if (document.HasMember("trace2")) {
            const Value& traceJSON = document["trace2"];
            trace2 = traceJSON.GetUint64();
        }

        //optional
        uint64_t dumpRawData = 0;
        if (document.HasMember("dump-raw-data")) {
            const Value& dumpRawDataJSON = document["dump-raw-data"];
            dumpRawData = dumpRawDataJSON.GetUint64();
        }

        //optional
        uint64_t redoReadSleep = 10000;
        if (document.HasMember("redo-read-sleep")) {
            const Value& redoReadSleepJSON = document["redo-read-sleep"];
            redoReadSleep = redoReadSleepJSON.GetUint();
        }

        //optional
        uint32_t checkpointInterval = 10;
        if (document.HasMember("checkpoint-interval")) {
            const Value& checkpointIntervalJSON = document["checkpoint-interval"];
            checkpointInterval = checkpointIntervalJSON.GetUint64();
        }

        //optional
        uint64_t redoBufferSize = 65536;
        if (document.HasMember("redo-buffer-size")) {
            const Value& redoBufferSizeJSON = document["redo-buffer-size"];
            redoBufferSize = redoBufferSizeJSON.GetUint64();
        }
        if (redoBufferSize == 0 || redoBufferSize > 1048576)
            redoBufferSize = 1048576;

        const Value& redoBuffersJSON = getJSONfield(fileName, document, "redo-buffer-mb");
        uint64_t redoBuffers = redoBuffersJSON.GetUint64() * (1048576 / redoBufferSize);

        const Value& outputBufferSizeJSON = getJSONfield(fileName, document, "output-buffer-mb");
        uint64_t outputBufferSize = outputBufferSizeJSON.GetUint64() * 1048576;

        //optional
        uint64_t maxConcurrentTransactions = 65536;
        if (document.HasMember("max-concurrent-transactions")) {
            const Value& maxConcurrentTransactionsJSON = document["max-concurrent-transactions"];
            maxConcurrentTransactions = maxConcurrentTransactionsJSON.GetUint64();
        }

        //iterate through sources
        const Value& sources = getJSONfield(fileName, document, "sources");
        if (!sources.IsArray())
            throw ConfigurationException("bad JSON, sources should be an array");

        for (SizeType i = 0; i < sources.Size(); ++i) {
            const Value& source = sources[i];
            const Value& type = getJSONfield(fileName, source, "type");

            if (strcmp("ORACLE", type.GetString()) == 0) {
                const Value& alias = getJSONfield(fileName, source, "alias");

                uint64_t flags = 0;
                if (source.HasMember("flags")) {
                    const Value& flagsJSON = source["flags"];
                    flags = flagsJSON.GetUint64();
                }

                uint64_t mode = MODE_ONLINE;
                if (source.HasMember("mode")) {
                    const Value& modeJSON = source["mode"];
                    if (strncmp(modeJSON.GetString(),"offline", 7) == 0)
                        mode = MODE_OFFLINE;
                }

                //optional
                uint64_t disableChecks = 0;
                if (source.HasMember("disable-checks")) {
                    const Value& disableChecksJSON = source["disable-checks"];
                    disableChecks = disableChecksJSON.GetUint64();
                }

                const Value& name = getJSONfield(fileName, source, "name");
                cout << "Adding source: " << name.GetString() << endl;

                CommandBuffer *commandBuffer = new CommandBuffer(outputBufferSize);
                buffers.push_back(commandBuffer);
                if (commandBuffer == nullptr)
                    throw MemoryException("main.1", sizeof(CommandBuffer));

                if (mode == MODE_ONLINE) {
                    const Value& user = getJSONfield(fileName, source, "user");
                    const Value& password = getJSONfield(fileName, source, "password");
                    const Value& server = getJSONfield(fileName, source, "server");

                    oracleAnalyser = new OracleAnalyser(commandBuffer, alias.GetString(), name.GetString(), user.GetString(),
                            password.GetString(), server.GetString(), trace, trace2, dumpRedoLog, dumpRawData, flags, mode, disableChecks,
                            redoReadSleep, checkpointInterval, redoBuffers, redoBufferSize, maxConcurrentTransactions);
                    if (oracleAnalyser == nullptr)
                        throw MemoryException("main.2", sizeof(OracleAnalyser));
                } else {
                    oracleAnalyser = new OracleAnalyser(commandBuffer, alias.GetString(), name.GetString(), "",
                            "", "", trace, trace2, dumpRedoLog, dumpRawData, flags, mode, disableChecks,
                            redoReadSleep, checkpointInterval, redoBuffers, redoBufferSize, maxConcurrentTransactions);
                    if (oracleAnalyser == nullptr)
                        throw MemoryException("main.3", sizeof(OracleAnalyser));
                }

                //optional
                if (source.HasMember("path-mapping")) {
                    const Value& pathMapping = source["path-mapping"];
                    if (!pathMapping.IsArray())
                        throw ConfigurationException("bad JSON, path-mapping should be array");
                    if ((pathMapping.Size() % 2) != 0)
                        throw ConfigurationException("path-mapping should contain pairs of elements");

                    for (SizeType j = 0; j < pathMapping.Size() / 2; ++j) {
                        const Value& sourceMapping = pathMapping[j * 2];
                        const Value& targetMapping = pathMapping[j * 2 + 1];
                        oracleAnalyser->addPathMapping(sourceMapping.GetString(), targetMapping.GetString());
                    }
                }

                commandBuffer->setOracleAnalyser(oracleAnalyser);

                if (mode == MODE_ONLINE) {
                    oracleAnalyser->initialize();

                    string keysStr("");
                    vector<string> keys;
                    if (source.HasMember("event-table")) {
                        const Value& eventtable = source["event-table"];
                        oracleAnalyser->addTable(eventtable.GetString(), keys, keysStr, 1);
                    }

                    const Value& tables = getJSONfield(fileName, source, "tables");
                    if (!tables.IsArray())
                        throw ConfigurationException("bad JSON, tables should be array");

                    for (SizeType j = 0; j < tables.Size(); ++j) {
                        const Value& table = getJSONfield(fileName, tables[j], "table");

                        if (tables[j].HasMember("key")) {
                            const Value& key = tables[j]["key"];
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
                        oracleAnalyser->addTable(table.GetString(), keys, keysStr, 0);
                        keys.clear();
                    }

                    oracleAnalyser->writeSchema();
                } else {
                    if (!oracleAnalyser->readSchema())
                        throw ConfigurationException("can't read schema from <database>-schema.json");
                }

                if (pthread_create(&oracleAnalyser->pthread, nullptr, &OracleAnalyser::runStatic, (void*)oracleAnalyser))
                    throw ConfigurationException("error spawning thread");

                analysers.push_back(oracleAnalyser);
                oracleAnalyser = nullptr;
            }
        }

        //iterate through targets
        const Value& targets = getJSONfield(fileName, document, "targets");
        if (!targets.IsArray())
            throw ConfigurationException("bad JSON, targets should be array");
        for (SizeType i = 0; i < targets.Size(); ++i) {
            const Value& target = targets[i];
            const Value& type = getJSONfield(fileName, target, "type");

            if (strcmp("KAFKA", type.GetString()) == 0) {
                const Value& alias = getJSONfield(fileName, target, "alias");
                const Value& brokers = getJSONfield(fileName, target, "brokers");
                const Value& source = getJSONfield(fileName, target, "source");
                const Value& format = getJSONfield(fileName, target, "format");

                const Value& streamJSON = getJSONfield(fileName, format, "stream");
                uint64_t stream = 0;
                if (strcmp("JSON", streamJSON.GetString()) == 0)
                    stream = STREAM_JSON;
                else if (strcmp("DBZ-JSON", streamJSON.GetString()) == 0)
                    stream = STREAM_DBZ_JSON;
                else
                    throw ConfigurationException("bad JSON, invalid stream type");

                const Value& topic = getJSONfield(fileName, format, "topic");

                //optional
                uint64_t metadata = 0;
                if (format.HasMember("metadata")) {
                    const Value& metadataJSON = format["metadata"];
                    metadata = metadataJSON.GetUint64();
                }

                //optional
                uint64_t singleDml = 0;
                if (format.HasMember("single-dml")) {
                    const Value& singleDmlJSON = format["single-dml"];
                    singleDml = singleDmlJSON.GetUint64();
                }

                //optional
                uint64_t showColumns = 0;
                if (format.HasMember("show-columns")) {
                    const Value& showColumnsJSON = format["show-columns"];
                    showColumns = showColumnsJSON.GetUint64();
                }

                //optional
                uint64_t test = 0;
                if (format.HasMember("test")) {
                    const Value& testJSON = format["test"];
                    test = testJSON.GetUint64();
                }

                //optional
                uint64_t timestampFormat = 0;
                if (format.HasMember("timestamp-format")) {
                    const Value& timestampFormatJSON = format["timestamp-format"];
                    timestampFormat = timestampFormatJSON.GetUint64();
                }

                OracleAnalyser *oracleAnalyser = nullptr;

                for (Thread *analyser : analysers)
                    if (analyser->alias.compare(source.GetString()) == 0)
                        oracleAnalyser = (OracleAnalyser*)analyser;
                if (oracleAnalyser == nullptr)
                    throw ConfigurationException("bad JSON, unknown alias");

                cout << "Adding target: " << alias.GetString() << endl;
                kafkaWriter = new KafkaWriter(alias.GetString(), brokers.GetString(), topic.GetString(), oracleAnalyser, trace, trace2,
                        stream, metadata, singleDml, showColumns, test, timestampFormat);
                if (kafkaWriter == nullptr)
                    throw MemoryException("main.4", sizeof(KafkaWriter));

                oracleAnalyser->commandBuffer->writer = kafkaWriter;
                oracleAnalyser->commandBuffer->test = test;
                oracleAnalyser->commandBuffer->timestampFormat = timestampFormat;

                kafkaWriter->initialize();
                if (pthread_create(&kafkaWriter->pthread, nullptr, &KafkaWriter::runStatic, (void*)kafkaWriter))
                    throw ConfigurationException("error spawning thread");

                writers.push_back(kafkaWriter);
                kafkaWriter = nullptr;
            }
        }

        //sleep until killed
        {
            unique_lock<mutex> lck(mainMtx);
            mainThread.wait(lck);
        }

    } catch(ConfigurationException &ex) {
        cerr << "ERROR: configuration error: " << ex.msg << endl;
    } catch(RuntimeException &ex) {
        cerr << "ERROR: runtime: " << ex.msg << endl;
    } catch (MemoryException &e) {
        cerr << "ERROR: memory allocation error for " << e.msg << " for " << e.bytes << " bytes" << endl;
    }

    if (oracleAnalyser != nullptr) {
        delete oracleAnalyser;
        oracleAnalyser = nullptr;
    }

    if (kafkaWriter != nullptr) {
        delete kafkaWriter;
        kafkaWriter = nullptr;
    }

    for (Thread *analyser : analysers)
        analyser->stop();
    for (CommandBuffer *commandBuffer : buffers) {
        unique_lock<mutex> lck(commandBuffer->mtx);
        commandBuffer->writerCond.notify_all();
    }
    for (Thread *analyser : analysers) {
        analyser->stop();
        pthread_join(analyser->pthread, nullptr);
        delete analyser;
    }
    analysers.clear();

    for (Thread *writer : writers)
        writer->stop();
    for (CommandBuffer *commandBuffer : buffers) {
        unique_lock<mutex> lck(commandBuffer->mtx);
        commandBuffer->analysersCond.notify_all();
    }
    for (Thread *writer : writers) {
        pthread_join(writer->pthread, nullptr);
        delete writer;
    }
    writers.clear();

    for (CommandBuffer *commandBuffer : buffers) {
        commandBuffer->stop();
        delete commandBuffer;
    }
    buffers.clear();

    return 0;
}
