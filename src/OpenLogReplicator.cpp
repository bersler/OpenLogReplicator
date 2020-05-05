/* Main class for the program
   Copyright (C) 2018-2020 Adam Leszczynski.

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

#include <iostream>
#include <cstdio>
#include <string>
#include <fstream>
#include <streambuf>
#include <list>
#include <mutex>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <execinfo.h>
#include <rapidjson/document.h>

#include "CommandBuffer.h"
#include "OracleReader.h"
#include "KafkaWriter.h"

using namespace std;
using namespace rapidjson;
using namespace OpenLogReplicator;

const Value& getJSONfield(const Value& value, const char* field) {
    if (!value.HasMember(field)) {
        cerr << "ERROR: Bad JSON: field " << field << " not found" << endl;
        throw new exception;
    }
    return value[field];
}

const Value& getJSONfield(const Document& document, const char* field) {
    if (!document.HasMember(field)) {
        cerr << "ERROR: Bad JSON: field " << field << " not found" << endl;
        throw new exception;
    }
    return document[field];
}

mutex mainMtx;
condition_variable mainThread;

void stopMain() {
    unique_lock<mutex> lck(mainMtx);
    mainThread.notify_all();
}

void signalHandler(int s) {
    cout << "Caught signal " << s << ", exiting" << endl;
    stopMain();
}

void signalCrash(int sig) {
    void *array[32];
    size_t size = backtrace(array, 32);
    cerr << "Error: signal " << dec << sig << endl;
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    exit(1);
}

int main() {
    signal(SIGINT, signalHandler);
    signal(SIGPIPE, signalHandler);
    signal(SIGSEGV, signalCrash);
    cout << "Open Log Replicator v. 0.5.1 (C) 2018-2020 by Adam Leszczynski, aleszczynski@bersler.com, see LICENSE file for licensing information" << endl;
    list<Thread *> readers, writers;
    list<CommandBuffer *> buffers;

    try {
        ifstream config("OpenLogReplicator.json");
        string configJSON((istreambuf_iterator<char>(config)), istreambuf_iterator<char>());
        Document document;

        if (configJSON.length() == 0 || document.Parse(configJSON.c_str()).HasParseError())
            {cerr << "ERROR: parsing OpenLogReplicator.json" << endl; return 1;}

        const Value& version = getJSONfield(document, "version");
        if (strcmp(version.GetString(), "0.5.1") != 0)
            {cerr << "ERROR: bad JSON, incompatible version!" << endl; return 1;}

        const Value& dumpRedoLogJSON = getJSONfield(document, "dump-redo-log");
        uint64_t dumpRedoLog = dumpRedoLogJSON.GetUint64();

        const Value& traceJSON = getJSONfield(document, "trace");
        uint64_t trace = traceJSON.GetUint64();

        const Value& trace2JSON = getJSONfield(document, "trace2");
        uint64_t trace2 = trace2JSON.GetUint64();

        const Value& dumpRawDataJSON = getJSONfield(document, "dump-raw-data");
        uint64_t dumpRawData = dumpRawDataJSON.GetUint64();

        const Value& directReadJSON = getJSONfield(document, "direct-read");
        uint64_t directRead = directReadJSON.GetUint64();

        const Value& checkpointIntervalJSON = getJSONfield(document, "checkpoint-interval");
        uint64_t checkpointInterval = checkpointIntervalJSON.GetUint64();

        //optional
        uint64_t redoBufferSize = 65536;
        if (document.HasMember("redo-buffer-size")) {
            const Value& redoBufferSizeJSON = getJSONfield(document, "redo-buffer-size");
            redoBufferSize = redoBufferSizeJSON.GetUint64();
        }
        if (redoBufferSize == 0 || redoBufferSize > 1048576)
            redoBufferSize = 1048576;

        const Value& redoBuffersJSON = getJSONfield(document, "redo-buffer-mb");
        uint64_t redoBuffers = redoBuffersJSON.GetUint64() * (1048576 / redoBufferSize);

        const Value& outputBufferSizeJSON = getJSONfield(document, "output-buffer-mb");
        uint64_t outputBufferSize = outputBufferSizeJSON.GetUint64() * 1048576;

        const Value& maxConcurrentTransactionsJSON = getJSONfield(document, "max-concurrent-transactions");
        uint64_t maxConcurrentTransactions = maxConcurrentTransactionsJSON.GetUint64();

        //iterate through sources
        const Value& sources = getJSONfield(document, "sources");
        if (!sources.IsArray())
            {cerr << "ERROR: bad JSON, sources should be an array!" << endl; return 1;}
        for (SizeType i = 0; i < sources.Size(); ++i) {
            const Value& source = sources[i];
            const Value& type = getJSONfield(source, "type");

            if (strcmp("ORACLE", type.GetString()) == 0) {
                const Value& alias = getJSONfield(source, "alias");
                const Value& name = getJSONfield(source, "name");
                const Value& user = getJSONfield(source, "user");
                const Value& password = getJSONfield(source, "password");
                const Value& server = getJSONfield(source, "server");
                const Value& eventtable = getJSONfield(source, "eventtable");
                const Value& tables = getJSONfield(source, "tables");
                if (!tables.IsArray())
                    {cerr << "ERROR: bad JSON, objects should be array!" << endl; return 1;}

                cout << "Adding source: " << name.GetString() << endl;
                CommandBuffer *commandBuffer = new CommandBuffer(outputBufferSize);

                buffers.push_back(commandBuffer);
                OracleReader *oracleReader = new OracleReader(commandBuffer, alias.GetString(), name.GetString(), user.GetString(),
                        password.GetString(), server.GetString(), trace, trace2, dumpRedoLog, dumpRawData, directRead,
                        checkpointInterval, redoBuffers, redoBufferSize, maxConcurrentTransactions);
                readers.push_back(oracleReader);

                //initialize
                if (!oracleReader->initialize()) {
                    delete oracleReader;
                    oracleReader = nullptr;
                    return -1;
                }

                oracleReader->addTable(eventtable.GetString(), 1);
                for (SizeType j = 0; j < tables.Size(); ++j) {
                    const Value& table = getJSONfield(tables[j], "table");
                    oracleReader->addTable(table.GetString(), 0);
                }

                //run
                pthread_create(&oracleReader->pthread, nullptr, &OracleReader::runStatic, (void*)oracleReader);
            }
        }

        //iterate through targets
        const Value& targets = getJSONfield(document, "targets");
        if (!targets.IsArray())
            {cerr << "ERROR: bad JSON, targets should be an array!" << endl; return 1;}
        for (SizeType i = 0; i < targets.Size(); ++i) {
            const Value& target = targets[i];
            const Value& type = getJSONfield(target, "type");

            if (strcmp("KAFKA", type.GetString()) == 0) {
                const Value& alias = getJSONfield(target, "alias");
                const Value& brokers = getJSONfield(target, "brokers");
                const Value& source = getJSONfield(target, "source");
                const Value& format = getJSONfield(target, "format");

                const Value& streamJSON = getJSONfield(format, "stream");
                uint64_t stream = 0;
                if (strcmp("JSON", streamJSON.GetString()) == 0)
                    stream = STREAM_JSON;
                else {cerr << "ERROR: bad JSON, only stream of type JSON is currently supported!" << endl; return 1;}

                const Value& topic = getJSONfield(format, "topic");
                const Value& sortColumnsJSON = getJSONfield(format, "sort-columns");
                uint64_t sortColumns = sortColumnsJSON.GetUint64();
                const Value& metadataJSON = getJSONfield(format, "metadata");
                uint64_t metadata = metadataJSON.GetUint64();
                const Value& singleDmlJSON = getJSONfield(format, "single-dml");
                uint64_t singleDml = singleDmlJSON.GetUint64();
                const Value& nullColumnsJSON = getJSONfield(format, "null-columns");
                uint64_t nullColumns = nullColumnsJSON.GetUint64();
                const Value& testJSON = getJSONfield(format, "test");
                uint64_t test = testJSON.GetUint64();

                OracleReader *oracleReader = nullptr;

                for (auto reader : readers)
                    if (reader->alias.compare(source.GetString()) == 0)
                        oracleReader = (OracleReader*)reader;
                if (oracleReader == nullptr)
                    {cerr << "ERROR: Alias " << alias.GetString() << " not found!" << endl; return 1;}

                cout << "Adding target: " << alias.GetString() << endl;
                KafkaWriter *kafkaWriter = new KafkaWriter(alias.GetString(), brokers.GetString(), topic.GetString(), oracleReader, trace, trace2,
                        stream, sortColumns, metadata, singleDml, nullColumns, test);
                oracleReader->commandBuffer->writer = kafkaWriter;
                writers.push_back(kafkaWriter);

                //initialize
                if (!kafkaWriter->initialize()) {
                    delete kafkaWriter;
                    kafkaWriter = nullptr;
                    cerr << "ERROR: Kafka starting writer for " << brokers.GetString() << " topic " << topic.GetString() << endl;
                    return -1;
                }

                //run
                pthread_create(&kafkaWriter->pthread, nullptr, &KafkaWriter::runStatic, (void*)kafkaWriter);
            }
        }

        //sleep until killed
        {
            unique_lock<mutex> lck(mainMtx);
            mainThread.wait(lck);
        }

    } catch (exception &e) {
        cerr << "ERROR parsing OpenLogReplicator.json" << endl;
    }


    for (auto reader : readers)
        reader->stop();
    for (auto commandBuffer : buffers) {
        unique_lock<mutex> lck(commandBuffer->mtx);
        commandBuffer->writerCond.notify_all();
    }
    for (auto reader : readers) {
        reader->stop();
        pthread_join(reader->pthread, nullptr);
        delete reader;
    }
    readers.clear();

    for (auto writer : writers)
        writer->stop();
    for (auto commandBuffer : buffers) {
        unique_lock<mutex> lck(commandBuffer->mtx);
        commandBuffer->readersCond.notify_all();
    }
    for (auto writer : writers) {
        pthread_join(writer->pthread, nullptr);
        delete writer;
    }
    writers.clear();

    //deactivate command buffers
    for (auto commandBuffer : buffers) {
        commandBuffer->stop();
        delete commandBuffer;
    }
    buffers.clear();

    return 0;
}
