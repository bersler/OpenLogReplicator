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
#include "OracleEnvironment.h"
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
    cerr << "Caught signal " << s << ", exiting" << endl;
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
    cout << "Open Log Replicator v. 0.4.0 (C) 2018-2020 by Adam Leszczynski, aleszczynski@bersler.com, see LICENSE file for licensing information" << endl;
    list<Thread *> readers, writers;
    list<CommandBuffer *> buffers;

    try {
        ifstream config("OpenLogReplicator.json");
        string configJSON((istreambuf_iterator<char>(config)), istreambuf_iterator<char>());
        Document document;

        if (configJSON.length() == 0 || document.Parse(configJSON.c_str()).HasParseError())
            {cerr << "ERROR: parsing OpenLogReplicator.json" << endl; return 1;}

        const Value& version = getJSONfield(document, "version");
        if (strcmp(version.GetString(), "0.4.0") != 0)
            {cerr << "ERROR: bad JSON, incompatible version!" << endl; return 1;}

        const Value& dumpLogFile = getJSONfield(document, "dumplogfile");
        uint32_t dumpLogFileInt = 0;
        dumpLogFileInt = atoi(dumpLogFile.GetString());

        const Value& trace = getJSONfield(document, "trace");
        uint32_t traceInt = 0;
        traceInt = atoi(trace.GetString());

        const Value& dumpData = getJSONfield(document, "dumpdata");
        bool dumpDataBool = false;
        if (strcmp(dumpData.GetString(), "1") == 0)
            dumpDataBool = true;

        const Value& directRead = getJSONfield(document, "directread");
        bool directReadBool = false;
        if (strcmp(directRead.GetString(), "1") == 0)
            directReadBool = true;

        const Value& sortCols = getJSONfield(document, "sortcols");
        uint32_t sortColsInt = 0;
        sortColsInt = atoi(sortCols.GetString());

        const Value& redoBuffers = getJSONfield(document, "redo-buffers");
        uint32_t redoBuffersInt = 0;
        redoBuffersInt = atoi(redoBuffers.GetString());

        const Value& redoBufferSize = getJSONfield(document, "redo-buffer-size");
        uint32_t redoBufferSizeInt = 0;
        redoBufferSizeInt = atoi(redoBufferSize.GetString());

        const Value& outputBufferSize = getJSONfield(document, "output-buffer-size");
        uint32_t outputBufferSizeInt = 0;
        outputBufferSizeInt = atoi(outputBufferSize.GetString());

        const Value& maxConcurrentTransactions = getJSONfield(document, "max-concurrent-transactions");
        uint32_t maxConcurrentTransactionsInt = 0;
        maxConcurrentTransactionsInt = atoi(maxConcurrentTransactions.GetString());

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
                CommandBuffer *commandBuffer = new CommandBuffer(outputBufferSizeInt);

                buffers.push_back(commandBuffer);
                OracleReader *oracleReader = new OracleReader(commandBuffer, alias.GetString(), name.GetString(), user.GetString(),
                        password.GetString(), server.GetString(), traceInt, dumpLogFileInt, dumpDataBool, directReadBool, sortColsInt,
                        redoBuffersInt, redoBufferSizeInt, maxConcurrentTransactionsInt);
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
                const Value& topic = getJSONfield(target, "topic");
                const Value& source = getJSONfield(target, "source");
                const Value& traceKafka = getJSONfield(target, "trace");
                CommandBuffer *commandBuffer = nullptr;

                for (auto reader : readers)
                    if (reader->alias.compare(source.GetString()) == 0)
                        commandBuffer = reader->commandBuffer;
                if (commandBuffer == nullptr)
                    {cerr << "ERROR: Alias " << alias.GetString() << " not found!" << endl; return 1;}

                int traceKafkaInt = 0;
                traceKafkaInt = atoi(traceKafka.GetString());

                cout << "Adding target: " << alias.GetString() << endl;
                KafkaWriter *kafkaWriter = new KafkaWriter(alias.GetString(), brokers.GetString(), topic.GetString(), commandBuffer, traceKafkaInt);
                commandBuffer->writer = kafkaWriter;
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

    } catch (exception e) {
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
