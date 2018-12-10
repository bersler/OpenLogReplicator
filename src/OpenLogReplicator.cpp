/* Main class for the program
   Copyright (C) 2018 Adam Leszczynski.

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
#include <rapidjson/document.h>
#include "OracleEnvironment.h"
#include "JsonBuffer.h"
#include "OracleReader.h"
#include "KafkaWriter.h"

using namespace std;
using namespace rapidjson;
using namespace OpenLogReplicator;
using namespace OpenLogReplicatorOracle;
using namespace OpenLogReplicatorKafka;

const Value& getJSONfield(const Value& value, const char* field) {
	if (!value.HasMember(field)) {
		cerr << "ERROR: Bad JSON: field " << field << " not found" << endl;
		exit(1);
	}
	return value[field];
}

const Value& getJSONfield(const Document& document, const char* field) {
	if (!document.HasMember(field)) {
		cerr << "ERROR: Bad JSON: field " << field << " not found" << endl;
		exit(1);
	}
	return document[field];
}

mutex mainMtx;
condition_variable mainThread;
void signalHandler(int s) {
	cout << "Caught signal " << s << ", exiting" << endl;
	unique_lock<mutex> lck(mainMtx);
	mainThread.notify_all();
}

int main() {
	signal (SIGINT, signalHandler);
	cout << "Open Log Replicator v. 0.0.1 (C) 2018 by Adam Leszcznski, aleszczynski@bersler.com" << endl;

	ifstream config("OpenLogReplicator.json");
	string configJSON((istreambuf_iterator<char>(config)), istreambuf_iterator<char>());
	Document document;
	list<Thread *> readers, writers;
	list<JsonBuffer *> buffers;

    if (configJSON.length() == 0 || document.Parse(configJSON.c_str()).HasParseError())
    	{cerr << "ERROR: parsing OpenLogReplicator.json" << endl; return 1;}

    const Value& version = getJSONfield(document, "version");
    if (strcmp(version.GetString(), "0.0.1") != 0)
    	{cerr << "ERROR: bad JSON, incompatible version!" << endl; return 1;}

    const Value& dumpLogFile = getJSONfield(document, "dumplogfile");
    bool dumpLogFileBool = false;
    if (strcmp(dumpLogFile.GetString(), "1") == 0)
    	dumpLogFileBool = true;

    const Value& dumpData = getJSONfield(document, "dumpdata");
    bool dumpDataBool = false;
    if (strcmp(dumpData.GetString(), "1") == 0)
    	dumpDataBool = true;

    const Value& directRead = getJSONfield(document, "directread");
    bool directReadBool = false;
    if (strcmp(directRead.GetString(), "1") == 0)
    	directReadBool = true;

    //iterate through sources
    const Value& sources = getJSONfield(document, "sources");
    if (!sources.IsArray())
    	{cerr << "ERROR: bad JSON, sources should be array!" << endl; return 1;}
    for (SizeType i = 0; i < sources.Size(); ++i) {
    	const Value& source = sources[i];
    	const Value& type = getJSONfield(source, "type");

    	if (strcmp("ORACLE11204", type.GetString()) == 0) {
    		const Value& alias = getJSONfield(source, "alias");
    		const Value& name = getJSONfield(source, "name");
        	const Value& user = getJSONfield(source, "user");
        	const Value& password = getJSONfield(source, "password");
        	const Value& server = getJSONfield(source, "server");
        	const Value& tables = getJSONfield(source, "tables");
        	if (!tables.IsArray())
        		{cerr << "ERROR: bad JSON, objects should be array!" << endl; return 1;}

        	cout << "Adding source: " << name.GetString() << endl;
        	JsonBuffer *jsonBuffer = new JsonBuffer();
        	buffers.push_back(jsonBuffer);
        	OracleReader *oracleReader = new OracleReader(jsonBuffer, alias.GetString(), name.GetString(), user.GetString(),
        			password.GetString(), server.GetString(), dumpLogFileBool, dumpDataBool, directReadBool);
        	readers.push_back(oracleReader);

        	//initialize
        	if (!oracleReader->initialize()) {
        		delete oracleReader;
        		oracleReader = nullptr;
        		return -1;
        	}

        	for (SizeType j = 0; j < tables.Size(); ++j) {
        		const Value& table = getJSONfield(tables[j], "table");
        		oracleReader->addTable(table.GetString());
        	}

        	//run
        	pthread_create(&oracleReader->thread, nullptr, &OracleReader::runStatic, (void*)oracleReader);
    	}
    }

    //iterate through sources
    const Value& targets = getJSONfield(document, "targets");
    if (!targets.IsArray())
    	{cerr << "ERROR: bad JSON, targets should be array!" << endl; return 1;}
    for (SizeType i = 0; i < targets.Size(); ++i) {
    	const Value& target = targets[i];
    	const Value& type = getJSONfield(target, "type");

    	if (strcmp("KAFKA", type.GetString()) == 0) {
    		const Value& alias = getJSONfield(target, "alias");
    		const Value& brokers = getJSONfield(target, "brokers");
    		const Value& topic = getJSONfield(target, "topic");
    		const Value& source = getJSONfield(target, "source");
        	JsonBuffer *jsonBuffer = nullptr;

			for (auto reader : readers) {
				if (reader->alias.compare(source.GetString()) == 0)
					jsonBuffer = reader->jsonBuffer;
			}
			if (jsonBuffer == nullptr)
	    		{cerr << "ERROR: Alias " << alias.GetString() << " not found!" << endl; return 1;}

        	cout << "Adding target: " << alias.GetString() << endl;
        	KafkaWriter *kafkaWriter = new KafkaWriter(alias.GetString(), brokers.GetString(), topic.GetString(), jsonBuffer);
        	writers.push_back(kafkaWriter);

        	//initialize
        	if (!kafkaWriter->initialize()) {
        		delete kafkaWriter;
        		kafkaWriter = nullptr;
        		cerr << "ERROR: Kafka starting writer for " << brokers.GetString() << " topic " << topic.GetString() << endl;
        		return -1;
        	}

        	//run
        	pthread_create(&kafkaWriter->thread, nullptr, &OracleReader::runStatic, (void*)kafkaWriter);
    	}
    }

    //sleep until killed
    {
    	unique_lock<mutex> lck(mainMtx);
    	mainThread.wait(lck);
    }


    //stop gently all threads
	for (auto writer : writers)
		writer->terminate();
	for (auto reader : readers)
		reader->terminate();
	for (auto jsonBuffer : buffers)
		jsonBuffer->terminate();

	for (auto jsonBuffer : buffers) {
		unique_lock<mutex> lck(jsonBuffer->mtx);
		jsonBuffer->readers.notify_all();
		jsonBuffer->writer.notify_all();
	}

    cout << "Waiting for writers to terminate" << endl;
	for (auto writer : writers) {
		pthread_join(writer->thread, nullptr);
		delete writer;
		cout << "- stopped" << endl;
	}
	writers.clear();

    cout << "Waiting for readers to terminate" << endl;
	for (auto reader : readers) {
		reader->terminate();
		pthread_join(reader->thread, nullptr);
		delete reader;
		cout << "- stopped" << endl;
	}
	readers.clear();

	for (auto jsonBuffer : buffers)
		delete jsonBuffer;
	buffers.clear();

	return 0;
}
