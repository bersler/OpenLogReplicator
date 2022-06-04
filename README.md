# OpenLogReplicator
Open Source Oracle database CDC written purely in C++. Reads transactions directly from database redo log files and streams in JSON or Protobuf format to:
* Kafka
* flat file
* network stream (plain TCP/IP or ZeroMQ)

Updating Protobuf code:
1. cd proto
2. export PATH=/opt/protobuf/bin:$PATH
3. protoc OraProtoBuf.proto --cpp_out=../src/main

Please refer to Docker scripts in https://github.com/bersler/OpenLogReplicator-docker for compilation and run scripts

The documentation for the OpenLogReplicator program can be found on https://www.bersler.com/openlogreplicator/

Please do not create issues claiming that the documentation is missing. It is still being created. This is not speed up the process, but will it will down it down. Instead of writing documenation and finishing the code the time is spent on answering questions.

The currently available chapters are:

1. Getting started: https://www.bersler.com/openlogreplicator/getting-started/
2. Installation: https://www.bersler.com/openlogreplicator/installation/
3. Configuration: https://www.bersler.com/openlogreplicator/configuration/
4. Tutorials: https://www.bersler.com/openlogreplicator/tutorials/
5. FAQ: https://www.bersler.com/openlogreplicator/faq/
6. Support: https://www.bersler.com/openlogreplicator/support/
7. Contribution: https://www.bersler.com/openlogreplicator/contribution/

I have also opened a gitter chat at https://gitter.im/bersler/OpenLogReplicator
