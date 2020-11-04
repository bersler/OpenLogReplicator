# OpenLogReplicator
Open Source logbased replictor of Oracle Database to Kafka

Please mind that the code has 2 branches:
1. master - branch with stable code - updated monthly
2. nightly - unstable current branch with daily code updates

Updating GRPC/Protobuf code:
1. cd proto
2. export PATH=/opt/grpc/bin:$PATH
3. protoc OraProtoBuf.proto --cpp_out=.
4. protoc OraProtoBuf.proto --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` --grpc_out=.
5. mv OraProtoBuf.pb.cc ../src/OraProtoBuf.pb.cpp
6. mv OraProtoBuf.pb.h ../src/OraProtoBuf.pb.h
7. mv OraProtoBuf.grpc.pb.cc ../src/OraProtoBuf.grpc.pb.cpp
8. mv OraProtoBuf.grpc.pb.h ../src/OraProtoBuf.grpc.pb.h

Compilation for Debug:
1. git clone https://github.com/bersler/OpenLogReplicator
2. cd OpenLogReplicator
3. autoreconf -f -i
4. ./configure CXXFLAGS='-g -O0 -fsanitize=address' --with-rapidjson=/opt/rapidjson --with-rdkafka=/opt/librdkafka --with-instantclient=/opt/instantclient_19_8 --with-grpc=/opt/grpc
5. make

Compilation for Release:
1. git clone https://github.com/bersler/OpenLogReplicator
2. cd OpenLogReplicator
3. autoreconf -f -i
4. ./configure CXXFLAGS='-O3' --with-rapidjson=/opt/rapidjson --with-rdkafka=/opt/librdkafka --with-instantclient=/opt/instantclient_19_8 --with-grpc=/opt/grpc
5. make

Step 3 is optional and required if you downloaded the files from GIT and timestamps of files may be changed.

Running:
1. cp sample/OpenLogReplicator.json.example OpenLogReplicator.json
2. vi OpenLogReplicator.json
3. export LD_LIBRARY_PATH=/opt/instantclient_19_8:/opt/grpc/lib:/opt/grpc/lib64:/opt/librdkafka/lib
4. ./src/OpenLogReplicator

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
