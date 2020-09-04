# OpenLogReplicator
Open Source logbased replictor of Oracle Database to Kafka

Compilation for Debug:
1. git clone https://github.com/bersler/OpenLogReplicator
2. cd OpenLogReplicator
3. ./configure CXXFLAGS='-g -O0 -fsanitize=address' --with-rapidjson=/opt/rapidjson --with-rdkafka=/opt/librdkafka --with-instantclient=/opt/instantclient_19_8
4. make

Compilation for Release:
1. git clone https://github.com/bersler/OpenLogReplicator
2. cd OpenLogReplicator
3. ./configure CXXFLAGS='-O3' --with-rapidjson=/opt/rapidjson --with-rdkafka=/opt/librdkafka --with-instantclient=/opt/instantclient_19_8
4. make

The documentation for the OpenLogReplicator program can be found on https://www.bersler.com/openlogreplicator/

The currently available chapters are:

1. Getting started:
https://www.bersler.com/openlogreplicator/getting-started/

2. Installation:
https://www.bersler.com/openlogreplicator/installation/

3. Configuration:
https://www.bersler.com/openlogreplicator/configuration/

4. Tutorials:
https://www.bersler.com/openlogreplicator/tutorials/

5. FAQ:
https://www.bersler.com/openlogreplicator/faq/

6. Support:
https://www.bersler.com/openlogreplicator/support/

7. Contribution:
https://www.bersler.com/openlogreplicator/contribution/
