# OpenLogReplicator

This project contains open source Oracle database CDC written purely in C++.
OpenLogReplicator reads transactions directly from database redo log files (parses binary files) and streams in JSON or Protobuf format to various targets.
The following targets are supported:

- Kafka
- flat file
- network stream (plain TCP/IP or ZeroMQ)
- discard (for testing purposes)

**Remember** that OpenLogReplicator has been released under **GPL license**. 
If you have received any software, that is based on OpenLogReplicator, you have the right to demand the full source code of the software.
For details, please refer to [LICENSE](LICENSE) file.

## Getting started

Refer to [OpenLogReplicator tutorials repository](https://github.com/bersler/OpenLogReplicator-tutorials) for a quick start with the project.

## Documentation

1. [Introduction to OpenLogReplicator](documentation/introduction/introduction.adoc)
2. User Manual
   1. [Getting Started](documentation/user-manual/1.getting-started.adoc)
   2. [Architecture and components](documentation/user-manual/2.architecture-and-components.adoc)
   3. [Output format](documentation/user-manual/3.output-format.adoc)
   4. [Output target](documentation/user-manual/4.output-target.adoc)
   5. [Supported features](documentation/user-manual/5.supported-features.adoc)
   6. [Replication](documentation/user-manual/6.replication.adoc)
   7. [Startup](documentation/user-manual/7.startup.adoc)
   8. [Checkpointing](documentation/user-manual/8.checkpointing.adoc)
   9. [Advanced topics](documentation/user-manual/9.advanced-topics.adoc)
3. [Reference Manual](documentation/reference-manual/reference-manual.adoc)
4. [OpenLogReplicator.json config file](documentation/json/json.adoc)
   1. [source](documentation/json/1.source.adoc)
   2. [reader](documentation/json/2.reader.adoc)
   3. [debug](documentation/json/3.debug.adoc)
   4. [format](documentation/json/4.format.adoc)
   5. [filter](documentation/json/5.filter.adoc)
   6. [table](documentation/json/6.table.adoc)
   7. [target](documentation/json/7.target.adoc)
   8. [writer](documentation/json/8.writer.adoc)
   9. [memory](documentation/json/9.memory.adoc)
   10. [metrics](documentation/json/10.metrics.adoc)
   11. [state](documentation/json/11.state.adoc)
5. [Installation Guide](documentation/installation/installation.adoc)
6. [Release Notes](documentation/release-notes/release-notes.adoc)
7. [Metrics](documentation/metrics/metrics.adoc)
8. [Troubleshooting Guide](documentation/troubleshooting/troubleshooting.adoc)
9. [Log Message Guide](documentation/log-messages/log-messages.adoc)
   1. [Runtime Error Messages](documentation/log-messages/1.runtume-error-messages.adoc)
   2. [Data Error Messages](documentation/log-messages/2.data-error-messages.adoc)
   3. [Configuration Error Messages](documentation/log-messages/3.configuration-error-messages.adoc)
   4. [Redo Log Error Messages](documentation/log-messages/4.redo-log-error-messages.adoc)
   5. [Internal Error Messages](documentation/log-messages/5.internal-error-messages.adoc)
   6. [Warning Messages](documentation/log-messages/6.warning-messages.adoc)
   7. [Internal Warning Messages](documentation/log-messages/7.internal-warning-messages.adoc)
10. [Experimental Features](documentation/experimental-features/experimental-features.adoc)

Additionally:

1. [Tutorials](https://www.bersler.com/openlogreplicator/tutorials/)
2. [FAQ](https://www.bersler.com/openlogreplicator/faq/)
3. [Support](https://www.bersler.com/openlogreplicator/support/)
4. [Contribution](https://www.bersler.com/openlogreplicator/contribution/)

Use also [Gitter chat](https://gitter.im/bersler/OpenLogReplicator).

## Debezium

OpenLogReplicator can work in cooperation with [Debezium](https://debezium.io/) CDC as a replacement for LogMiner reader. 
Use the following table to find out which version of OpenLogReplicator is compatible with which version of Debezium.

| Debezium | OLR minimal version | OLR recommended version |
|:--------:|:-------------------:|:-----------------------:|
|   2.x    |        1.3.0        |          1.9.0          |
|   3.x    |        1.3.0        |          1.9.0          |

## Support OpenLogReplicator

If you feel that I should receive some feedback from the world to continue with my hard work - feel free to make a **donation** or become a **sponsor**.

I am very grateful for any amount you donate.

[![Sponsor via GitHub](https://img.shields.io/badge/Sponsor-GitHub-brightgreen)](https://github.com/sponsors/bersler)
[![Librepay](https://img.shields.io/badge/Donate-Librepay-orange)](https://liberapay.com/bersler)
[![Buy Me a Coffee](https://img.shields.io/badge/Donate-Coffee-yellow)](https://www.buymeacoffee.com/bersler)
