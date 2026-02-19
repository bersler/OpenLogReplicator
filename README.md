# OpenLogReplicator

OpenLogReplicator is an open-source Oracle database Change Data Capture (CDC) solution written in C++.

It reads transactions directly from Oracle redo log files (by parsing binary redo logs) and streams changes in `JSON` or `Protobuf` format to various targets.

## Supported targets

- Kafka
- Flat file
- Network stream (plain TCP/IP or ZeroMQ)
- Discard (for testing)

---

## Stability, Reliability & Private Validation

OpenLogReplicator processes mission-critical data streams where correctness is non-negotiable.
While the source code is GPL, every official release is validated against a private, proprietary test suite with 1,000+ edge-case scenarios, including Oracle RAC, ASM, and multiple redo log versions.
This internal validation is the only way to guarantee stability and prevent silent data corruption in production.

---

## Important Note on AI-Generated Patches and Forks

The Oracle Redo Log format is undocumented and extremely complex.
Using AI-generated patches (for example, ChatGPT or GitHub Copilot) to modify the core parser, or maintaining private forks, is highly risky and can introduce undetectable corruption.
Production deployments should use only officially signed binaries that have passed the full internal regression suite.

---

## License and user rights

OpenLogReplicator is released under the **GNU General Public License (GPL)**.

If you have received software that is based on OpenLogReplicator, you are legally entitled to obtain the **full corresponding source code** of that software.

For details, see the [LICENSE](LICENSE) file.

---

## Getting started

For a quick start, refer to the [OpenLogReplicator tutorials repository](https://github.com/bersler/OpenLogReplicator-tutorials).

---

## Documentation

1. [Introduction to OpenLogReplicator](documentation/introduction/introduction.adoc)
2. **User Manual**
    1. [Getting Started](documentation/user-manual/1.getting-started.adoc)
    2. [Architecture and Components](documentation/user-manual/2.architecture-and-components.adoc)
    3. [Output Format](documentation/user-manual/3.output-format.adoc)
    4. [Output Targets](documentation/user-manual/4.output-target.adoc)
    5. [Supported Features](documentation/user-manual/5.supported-features.adoc)
    6. [Replication](documentation/user-manual/6.replication.adoc)
    7. [Startup](documentation/user-manual/7.startup.adoc)
    8. [Checkpointing](documentation/user-manual/8.checkpointing.adoc)
    9. [Advanced Topics](documentation/user-manual/9.advanced-topics.adoc)
3. [Reference Manual](documentation/reference-manual/reference-manual.adoc)
4. **Configuration**
    1. [`OpenLogReplicator.json`](documentation/json/json.adoc)
    2. [`source`](documentation/json/1.source.adoc)
    3. [`reader`](documentation/json/2.reader.adoc)
    4. [`debug`](documentation/json/3.debug.adoc)
    5. [`format`](documentation/json/4.format.adoc)
    6. [`filter`](documentation/json/5.filter.adoc)
    7. [`table`](documentation/json/6.table.adoc)
    8. [`target`](documentation/json/7.target.adoc)
    9. [`writer`](documentation/json/8.writer.adoc)
    10. [`memory`](documentation/json/9.memory.adoc)
    11. [`metrics`](documentation/json/10.metrics.adoc)
    12. [`state`](documentation/json/11.state.adoc)
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

### Additional resources

- [Tutorials](https://www.bersler.com/openlogreplicator/tutorials/)
- [FAQ](https://www.bersler.com/openlogreplicator/faq/)
- [Support](https://www.bersler.com/openlogreplicator/support/)
- [Contribution guidelines](https://www.bersler.com/openlogreplicator/contribution/)

Community discussion is available via [Gitter chat](https://gitter.im/bersler/OpenLogReplicator).

---

## Debezium integration

OpenLogReplicator can be used together with [Debezium](https://debezium.io/) CDC as a replacement for the LogMiner reader.

| Debezium | OLR minimal version | OLR recommended version |
|:--------:|:-------------------:|:-----------------------:|
| 2.x      | 1.3.0               | 1.9.0                   |
| 3.x      | 1.3.0               | 1.9.0                   |

---

## Project maintenance and sustainability

OpenLogReplicator is primarily developed and maintained by its author.

Bug reports, feature requests, and discussions are welcome.
Please note that **ongoing development, maintenance, and support require significant time and effort**.

- Issue reports are handled on a best-effort basis.
- Companies using OpenLogReplicator in production are encouraged to support its maintenance.

If OpenLogReplicator is critical to your systems, consider **financially supporting the project** or **contributing improvements upstream** instead of maintaining private forks.

---

## Commercial support and collaboration

Professional support, priority bug fixes, and feature development are available.

If you are:
- Running OpenLogReplicator in production
- Building commercial products or services on top of it
- Requiring guaranteed response times or roadmap influence

please consider the official support options at:
https://www.bersler.com/openlogreplicator/support/

---

## Support OpenLogReplicator

Our stability is powered by a dedicated hardware lab.
Learn more about our testing infrastructure and how you can support it in [SPONSORS.md](SPONSORS.md).
If you find OpenLogReplicator useful, consider supporting its development.

Your support directly helps ensure:
- Continued maintenance
- Faster bug fixes
- New features
- Long-term project sustainability

Any contribution is appreciated.

[![Sponsor via GitHub](https://img.shields.io/badge/Sponsor-GitHub-brightgreen)](https://github.com/sponsors/bersler)
[![Donate via Libeapay](https://img.shields.io/badge/Donate-Librepay-orange)](https://liberapay.com/bersler)
[![Buy Me a Coffee](https://img.shields.io/badge/Donate-Coffee-yellow)](https://www.buymeacoffee.com/bersler)

---

## Enterprise Partnership & Certification

Enterprise collaboration is focused on risk mitigation and certified releases, not generic support.
A Private Validation Service is available to test your configuration and workload against the internal regression suite and deliver certified results.

https://www.bersler.com/openlogreplicator/support/

---

## Infrastructure Sponsorship

Sponsorship funds maintain the dedicated Oracle testing lab, including servers, storage, and licensing required for full validation.
This directly sustains reliable releases and long-term project continuity.

See [SPONSORS.md](SPONSORS.md) for sponsorship tiers and enterprise options.
