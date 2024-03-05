---
name: Submit bug
about: Submit a bug report
title: ''
labels: ''
assignees: ''

---

**A brief description of the bug.**
To make the fix possible, the bug report should contain a clear and concise description of the problem.

**Is the bug present on the latest master branch.**
Verify if the bug is present on the latest master branch.

**Describe steps required to reproduce the bug.**
It is not enough just to claim hat some bug appeared.
The database can have multiple configuration parameters, and the bug can be related to some specific configuration.
To make the fix possible, the bug report should contain a clear and concise description of the problem.
It is important that the fix would not introduce new bugs or make the system unstable.
A list of steps required to reproduce the bug.

1. Take docker image for OLR + Oracle (preferred: https://github.com/bersler/OpenLogReplicator-tutorials so that the fault can be reproduced on Oracle XE)
2. Set configuration of OLR to ... (preferred: json file)
3. List of commands required to achieve the error, for example:
   - Run some SQL commands,
   - Restart of the database or OpenLogReplicator,
   - Manual operation, for example delet or create of checkpoint file,
   - etc.
4. Information about expected error

**For bugs related to Redo Log parse error where reproduction is not possible.**
This is for cases, where the fault is related to actual redo log data parsing.
There is some redo log file which is causing the error, but it is not known which actual combination of SQL commands caused the error.
To make a fix possible, the redo log file is required (even if the schema file or checkpoint file is not available).
Please provide the redo log file.

Provide the following information:

1. OLR configuration file (necessary)
2. Set of redo log files which contain error (necessary)
3. OLR checkpoint file set (helpful, but not crucial)
4. Information about expected error

**Additional context**
Add any other context or screenshots about the feature request here.
