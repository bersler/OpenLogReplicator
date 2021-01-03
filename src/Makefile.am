#   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)
#
#This file is part of OpenLogReplicator.
#
#OpenLogReplicator is free software; you can redistribute it and/or
#modify it under the terms of the GNU General Public License as published
#by the Free Software Foundation; either version 3, or (at your option)
#any later version.
#
#OpenLogReplicator is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
#Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with OpenLogReplicator; see the file LICENSE;  If not see
#<http://www.gnu.org/licenses/>.

bin_PROGRAMS=OpenLogReplicator
OpenLogReplicator_SOURCES = CharacterSet16bit.cpp \
CharacterSet7bit.cpp \
CharacterSet8bit.cpp \
CharacterSetAL16UTF16.cpp \
CharacterSetAL32UTF8.cpp \
CharacterSet.cpp \
CharacterSetJA16EUC.cpp \
CharacterSetJA16EUCTILDE.cpp \
CharacterSetJA16SJIS.cpp \
CharacterSetJA16SJISTILDE.cpp \
CharacterSetKO16KSCCS.cpp \
CharacterSetUTF8.cpp \
CharacterSetZHS16GBK.cpp \
CharacterSetZHS32GB18030.cpp \
CharacterSetZHT16HKSCS31.cpp \
CharacterSetZHT32EUC.cpp \
CharacterSetZHT32TRIS.cpp \
ConfigurationException.cpp \
NetworkException.cpp \
OpCode0501.cpp \
OpCode0502.cpp \
OpCode0504.cpp \
OpCode0506.cpp \
OpCode050B.cpp \
OpCode0513.cpp \
OpCode0514.cpp \
OpCode0B02.cpp \
OpCode0B03.cpp \
OpCode0B04.cpp \
OpCode0B05.cpp \
OpCode0B06.cpp \
OpCode0B08.cpp \
OpCode0B0B.cpp \
OpCode0B0C.cpp \
OpCode0B10.cpp \
OpCode1801.cpp \
OpCode.cpp \
OpenLogReplicator.cpp \
OracleAnalyzer.cpp \
OracleAnalyzerBatch.cpp \
OracleColumn.cpp \
OracleObject.cpp \
OutputBuffer.cpp \
OutputBufferJson.cpp \
Reader.cpp \
ReaderFilesystem.cpp \
RedoLog.cpp \
RedoLogException.cpp \
RedoLogRecord.cpp \
RowId.cpp \
RuntimeException.cpp \
Schema.cpp \
SchemaElement.cpp \
Thread.cpp \
TransactionBuffer.cpp \
Transaction.cpp \
Writer.cpp \
WriterFile.cpp \
uintX_t.cpp

if OCI_COMPILE
OpenLogReplicator_SOURCES += DatabaseConnection.cpp \
DatabaseEnvironment.cpp \
DatabaseStatement.cpp \
OracleAnalyzerOnline.cpp \
OracleAnalyzerOnlineASM.cpp \
ReaderASM.cpp
endif

if KAFKA_COMPILE
OpenLogReplicator_SOURCES += WriterKafka.cpp
endif

if PROTOBUF_COMPILE
OpenLogReplicator_SOURCES += OraProtoBuf.pb.cpp \
OutputBufferProtobuf.cpp \
Stream.cpp \
StreamNetwork.cpp \
WriterStream.cpp
StreamClient_SOURCES = StreamClient.cpp \
OraProtoBuf.pb.cpp \
NetworkException.cpp \
RuntimeException.cpp \
Stream.cpp \
StreamNetwork.cpp
bin_PROGRAMS += StreamClient
if ZEROMQ_COMPILE
OpenLogReplicator_SOURCES += StreamZeroMQ.cpp
StreamClient_SOURCES += StreamZeroMQ.cpp
endif
endif
