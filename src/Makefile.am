#   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)
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
OpenLogReplicator_SOURCES=CharacterSet16bit.cpp \
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
DatabaseConnection.cpp \
DatabaseEnvironment.cpp \
DatabaseStatement.cpp \
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
OracleAnalyser.cpp \
OracleAnalyserRedoLog.cpp \
OracleColumn.cpp \
OracleObject.cpp \
OutputBuffer.cpp \
OutputBufferJson.cpp \
OutputBufferProtobuf.cpp \
ReaderASM.cpp \
Reader.cpp \
ReaderFilesystem.cpp \
RedoLogException.cpp \
RedoLogRecord.cpp \
RuntimeException.cpp \
Thread.cpp \
TransactionBuffer.cpp \
Transaction.cpp \
TransactionHeap.cpp \
TransactionMap.cpp \
Writer.cpp \
WriterFile.cpp \
WriterKafka.cpp \
WriterService.cpp

if PROTOBUF_COMPILE
OpenLogReplicator_SOURCES += OraProtoBuf.pb.cpp OraProtoBuf.grpc.pb.cpp
endif
