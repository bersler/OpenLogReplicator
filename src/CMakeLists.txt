# Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)
#
# This file is part of OpenLogReplicator.
#
# OpenLogReplicator is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as published
# by the Free Software Foundation; either version 3, or (at your option)
# any later version.
#
# OpenLogReplicator is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
# Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with OpenLogReplicator; see the file LICENSE;  If not see
# <http://www.gnu.org/licenses/>.

list(APPEND ListCommon
        common/ClockHW.cpp
        common/Ctx.cpp
        common/LobCtx.cpp
        common/LobData.cpp
        common/LobKey.cpp
        common/OracleColumn.cpp
        common/OracleIncarnation.cpp
        common/OracleLob.cpp
        common/OracleTable.cpp
        common/RedoLogRecord.cpp
        common/MemoryManager.cpp
        common/Thread.cpp
        common/XmlCtx.cpp
        common/exception/BootException.cpp
        common/exception/ConfigurationException.cpp
        common/exception/DataException.cpp
        common/exception/NetworkException.cpp
        common/exception/RedoLogException.cpp
        common/exception/RuntimeException.cpp
        common/expression/BoolValue.cpp
        common/expression/Expression.cpp
        common/expression/StringValue.cpp
        common/expression/Token.cpp
        common/metrics/Metrics.cpp)

list(APPEND ListReplicator
        replicator/Replicator.cpp
        replicator/ReplicatorBatch.cpp)

list(APPEND ListLocales
        locales/CharacterSet.cpp
        locales/CharacterSet16bit.cpp
        locales/CharacterSet7bit.cpp
        locales/CharacterSet8bit.cpp
        locales/CharacterSetAL16UTF16.cpp
        locales/CharacterSetAL32UTF8.cpp
        locales/CharacterSetJA16EUC.cpp
        locales/CharacterSetJA16EUCTILDE.cpp
        locales/CharacterSetJA16SJIS.cpp
        locales/CharacterSetJA16SJISTILDE.cpp
        locales/CharacterSetKO16KSCCS.cpp
        locales/CharacterSetUTF8.cpp
        locales/CharacterSetZHS16GBK.cpp
        locales/CharacterSetZHS32GB18030.cpp
        locales/CharacterSetZHT16HKSCS31.cpp
        locales/CharacterSetZHT32EUC.cpp
        locales/CharacterSetZHT32TRIS.cpp
        locales/Locales.cpp)

list(APPEND ListBuilder
        builder/Builder.cpp
        builder/BuilderJson.cpp
        builder/SystemTransaction.cpp)

list(APPEND ListParser
        parser/OpCode.cpp
        parser/OpCode0501.cpp
        parser/OpCode0502.cpp
        parser/OpCode0504.cpp
        parser/OpCode0506.cpp
        parser/OpCode050B.cpp
        parser/OpCode0513.cpp
        parser/OpCode0514.cpp
        parser/OpCode0A02.cpp
        parser/OpCode0A08.cpp
        parser/OpCode0A12.cpp
        parser/OpCode0B02.cpp
        parser/OpCode0B03.cpp
        parser/OpCode0B04.cpp
        parser/OpCode0B05.cpp
        parser/OpCode0B06.cpp
        parser/OpCode0B08.cpp
        parser/OpCode0B0B.cpp
        parser/OpCode0B0C.cpp
        parser/OpCode0B10.cpp
        parser/OpCode0B16.cpp
        parser/OpCode1301.cpp
        parser/OpCode1801.cpp
        parser/OpCode1A02.cpp
        parser/OpCode1A06.cpp
        parser/Parser.cpp
        parser/Transaction.cpp
        parser/TransactionBuffer.cpp)

list(APPEND ListReader
        reader/Reader.cpp
        reader/ReaderFilesystem.cpp)

list(APPEND ListMetadata
        metadata/Checkpoint.cpp
        metadata/RedoLog.cpp
        metadata/Metadata.cpp
        metadata/Schema.cpp
        metadata/SchemaElement.cpp
        metadata/Serializer.cpp
        metadata/SerializerJson.cpp)

list(APPEND ListState
        state/State.cpp
        state/StateDisk.cpp)

list(APPEND ListWriter
        writer/Writer.cpp
        writer/WriterDiscard.cpp
        writer/WriterFile.cpp)

if (WITH_OCI)
    list(APPEND ListReplicator
            replicator/DatabaseConnection.cpp
            replicator/DatabaseEnvironment.cpp
            replicator/DatabaseStatement.cpp
            replicator/ReplicatorOnline.cpp)
endif ()

if (WITH_RDKAFKA)
    list(APPEND ListWriter
            writer/WriterKafka.cpp)
endif ()

if (WITH_PROMETHEUS)
    list(APPEND ListCommon
            common/metrics/MetricsPrometheus.cpp)
endif ()

if (WITH_PROTOBUF)
    list(APPEND ListCommon
            common/OraProtoBuf.pb.cpp)
    list(APPEND ListBuilder
            builder/BuilderProtobuf.cpp)
    list(APPEND ListStream
            stream/Stream.cpp
            stream/StreamNetwork.cpp)
    list(APPEND ListWriter
            writer/WriterStream.cpp)

    if (WITH_ZEROMQ)
        list(APPEND ListStream
                stream/StreamZeroMQ.cpp)
    endif ()
endif ()

add_library(LibCommon OBJECT ${ListCommon})
add_library(LibReplicator OBJECT ${ListReplicator})
add_library(LibLocales OBJECT ${ListLocales})
add_library(LibBuilder OBJECT ${ListBuilder})
add_library(LibParser OBJECT ${ListParser})
add_library(LibReader OBJECT ${ListReader})
add_library(LibMetadata OBJECT ${ListMetadata})
add_library(LibState OBJECT ${ListState})
add_library(LibWriter OBJECT ${ListWriter})

target_sources(OpenLogReplicator PUBLIC OpenLogReplicator.cpp main.cpp)
target_link_libraries(OpenLogReplicator LibCommon)
target_link_libraries(OpenLogReplicator LibReplicator)
target_link_libraries(OpenLogReplicator LibLocales)
target_link_libraries(OpenLogReplicator LibBuilder)
target_link_libraries(OpenLogReplicator LibParser)
target_link_libraries(OpenLogReplicator LibReader)
target_link_libraries(OpenLogReplicator LibMetadata)
target_link_libraries(OpenLogReplicator LibState)
target_link_libraries(OpenLogReplicator LibWriter)

if (WITH_PROTOBUF)
    add_library(LibStream ${ListStream})
    target_link_libraries(OpenLogReplicator LibStream)

    target_sources(StreamClient PUBLIC StreamClient.cpp)
    target_link_libraries(StreamClient LibCommon)
    target_link_libraries(StreamClient LibStream)
endif ()
