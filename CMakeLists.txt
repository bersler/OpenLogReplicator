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

cmake_minimum_required(VERSION 3.16)
project(OpenLogReplicator
        VERSION 1.4.1
        DESCRIPTION "OpenLogReplicator, Open Source Oracle database CDC"
        HOMEPAGE_URL "https://github.com/bersler/OpenLogReplicator"
)

set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS FALSE)
if (OSX_ARCH)
    set(CMAKE_OSX_ARCHITECTURES ${OSX_ARCH})
endif ()

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -O3 -Wall -Wextra -Wshadow -Wnon-virtual-dtor -pedantic -Wold-style-cast -Wcast-align -Wunused -Woverloaded-virtual -Wpedantic -Wmisleading-indentation  -Wnull-dereference -Wdouble-promotion -Wimplicit-fallthrough")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall -Wextra -Wshadow -Wnon-virtual-dtor -pedantic -Wold-style-cast -Wcast-align -Wunused -Woverloaded-virtual -Wpedantic -Wmisleading-indentation -Wnull-dereference -Wdouble-promotion -Wimplicit-fallthrough -pedantic -fsanitize=address")
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
configure_file(config.h.in ../config.h)

find_package(Threads REQUIRED)

# RapidJSON
if (WITH_RAPIDJSON)
    include_directories(${WITH_RAPIDJSON}/include)
else ()
    message(FATAL_ERROR "plase provide WITH_RAPIDJSON parameter with path to rapidjson library")
endif ()

# Oracle, only dynamic
if (WITH_OCI)
    include_directories(${WITH_OCI}/sdk/include)
    link_directories(${WITH_OCI})
    add_compile_definitions(LINK_LIBRARY_OCI)

    add_library(Oci::ClntShCore SHARED IMPORTED)
    set_target_properties(Oci::ClntShCore PROPERTIES IMPORTED_LOCATION ${WITH_OCI}/libclntshcore.so.19.1)

    add_library(Oci::Nnz19 SHARED IMPORTED)
    set_target_properties(Oci::Nnz19 PROPERTIES IMPORTED_LOCATION ${WITH_OCI}/libnnz19.so)

    add_library(Oci::ClntSh SHARED IMPORTED)
    set_target_properties(Oci::ClntSh PROPERTIES IMPORTED_LOCATION ${WITH_OCI}/libclntsh.so.19.1)

    add_library(Oci INTERFACE IMPORTED)
    set_property(TARGET Oci PROPERTY
            INTERFACE_LINK_LIBRARIES Oci::ClntShCore Oci::Nnz19 Oci::ClntSh)
endif ()

if (WITH_STATIC)
    add_compile_definitions(LINK_STATIC)
endif ()

# Protobuf, dynamic or static
if (WITH_PROTOBUF)
    include_directories(${WITH_PROTOBUF}/include)
    add_compile_definitions(LINK_LIBRARY_PROTOBUF)

    if (WITH_STATIC)
        add_library(Protobuf STATIC IMPORTED)
        set_target_properties(Protobuf PROPERTIES IMPORTED_LOCATION ${WITH_PROTOBUF}/lib/libprotobuf.a)
    else ()
        add_library(Protobuf SHARED IMPORTED)
        set_target_properties(Protobuf PROPERTIES IMPORTED_LOCATION ${WITH_PROTOBUF}/lib/libprotobuf.so)
    endif ()

    # ZeroMQ, just dynamic
    if (WITH_ZEROMQ)
        include_directories(${WITH_ZEROMQ}/include)
        add_compile_definitions(LINK_LIBRARY_ZEROMQ)

        add_library(Zmq SHARED IMPORTED)
        set_target_properties(Zmq PROPERTIES IMPORTED_LOCATION ${WITH_ZEROMQ}/lib/x86_64-linux-gnu/libzmq.so)
    endif ()
endif ()

# Kafka, dynamic or static
if (WITH_RDKAFKA)
    find_package(OpenSSL REQUIRED)

    include_directories(${WITH_RDKAFKA}/include)
    add_compile_definitions(LINK_LIBRARY_RDKAFKA)

    if (WITH_STATIC)
        add_library(RdKafka::LibRdKafka STATIC IMPORTED)
        set_target_properties(RdKafka::LibRdKafka PROPERTIES IMPORTED_LOCATION ${WITH_RDKAFKA}/lib/librdkafka.a)
        add_library(RdKafka::LibRdKafka++ STATIC IMPORTED)
        set_target_properties(RdKafka::LibRdKafka++ PROPERTIES IMPORTED_LOCATION ${WITH_RDKAFKA}/lib/librdkafka++.a)
    else ()
        add_library(RdKafka::LibRdKafka SHARED IMPORTED)
        set_target_properties(RdKafka::LibRdKafka PROPERTIES IMPORTED_LOCATION ${WITH_RDKAFKA}/lib/librdkafka.so)
        add_library(RdKafka::LibRdKafka++ SHARED IMPORTED)
        set_target_properties(RdKafka::LibRdKafka++ PROPERTIES IMPORTED_LOCATION ${WITH_RDKAFKA}/lib/librdkafka++.so)
    endif ()

    add_library(RdKafka INTERFACE IMPORTED)
    set_property(TARGET RdKafka PROPERTY INTERFACE_LINK_LIBRARIES RdKafka::LibRdKafka RdKafka::LibRdKafka++ OpenSSL::Crypto OpenSSL::SSL)
endif ()

add_executable(OpenLogReplicator ${SOURCE_FILES})

if (WITH_PROTOBUF)
    add_executable(StreamClient ${SOURCE_FILES})
endif ()

add_subdirectory(src)
if (WITH_TESTS)
    add_subdirectory(tests)
endif ()

target_link_libraries(OpenLogReplicator Threads::Threads)

if (WITH_OCI)
    target_link_libraries(OpenLogReplicator Oci)
endif ()

if (WITH_RDKAFKA)
    target_link_libraries(OpenLogReplicator RdKafka)
endif ()

if (WITH_PROTOBUF)
    target_link_libraries(OpenLogReplicator Protobuf)
    target_link_libraries(StreamClient Threads::Threads)
    target_link_libraries(StreamClient Protobuf)

    if (WITH_ZEROMQ)
        target_link_libraries(OpenLogReplicator Zmq)
        target_link_libraries(StreamClient Zmq)
    endif ()
endif ()

target_include_directories(OpenLogReplicator PUBLIC "${PROJECT_BINARY_DIR}")
