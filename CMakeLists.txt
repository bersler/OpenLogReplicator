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
        VERSION 1.6.1
        DESCRIPTION "OpenLogReplicator, Open Source Oracle database CDC"
        HOMEPAGE_URL "https://github.com/bersler/OpenLogReplicator"
)

set(SCHEMA_VERSION "1.6.1")
string(TIMESTAMP CMAKE_BUILD_TIMESTAMP "%Y-%m-%d %H:%M" UTC)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS FALSE)
if (OSX_ARCH)
    set(CMAKE_OSX_ARCHITECTURES ${OSX_ARCH})
endif ()
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(CMAKE_MACOSX_RPATH 1)
    set(MACOSX TRUE)
endif()

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -O3 -Wall -Wextra -Wshadow -Wnon-virtual-dtor -pedantic -Wold-style-cast -Wcast-align -Wunused -Woverloaded-virtual -Wpedantic -Wmisleading-indentation  -Wnull-dereference -Wdouble-promotion -Wimplicit-fallthrough")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall -Wextra -Wshadow -Wnon-virtual-dtor -pedantic -Wold-style-cast -Wcast-align -Wunused -Woverloaded-virtual -Wpedantic -Wmisleading-indentation -Wnull-dereference -Wdouble-promotion -Wimplicit-fallthrough -pedantic -fsanitize=address")
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
configure_file(config.h.in ../config.h)

if (CPU_ARCH)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=${CPU_ARCH} -mtune=${CPU_ARCH}")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -march=${CPU_ARCH} -mtune=${CPU_ARCH}")
    add_compile_definitions(CPU_ARCH=${CPU_ARCH})
endif()

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
endif ()

if (WITH_STATIC)
    add_compile_definitions(LINK_STATIC)
endif ()

# Protobuf, dynamic or static
if (WITH_PROTOBUF)
    include_directories(${WITH_PROTOBUF}/include)
    add_compile_definitions(LINK_LIBRARY_PROTOBUF)

    if (WITH_STATIC)
        add_library(static_protobuf STATIC IMPORTED)
        set_target_properties(static_protobuf PROPERTIES IMPORTED_LOCATION ${WITH_PROTOBUF}/lib/libprotobuf.a)
    else ()
        link_directories(${WITH_PROTOBUF}/lib)
    endif ()

    # ZeroMQ, just dynamic
    if (WITH_ZEROMQ)
        include_directories(${WITH_ZEROMQ}/include)
        add_compile_definitions(LINK_LIBRARY_ZEROMQ)
        if (MACOSX)
            link_directories(${WITH_ZEROMQ}/lib)
        else ()
            link_directories(${WITH_ZEROMQ}/lib/x86_64-linux-gnu)
        endif ()
    endif ()
endif ()

# Kafka, dynamic or static
if (WITH_RDKAFKA)
    include_directories(${WITH_RDKAFKA}/include)
    add_compile_definitions(LINK_LIBRARY_RDKAFKA)

    if (WITH_STATIC)
        find_package(OpenSSL REQUIRED)
        add_library(static_rdkafka1 STATIC IMPORTED)
        add_library(static_rdkafka2 STATIC IMPORTED)
        set_target_properties(static_rdkafka1 PROPERTIES IMPORTED_LOCATION ${WITH_RDKAFKA}/lib/librdkafka.a)
        set_target_properties(static_rdkafka2 PROPERTIES IMPORTED_LOCATION ${WITH_RDKAFKA}/lib/librdkafka++.a)
        add_library(static_rdkafka INTERFACE IMPORTED)
        set_property(TARGET static_rdkafka PROPERTY INTERFACE_LINK_LIBRARIES static_rdkafka1 static_rdkafka2 OpenSSL::Crypto OpenSSL::SSL)
    else ()
        link_directories(${WITH_RDKAFKA}/lib)
    endif ()
endif ()

# Prometheus, only dynamic
if (WITH_PROMETHEUS)
    include_directories(${WITH_PROMETHEUS}/include)
    link_directories(${WITH_PROMETHEUS}/lib)
    add_compile_definitions(LINK_LIBRARY_PROMETHEUS)
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
    target_link_libraries(OpenLogReplicator clntshcore nnz19 clntsh)
endif ()

if (WITH_RDKAFKA)
    if (WITH_STATIC)
        target_link_libraries(OpenLogReplicator static_rdkafka)
    else ()
        target_link_libraries(OpenLogReplicator rdkafka++ rdkafka)
    endif ()
endif ()

if (WITH_PROMETHEUS)
    target_link_libraries(OpenLogReplicator prometheus-cpp-core prometheus-cpp-pull)
endif ()

if (WITH_PROTOBUF)
    if (WITH_STATIC)
        target_link_libraries(OpenLogReplicator static_protobuf)
        target_link_libraries(StreamClient static_protobuf)
    else ()
        target_link_libraries(OpenLogReplicator protobuf)
        target_link_libraries(StreamClient protobuf)
    endif ()
    target_link_libraries(StreamClient Threads::Threads)

    if (WITH_PROMETHEUS)
        target_link_libraries(StreamClient prometheus-cpp-core prometheus-cpp-pull)
    endif ()

    if (WITH_ZEROMQ)
        target_link_libraries(OpenLogReplicator zmq)
        target_link_libraries(StreamClient zmq)
    endif ()
endif ()

target_include_directories(OpenLogReplicator PUBLIC "${PROJECT_BINARY_DIR}")
