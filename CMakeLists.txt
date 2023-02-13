#Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)
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

cmake_minimum_required(VERSION 3.16)
project(OpenLogReplicator VERSION 1.0.1)

set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS FALSE)
if (OSX_ARCH)
    set(CMAKE_OSX_ARCHITECTURES ${OSX_ARCH})
endif()

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -Wextra -Wshadow -pedantic -O3 -g")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra -Wshadow -pedantic -Wno-gnu-statement-expression -pedantic-errors -O0 -g -w -fmessage-length=0 -fno-omit-frame-pointer -fsanitize=address")
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
configure_file(config.h.in ../config.h)

#RapidJSON
if (WITH_RAPIDJSON)
    include_directories(${WITH_RAPIDJSON}/include)
else()
    message(FATAL_ERROR "plase provide WITH_RAPIDJSON parameter with path to rapidjson library" )
endif()

#Oracle
if (WITH_OCI)
    include_directories(${WITH_OCI}/sdk/include)
    link_directories(${WITH_OCI})
    add_compile_definitions(LINK_LIBRARY_OCI)
endif()

#Protobuf
if (WITH_PROTOBUF)
    include_directories(${WITH_PROTOBUF}/include)
    link_directories(${WITH_PROTOBUF}/lib)
    add_compile_definitions(LINK_LIBRARY_PROTOBUF)

    if (WITH_ZEROMQ)
        include_directories(${WITH_ZEROMQ}/include)
        link_directories(${WITH_ZEROMQ}/lib)
        link_directories(${WITH_ZEROMQ}/lib/x86_64-linux-gnu)
        add_compile_definitions(LINK_LIBRARY_ZEROMQ)
    endif()
endif()

#Kafka
if (WITH_RDKAFKA)
    include_directories(${WITH_RDKAFKA}/include)
    link_directories(${WITH_RDKAFKA}/lib)
    add_compile_definitions(LINK_LIBRARY_RDKAFKA)
endif()

add_executable(OpenLogReplicator ${SOURCE_FILES})

if (WITH_OCI)
    target_link_libraries(OpenLogReplicator clntshcore nnz19 clntsh)
endif()

if (WITH_RDKAFKA)
    target_link_libraries(OpenLogReplicator rdkafka++ rdkafka)
endif()

if (WITH_PROTOBUF)
    add_executable(StreamClient ${SOURCE_FILES})
    target_link_libraries(OpenLogReplicator protobuf)
    target_link_libraries(StreamClient protobuf)

    if (WITH_ZEROMQ)
        target_link_libraries(OpenLogReplicator zmq)
        target_link_libraries(StreamClient zmq)
    endif()

    target_link_libraries(StreamClient pthread)
endif()

target_link_libraries(OpenLogReplicator pthread)

add_subdirectory(src)
if (WITH_TESTS)
    add_subdirectory(tests)
endif()

target_include_directories(OpenLogReplicator PUBLIC "${PROJECT_BINARY_DIR}")
