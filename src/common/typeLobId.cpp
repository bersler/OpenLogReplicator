/* Definition of type typeLobId
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

OpenLogReplicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

OpenLogReplicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with OpenLogReplicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <cstring>
#include <iostream>
#include <string>

#include "types.h"
#include "typeLobId.h"

namespace OpenLogReplicator {
    typeLobId::typeLobId() {
        memset((void*)data, 0, TYPE_LOBID_LENGTH);
    }

    typeLobId::typeLobId(const typeLobId& other) {
        memcpy((void*)data, (void*)other.data, TYPE_LOBID_LENGTH);
    }


    bool typeLobId::operator!=(const typeLobId& other) const {
        int ret = memcmp((void*)data, (void*)other.data, TYPE_LOBID_LENGTH);
        return ret != 0;
    }

    bool typeLobId::operator==(const typeLobId& other) const {
        int ret = memcmp((void*)data, (void*)other.data, TYPE_LOBID_LENGTH);
        return ret == 0;
    }

    bool typeLobId::operator<(const typeLobId& other) const {
        int ret = memcmp((void*)data, (void*)other.data, TYPE_LOBID_LENGTH);
        return ret < 0;
    }

    typeLobId& typeLobId::operator=(const typeLobId& other) {
        if (&other != this)
            memcpy((void*)data, (void*)other.data, TYPE_LOBID_LENGTH);
        return *this;
    }

    void typeLobId::set(const uint8_t* newData) {
        memcpy((void*)data, (void*)newData, TYPE_LOBID_LENGTH);
    }

    std::string typeLobId::lower() {
        std::stringstream  os;
        os << std::setfill('0') << std::hex << std::setw(2) << (uint64_t)data[0] << std::setw(2) << (uint64_t)data[1] <<
                std::setw(2) << (uint64_t)data[2] << std::setw(2) << (uint64_t)data[3] <<
                std::setw(2) << (uint64_t)data[4] << std::setw(2) << (uint64_t)data[5] <<
                std::setw(2) << (uint64_t)data[6] << std::setw(2) << (uint64_t)data[7] <<
                std::setw(2) << (uint64_t)data[8] << std::setw(2) << (uint64_t)data[9];
        return os.str();
    }

    std::string typeLobId::upper() {
        std::stringstream  os;
        os << std::uppercase << std::setfill('0') << std::hex << std::setw(2) << (uint64_t)data[0] << std::setw(2) << (uint64_t)data[1] <<
                std::setw(2) << (uint64_t)data[2] << std::setw(2) << (uint64_t)data[3] <<
                std::setw(2) << (uint64_t)data[4] << std::setw(2) << (uint64_t)data[5] <<
                std::setw(2) << (uint64_t)data[6] << std::setw(2) << (uint64_t)data[7] <<
                std::setw(2) << (uint64_t)data[8] << std::setw(2) << (uint64_t)data[9] << std::nouppercase;
        return os.str();
    }

    std::string typeLobId::narrow() {
        std::stringstream  os;
        os << std::uppercase << std::setfill('0') << std::hex << (uint64_t)data[0] << (uint64_t)data[1] <<
                (uint64_t)data[2] << (uint64_t)data[3] <<
                (uint64_t)data[4] << (uint64_t)data[5] <<
                (uint64_t)data[6] << (uint64_t)data[7] <<
                (uint64_t)data[8] << (uint64_t)data[9] << std::nouppercase;
        return os.str();
    }

    std::ostream& operator<<(std::ostream& os, const typeLobId& other) {
        os << std::uppercase << std::setfill('0') << std::hex << std::setw(2) << (uint64_t)other.data[0] << std::setw(2) << (uint64_t)other.data[1] <<
                std::setw(2) << (uint64_t)other.data[2] << std::setw(2) << (uint64_t)other.data[3] <<
                std::setw(2) << (uint64_t)other.data[4] << std::setw(2) << (uint64_t)other.data[5] <<
                std::setw(2) << (uint64_t)other.data[6] << std::setw(2) << (uint64_t)other.data[7] <<
                std::setw(2) << (uint64_t)other.data[8] << std::setw(2) << (uint64_t)other.data[9] << std::nouppercase;
        return os;
    }
}

namespace std {
    size_t std::hash<OpenLogReplicator::typeLobId>::operator()(const OpenLogReplicator::typeLobId& lobId) const {
        return (((size_t)lobId.data[0]) << 56) ^ (((size_t)lobId.data[0]) << 48) ^ (((size_t)lobId.data[0]) << 40) ^ (((size_t)lobId.data[0]) << 32) ^
             (((size_t)lobId.data[0]) << 24) ^ (((size_t)lobId.data[0]) << 16) ^ (((size_t)lobId.data[0]) << 8) ^ ((size_t)lobId.data[0]) ^
             (((size_t)lobId.data[0]) << 8) ^ ((size_t)lobId.data[0]);
    }
}
