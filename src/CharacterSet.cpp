/* Class to handle different character sets
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <iostream>

#include "CharacterSet.h"

using namespace std;

namespace OpenLogReplicator {

    CharacterSet::CharacterSet(const char *name) :
        name(name) {
    }

    CharacterSet::~CharacterSet() {
    }

    uint64_t CharacterSet::badChar(uint64_t byte1) {
        cerr << "WARNING: can't decode character: 0x" << setfill('0') << setw(2) << hex << byte1 << " in character set " << name << endl;
        return UNICODE_UNKNOWN_CHARACTER;
    }

    uint64_t CharacterSet::badChar(uint64_t byte1, uint64_t byte2) {
        cerr << "WARNING: can't decode character: 0x" << setfill('0') << setw(2) << hex << byte1 <<
                ",0x" << setfill('0') << setw(2) << hex << byte2 << " in character set " << name << endl;
        return UNICODE_UNKNOWN_CHARACTER;
    }

    uint64_t CharacterSet::badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3) {
        cerr << "WARNING: can't decode character: 0x" << setfill('0') << setw(2) << hex << byte1 <<
                ",0x" << setfill('0') << setw(2) << hex << byte2 <<
                ",0x" << setfill('0') << setw(2) << hex << byte3 << " in character set " << name << endl;
        return UNICODE_UNKNOWN_CHARACTER;
    }

    uint64_t CharacterSet::badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4) {
        cerr << "WARNING: can't decode character: 0x" << setfill('0') << setw(2) << hex << byte1 <<
                ",0x" << setfill('0') << setw(2) << hex << byte2 <<
                ",0x" << setfill('0') << setw(2) << hex << byte3 <<
                ",0x" << setfill('0') << setw(2) << hex << byte4 << " in character set " << name << endl;
        return UNICODE_UNKNOWN_CHARACTER;
    }

    uint64_t CharacterSet::badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5) {
        cerr << "WARNING: can't decode character: 0x" << setfill('0') << setw(2) << hex << byte1 <<
                ",0x" << setfill('0') << setw(2) << hex << byte2 <<
                ",0x" << setfill('0') << setw(2) << hex << byte3 <<
                ",0x" << setfill('0') << setw(2) << hex << byte4 <<
                ",0x" << setfill('0') << setw(2) << hex << byte5 << " in character set " << name << endl;
        return UNICODE_UNKNOWN_CHARACTER;
    }

    uint64_t CharacterSet::badChar(uint64_t byte1, uint64_t byte2, uint64_t byte3, uint64_t byte4, uint64_t byte5, uint64_t byte6) {
        cerr << "WARNING: can't decode character: 0x" << setfill('0') << setw(2) << hex << byte1 <<
                ",0x" << setfill('0') << setw(2) << hex << byte2 <<
                ",0x" << setfill('0') << setw(2) << hex << byte3 <<
                ",0x" << setfill('0') << setw(2) << hex << byte4 <<
                ",0x" << setfill('0') << setw(2) << hex << byte5 <<
                ",0x" << setfill('0') << setw(2) << hex << byte6 << " in character set " << name << endl;
        return UNICODE_UNKNOWN_CHARACTER;
    }
}
