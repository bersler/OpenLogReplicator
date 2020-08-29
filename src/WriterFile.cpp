/* Thread writing to stdout
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <thread>

#include "OutputBuffer.h"
#include "ConfigurationException.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "WriterFile.h"

using namespace std;

namespace OpenLogReplicator {

    WriterFile::WriterFile(const char *alias, OracleAnalyser *oracleAnalyser, uint64_t shortMessage) :
        Writer(alias, oracleAnalyser, shortMessage, 0) {
    }

    WriterFile::~WriterFile() {

    }

    void WriterFile::sendMessage(uint8_t *buffer, uint64_t length, bool dealloc) {
        cout.write((const char*)buffer, length);
        cout << endl;
        if (dealloc)
            free(buffer);
    }

    string WriterFile::getName() {
        return "stdout";
    }
}
