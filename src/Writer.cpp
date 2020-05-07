/* Base class for target writer
   Copyright (C) 2018-2020 Adam Leszczynski.

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
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <iostream>
#include "Writer.h"

#include "CommandBuffer.h"
#include "OracleReader.h"
#include "OracleObject.h"
#include "OracleColumn.h"
#include "RedoLogRecord.h"
#include "RedoLogException.h"

using namespace std;

namespace OpenLogReplicator {

    Writer::Writer(const string alias, OracleReader *oracleReader, uint64_t stream, uint64_t sortColumns, uint64_t metadata, uint64_t singleDml,
            uint64_t nullColumns, uint64_t test, uint64_t timestampFormat) :
        Thread(alias, oracleReader->commandBuffer),
        oracleReader(oracleReader),
        stream(stream),
        sortColumns(sortColumns),
        metadata(metadata),
        singleDml(singleDml),
        nullColumns(nullColumns),
        test(test),
        timestampFormat(timestampFormat) {
    }

    Writer::~Writer() {
    }
}
