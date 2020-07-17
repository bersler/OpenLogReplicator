/* Header for OracleColumn class
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

#include <string>

#include "types.h"

#ifndef ORACLECOLUMN_H_
#define ORACLECOLUMN_H_

using namespace std;

namespace OpenLogReplicator {

    class OracleColumn {
    public:
        uint64_t colNo;
        uint64_t segColNo;
        string columnName;
        uint64_t typeNo;
        uint64_t length;
        int64_t precision;
        int64_t scale;
        uint64_t numPk;
        uint64_t charsetId;
        bool nullable;

        OracleColumn(uint64_t colNo, uint64_t segColNo, const string columnName, uint64_t typeNo, uint64_t length, int64_t precision,
                int64_t scale, uint64_t numPk, uint64_t charsetId, bool nullable);
        virtual ~OracleColumn();

        friend ostream& operator<<(ostream& os, const OracleColumn& ors);
    };
}

#endif
