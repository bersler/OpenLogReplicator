/* Header for OracleColumn class
   Copyright (C) 2018-2019 Adam Leszczynski.

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

#include <string>
#include "types.h"

#ifndef ORACLECOLUMN_H_
#define ORACLECOLUMN_H_

using namespace std;

namespace OpenLogReplicatorOracle {

    class OracleColumn {
    public:
        uint32_t colNo;
        uint32_t segColNo;
        string columnName;
        uint32_t typeNo;
        uint32_t length;
        uint32_t numPk;

        OracleColumn(uint32_t colNo, uint32_t segSolNo, string columnName, uint32_t typeNo, uint32_t length, uint32_t numPk);
        virtual ~OracleColumn();

        friend ostream& operator<<(ostream& os, const OracleColumn& ors);
    };
}

#endif
