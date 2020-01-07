/* Header for OracleObject class
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

#include <string>
#include <vector>
#include "types.h"

#ifndef ORACLEOBJECT_H_
#define ORACLEOBJECT_H_

using namespace std;

namespace OpenLogReplicator {

    class OracleColumn;

    class OracleObject {
    public:
        uint32_t objn;
        uint32_t objd;
        uint32_t cluCols;
        uint32_t totalPk;
        uint32_t options;
        string owner;
        string objectName;
        vector<OracleColumn*> columns;

        void addColumn(OracleColumn *column);

        OracleObject(uint32_t objn, uint32_t objd, uint32_t cluCols, uint32_t options, string owner, string objectName);
        virtual ~OracleObject();

        friend ostream& operator<<(ostream& os, const OracleObject& ors);
    };
}

#endif
