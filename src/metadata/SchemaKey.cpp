/* Columns for the key of a database table
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../common/table/SysCol.h"
#include "SchemaKey.h"

namespace OpenLogReplicator {
    SchemaKey::SchemaKey(const char* newOwner, const char* newTable, const char* newColumns) :
            owner(newOwner), table(newTable) {
        std::string columns(newColumns);

        if (columns == "[pk]") {
            this->type = TYPE::PK;
            return;
        }

        if (columns == "[ui]") {
            this->type = TYPE::UI;
            return;
        }

        if (columns == "[all]") {
            this->type = TYPE::ALL;
            return;
        }

        this->type = TYPE::LIST;
        std::stringstream columnsStream(columns);
        while (columnsStream.good()) {
            std::string keyCol;
            getline(columnsStream, keyCol, ',');
            keyCol.erase(remove(keyCol.begin(), keyCol.end(), ' '), keyCol.end());
            transform(keyCol.begin(), keyCol.end(), keyCol.begin(), ::toupper);
            this->columnList.push_back(keyCol);
        }
    }
}
