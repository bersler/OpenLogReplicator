/* Table from Oracle Database
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
#include <string>

#include "ConfigurationException.h"
#include "OracleColumn.h"
#include "OracleObject.h"

using namespace std;

namespace OpenLogReplicator {

    OracleObject::OracleObject(typeobj objn, typeobj objd, uint64_t cluCols, uint64_t options, const string owner,
            const string objectName) :
        objn(objn),
        objd(objd),
        cluCols(cluCols),
        totalPk(0),
        options(options),
        totalCols(0),
        owner(owner),
        objectName(objectName) {
    }

    OracleObject::~OracleObject() {
        for (OracleColumn *column: columns) {
            delete column;
        }
        columns.clear();
    }

    void OracleObject::addColumn(OracleColumn *column) {
        if (column->colNo != columns.size() + 1) {
            cerr << "ERROR: trying to insert column " << column->columnName << "(" << dec << column->colNo << ") on position " << (columns.size() + 1) << endl;
            throw ConfigurationException("metadata error");
        }
        columns.push_back(column);
    }

    ostream& operator<<(ostream& os, const OracleObject& object) {
        os << "(\"" << object.owner << "\".\"" << object.objectName << "\", " << dec << object.objn << ", " <<
                object.objd << ", " << object.cluCols << ", " << object.totalCols << ")" << endl;
        for (OracleColumn *column : object.columns)
            os << "     - " << *column << endl;
        return os;
    }
}
