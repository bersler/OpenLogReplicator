/* Table from Oracle Database
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

#include "ConfigurationException.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RuntimeException.h"

using namespace std;

namespace OpenLogReplicator {

    OracleObject::OracleObject(typeobj objn, typeobj objd, uint64_t cluCols, uint64_t options, const string owner,
            const string objectName) :
        objn(objn),
        objd(objd),
        cluCols(cluCols),
        totalPk(0),
        options(options),
        maxSegCol(0),
        owner(owner),
        objectName(objectName) {
    }

    OracleObject::~OracleObject() {
        for (OracleColumn *column: columns) {
            delete column;
        }
        columns.clear();
        partitions.clear();
    }

    void OracleObject::addColumn(OracleColumn *column) {
        if (column->segColNo < columns.size() + 1) {
            CONFIG_FAIL("trying to insert table: " << owner << "." << objectName << " (OBJN: " << dec << objn << ", OBJD: " << dec << objd <<
                ") column: " << column->columnName << " (COL#: " << dec << column->colNo << ", SEGCOL#: " << dec << column->segColNo <<
                ") on position " << (columns.size() + 1));
        }

        if (column->segColNo > 1000) {
            CONFIG_FAIL("invalid segColNo value (" << dec << column->segColNo << "), metadata error");
        }

        while (column->segColNo > columns.size() + 1)
            columns.push_back(nullptr);

        columns.push_back(column);
    }

    void OracleObject::addPartition(typeobj partitionObjn, typeobj partitionObjd) {
        typeobj2 objx = (((typeobj2)partitionObjn)<<32) | ((typeobj2)partitionObjd);
        partitions.push_back(objx);
    }

    ostream& operator<<(ostream& os, const OracleObject& object) {
        os << "(\"" << object.owner << "\".\"" << object.objectName << "\", " << dec << object.objn << ", " <<
                object.objd << ", " << object.cluCols << ", " << object.maxSegCol << ")" << endl;
        for (OracleColumn *column : object.columns)
            os << "     - " << *column << endl;
        return os;
    }
}
