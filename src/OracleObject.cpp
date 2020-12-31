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

using namespace std;

namespace OpenLogReplicator {

    OracleObject::OracleObject(typeOBJ obj, typeDATAOBJ dataObj, typeCOL cluCols, uint64_t options, const char *owner, const char *name) :
        obj(obj),
        dataObj(dataObj),
        cluCols(cluCols),
        totalPk(0),
        options(options),
        maxSegCol(0),
        guardSegNo(-1),
        owner(owner),
        name(name) {
    }

    OracleObject::~OracleObject() {
        for (OracleColumn *column: columns)
            delete column;
        pk.clear();
        columns.clear();
        partitions.clear();
    }

    void OracleObject::addColumn(OracleColumn *column) {
        if (column->segColNo < columns.size() + 1) {
            CONFIG_FAIL("trying to insert table: " << owner << "." << name << " (OBJ: " << dec << obj << ", DATAOBJ: " << dec << dataObj <<
                ") column: " << column->name << " (COL#: " << dec << column->colNo << ", SEGCOL#: " << dec << column->segColNo <<
                ") on position " << (columns.size() + 1));
        }

        if (column->segColNo > 1000) {
            CONFIG_FAIL("invalid segColNo value (" << dec << column->segColNo << "), metadata error");
        }

        if (column->guard)
            guardSegNo = column->segColNo - 1;

        columns.push_back(column);
    }

    void OracleObject::addPartition(typeOBJ partitionObj, typeDATAOBJ partitionDataObj) {
        typeOBJ2 objx = (((typeOBJ2)partitionObj)<<32) | ((typeOBJ2)partitionDataObj);
        partitions.push_back(objx);
    }

    void OracleObject::updatePK(void) {
        for (typeCOL i = 0; i < maxSegCol; ++i) {
            if (columns[i] == nullptr)
                continue;
            if (columns[i]->numPk > 0)
                pk.push_back(i);
        }
    }

    ostream& operator<<(ostream& os, const OracleObject& object) {
        os << "(\"" << object.owner << "\".\"" << object.name << "\", " << dec << object.obj << ", " <<
                object.dataObj << ", " << object.cluCols << ", " << object.maxSegCol << ")" << endl;
        for (OracleColumn *column : object.columns)
            os << "     - " << *column << endl;
        return os;
    }
}
