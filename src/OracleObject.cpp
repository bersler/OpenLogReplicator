/* Table from Oracle Database
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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
    OracleObject::OracleObject(typeOBJ obj, typeDATAOBJ dataObj, typeUSER user, typeCOL cluCols, typeOPTIONS options, string& owner, string& name) :
        obj(obj),
        dataObj(dataObj),
        user(user),
        cluCols(cluCols),
        totalPk(0),
        options(options),
        maxSegCol(0),
        guardSegNo(-1),
        owner(owner),
        name(name) {

        systemTable = 0;
        if (this->owner.compare("SYS") == 0) {
            sys = true;
            if (this->name.compare("CCOL$") == 0)
                systemTable = TABLE_SYS_CCOL;
            else if (this->name.compare("CDEF$") == 0)
                systemTable = TABLE_SYS_CDEF;
            else if (this->name.compare("COL$") == 0)
                systemTable = TABLE_SYS_COL;
            else if (this->name.compare("DEFERRED_STG$") == 0)
                systemTable = TABLE_SYS_DEFERRED_STG;
            else if (this->name.compare("ECOL$") == 0)
                systemTable = TABLE_SYS_ECOL;
            else if (this->name.compare("OBJ$") == 0)
                systemTable = TABLE_SYS_OBJ;
            else if (this->name.compare("SEG$") == 0)
                systemTable = TABLE_SYS_SEG;
            else if (this->name.compare("TAB$") == 0)
                systemTable = TABLE_SYS_TAB;
            else if (this->name.compare("TABPART$") == 0)
                systemTable = TABLE_SYS_TABPART;
            else if (this->name.compare("TABCOMPART$") == 0)
                systemTable = TABLE_SYS_TABCOMPART;
            else if (this->name.compare("TABSUBPART$") == 0)
                systemTable = TABLE_SYS_TABSUBPART;
            else if (this->name.compare("USER$") == 0)
                systemTable = TABLE_SYS_USER;
        } else
            sys = false;
    }

    OracleObject::~OracleObject() {
        for (OracleColumn* column: columns)
            delete column;
        pk.clear();
        columns.clear();
        partitions.clear();
    }

    void OracleObject::addColumn(OracleColumn* column) {
        if (column->segColNo != columns.size() + 1) {
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
        for (OracleColumn* column : object.columns)
            os << "     - " << *column << endl;
        return os;
    }
}
