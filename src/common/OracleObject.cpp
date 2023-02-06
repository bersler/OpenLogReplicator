/* Table from Oracle Database
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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
#include "DataException.h"
#include "OracleColumn.h"
#include "OracleLob.h"
#include "OracleObject.h"

namespace OpenLogReplicator {
    OracleObject::OracleObject(typeObj newObj, typeDataObj newDataObj, typeUser newUser, typeCol newCluCols, typeOptions newOptions,
                               const std::string& newOwner, const std::string& newName) :
        obj(newObj),
        dataObj(newDataObj),
        user(newUser),
        cluCols(newCluCols),
        totalPk(0),
        totalLobs(0),
        options(newOptions),
        maxSegCol(0),
        guardSegNo(-1),
        owner(newOwner),
        name(newName) {

        systemTable = 0;
        if (this->owner == "SYS") {
            sys = true;
            if (this->name == "CCOL$")
                systemTable = TABLE_SYS_CCOL;
            else if (this->name == "CDEF$")
                systemTable = TABLE_SYS_CDEF;
            else if (this->name == "COL$")
                systemTable = TABLE_SYS_COL;
            else if (this->name == "DEFERRED_STG$")
                systemTable = TABLE_SYS_DEFERRED_STG;
            else if (this->name == "ECOL$")
                systemTable = TABLE_SYS_ECOL;
            else if (this->name == "LOB$")
                systemTable = TABLE_SYS_LOB;
            else if (this->name == "OBJ$")
                systemTable = TABLE_SYS_OBJ;
            else if (this->name == "TAB$")
                systemTable = TABLE_SYS_TAB;
            else if (this->name == "TABPART$")
                systemTable = TABLE_SYS_TABPART;
            else if (this->name == "TABCOMPART$")
                systemTable = TABLE_SYS_TABCOMPART;
            else if (this->name == "TABSUBPART$")
                systemTable = TABLE_SYS_TABSUBPART;
            else if (this->name == "USER$")
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

        for (OracleLob* lob: lobs)
            delete lob;
        lobs.clear();
    }

    void OracleObject::addColumn(OracleColumn* column) {
        if (column->segCol != static_cast<typeCol>(columns.size() + 1))
            throw DataException("trying to insert table: " + owner + "." + name + " (obj: " + std::to_string(obj) + ", dataobj: " +
                    std::to_string(dataObj) + ") column: " + column->name + " (col#: " + std::to_string(column->col) + ", segcol#: " +
                    std::to_string(column->segCol) + ") on position " + std::to_string(columns.size() + 1));

        if (column->guard)
            guardSegNo = column->segCol - static_cast<typeCol>(1);

        totalPk += column->numPk;
        if (column->numPk > 0)
            pk.push_back(columns.size());

        if (column->segCol > maxSegCol)
            maxSegCol = column->segCol;

        columns.push_back(column);
    }

    void OracleObject::addLob(OracleLob* lob) {
        ++totalLobs;
        lobs.push_back(lob);
    }

    void OracleObject::addPartition(typeObj partitionObj, typeDataObj partitionDataObj) {
        typeObj2 objx = (static_cast<typeObj2>(partitionObj) << 32) | static_cast<typeObj2>(partitionDataObj);
        partitions.push_back(objx);
    }

    std::ostream& operator<<(std::ostream& os, const OracleObject& object) {
        os << "('" << object.owner << "'.'" << object.name << "', " << std::dec << object.obj << ", " << object.dataObj << ", " << object.cluCols << ", " <<
                object.maxSegCol << ")" << std::endl;
        for (OracleColumn* column : object.columns)
            os << "     - " << *column << std::endl;
        return os;
    }
}
