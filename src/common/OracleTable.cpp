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
#include "OracleTable.h"

namespace OpenLogReplicator {
    OracleTable::OracleTable(typeObj newObj, typeDataObj newDataObj, typeUser newUser, typeCol newCluCols, typeOptions newOptions, const std::string& newOwner,
                             const std::string& newName) :
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
            else if (this->name == "LOBCOMPPART$")
                systemTable = TABLE_SYS_LOB_COMP_PART;
            else if (this->name == "LOBFRAG$")
                systemTable = TABLE_SYS_LOB_FRAG;
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
            else if (this->name == "TS$")
                systemTable = TABLE_SYS_TS;
            else if (this->name == "USER$")
                systemTable = TABLE_SYS_USER;
        } else
            sys = false;
    }

    OracleTable::~OracleTable() {
        for (OracleColumn* column: columns)
            delete column;
        pk.clear();
        columns.clear();
        tablePartitions.clear();
        lobPartitions.clear();
        lobIndexes.clear();

        for (OracleLob* lob: lobs)
            delete lob;
        lobs.clear();
    }

    void OracleTable::addColumn(OracleColumn* column) {
        if (column->segCol != static_cast<typeCol>(columns.size() + 1))
            throw DataException("trying to insert table: " + owner + "." + name + " (OBJ: " + std::to_string(obj) + ", DATAOBJ: " +
                    std::to_string(dataObj) + ") column: " + column->name + " (COL#: " + std::to_string(column->col) + ", SEGCOL#: " +
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

    void OracleTable::addLob(OracleLob* lob) {
        ++totalLobs;
        lobs.push_back(lob);
    }

    void OracleTable::addTablePartition(typeObj newObj, typeDataObj newDataObj) {
        typeObj2 objx = (static_cast<typeObj2>(newObj) << 32) | static_cast<typeObj2>(newDataObj);
        tablePartitions.push_back(objx);
    }

    void OracleTable::addLobPartition(typeDataObj newDataObj) {
        lobPartitions.push_back(newDataObj);
    }

    void OracleTable::addLobIndex(typeDataObj newDataObj) {
        lobIndexes.push_back(newDataObj);
    }

    std::ostream& operator<<(std::ostream& os, const OracleTable& table) {
        os << "('" << table.owner << "'.'" << table.name << "', " << std::dec << table.obj << ", " << table.dataObj << ", " << table.cluCols << ", " <<
                table.maxSegCol << ")" << std::endl;
        for (OracleColumn* column : table.columns)
            os << "     - " << *column << std::endl;
        return os;
    }
}
