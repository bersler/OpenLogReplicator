/* Encapsulation of Oracle Stament/ResultSet classes
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

#include <iostream>
#include "OracleStatement.h"

namespace OpenLogReplicator {

    OracleStatement::OracleStatement(Connection **conn, Environment *env) :
        conn(conn),
        env(env),
        stmt(nullptr),
        rset(nullptr) {
    }

    void OracleStatement::createStatement(string sql) {
        try {
            stmt = (*conn)->createStatement(sql);
        } catch(SQLException &ex) {
            cerr << "ERROR: " << ex.getErrorCode() << ": " << ex.getMessage();

            //close dropped connection
            if (ex.getErrorCode() == 3114) {
                if (conn != nullptr) {
                    env->terminateConnection(*conn);
                    *conn = nullptr;
                }
            }
        }
    }

    void OracleStatement::executeQuery() {
        rset = stmt->executeQuery();
    }

    OracleStatement::~OracleStatement() {
        if (stmt != nullptr) {
            if (rset != nullptr) {
                stmt->closeResultSet(rset);
                rset = nullptr;
            }
            if (*conn != nullptr)
                (*conn)->terminateStatement(stmt);
            stmt = nullptr;
        }
    }
}
