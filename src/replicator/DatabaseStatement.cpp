/* Class to handle SQL statement
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <cstring>

#include "DatabaseConnection.h"
#include "DatabaseEnvironment.h"
#include "DatabaseStatement.h"

namespace OpenLogReplicator {
    DatabaseStatement::DatabaseStatement(DatabaseConnection* newConn):
            conn(newConn) {
        conn->env->checkErr(conn->errhp, OCIHandleAlloc(conn->env->envhp, reinterpret_cast<dvoid**>(&stmthp), OCI_HTYPE_STMT,
                                                        0, nullptr));
    }

    DatabaseStatement::~DatabaseStatement() {
        unbindAll();

        if (executed) {
            OCIStmtRelease(stmthp, conn->errhp, nullptr, 0, OCI_DEFAULT);
            executed = false;
        }

        if (stmthp != nullptr) {
            OCIHandleFree(stmthp, OCI_HTYPE_STMT);
            stmthp = nullptr;
        }
    }

    void DatabaseStatement::createStatement(const std::string_view& sql) {
        unbindAll();

        if (executed) {
            OCIStmtRelease(stmthp, conn->errhp, nullptr, 0, OCI_DEFAULT);
            executed = false;
        }

        conn->env->checkErr(conn->errhp, OCIStmtPrepare2(conn->svchp, &stmthp, conn->errhp, reinterpret_cast<const OraText*>(sql.data()),
                                                         sql.length(), nullptr, 0, OCI_NTV_SYNTAX, OCI_DEFAULT));
    }

    int DatabaseStatement::executeQuery() {
        const sword status = OCIStmtExecute(conn->svchp, stmthp, conn->errhp, 1, 0, nullptr, nullptr,
                                            OCI_DEFAULT); // COMMIT_ON_SUCCESS
        executed = true;
        if (status == OCI_NO_DATA)
            return 0;

        conn->env->checkErr(conn->errhp, status);
        return 1;
    }

    void DatabaseStatement::unbindAll() {
        for (OCIBind* bindp: binds)
            OCIHandleFree(bindp, OCI_HTYPE_BIND);
        binds.clear();

        for (OCIDefine* defp: defines)
            OCIHandleFree(defp, OCI_HTYPE_DEFINE);
        defines.clear();
    }

    int DatabaseStatement::next() const {
        const sword status = OCIStmtFetch2(stmthp, conn->errhp, 1, OCI_FETCH_NEXT, 0, OCI_DEFAULT);
        if (status == OCI_NO_DATA)
            return 0;

        conn->env->checkErr(conn->errhp, status);
        return 1;
    }

    void DatabaseStatement::bindString(uint col, const std::string& val) {
        OCIBind* bindp = nullptr;
        const sword ret = OCIBindByPos(stmthp, &bindp, conn->errhp, col,
                                       const_cast<void*>(reinterpret_cast<const void*>(val.c_str())), val.length() + 1,
                                       SQLT_STR, nullptr, nullptr, nullptr, 0, nullptr, OCI_DEFAULT);
        if (bindp != nullptr)
            binds.push_back(bindp);
        conn->env->checkErr(conn->errhp, ret);
    }


    void DatabaseStatement::bindBinary(uint col, uint8_t* buf, uint64_t size) {
        OCIBind* bindp = nullptr;
        const sword ret = OCIBindByPos(stmthp, &bindp, conn->errhp, col, buf, size, SQLT_BIN,
                                       nullptr, nullptr, nullptr, 0, nullptr, OCI_DEFAULT);
        if (bindp != nullptr)
            binds.push_back(bindp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::defineString(uint col, char* val, uint64_t len) {
        OCIDefine* defp = nullptr;
        const sword ret = OCIDefineByPos(stmthp, &defp, conn->errhp, col, val, len, SQLT_STR, nullptr,
                                         nullptr, nullptr, OCI_DEFAULT);
        if (defp != nullptr)
            defines.push_back(defp);
        conn->env->checkErr(conn->errhp, ret);
    }

    bool DatabaseStatement::isNull(uint col) const {
        OCIParam* paramdp;
        conn->env->checkErr(conn->errhp, OCIParamGet(stmthp, OCI_HTYPE_STMT, conn->errhp,
                                                     reinterpret_cast<void**>(&paramdp), col));
        uint32_t fieldSize = 0;
        conn->env->checkErr(conn->errhp, OCIAttrGet(paramdp, OCI_DTYPE_PARAM, &fieldSize, nullptr,
                                                    OCI_ATTR_DATA_SIZE, conn->errhp));
        return (fieldSize == 0);
    }
}
