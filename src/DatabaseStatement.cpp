/* Class to handle SQL statement
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

#include "DatabaseConnection.h"
#include "DatabaseEnvironment.h"
#include "DatabaseStatement.h"

using namespace std;

namespace OpenLogReplicator {
    DatabaseStatement::DatabaseStatement(DatabaseConnection *conn) :
		conn(conn),
		isExecuted(false),
		stmthp(nullptr) {

        conn->env->checkErr(conn->errhp, OCIHandleAlloc(conn->env->envhp, (dvoid **)&stmthp, OCI_HTYPE_STMT, 0, nullptr));
    }

    DatabaseStatement::~DatabaseStatement() {
        unbindAll();

        if (isExecuted) {
            OCIStmtRelease(stmthp, conn->errhp, nullptr, 0, OCI_DEFAULT);
            isExecuted = false;
        }

        if (stmthp != nullptr) {
            OCIHandleFree(stmthp, OCI_HTYPE_STMT);
            stmthp = nullptr;
        }
    }

    void DatabaseStatement::createStatement(const char *sql) {
        unbindAll();

        if (isExecuted) {
            OCIStmtRelease(stmthp, conn->errhp, nullptr, 0, OCI_DEFAULT);
            isExecuted = false;
        }

        conn->env->checkErr(conn->errhp, OCIStmtPrepare2(conn->svchp, &stmthp, conn->errhp, (const OraText*)sql, strlen(sql),
                nullptr, 0, OCI_NTV_SYNTAX, OCI_DEFAULT));
    }

    int64_t DatabaseStatement::executeQuery(void) {
        sword status = OCIStmtExecute(conn->svchp, stmthp, conn->errhp, 1, 0, nullptr, nullptr, OCI_DEFAULT); //COMMIT_ON_SUCCESS
        isExecuted = true;
        if (status == OCI_NO_DATA)
            return 0;

        conn->env->checkErr(conn->errhp, status);
        return 1;
    }

    void DatabaseStatement::unbindAll(void) {
        for (OCIBind *bindp: binds)
            OCIHandleFree((dvoid*)bindp, OCI_HTYPE_BIND);
        binds.clear();

        for (OCIDefine *defp: defines)
            OCIHandleFree((dvoid*)defp, OCI_HTYPE_DEFINE);
        defines.clear();
    }

    int64_t DatabaseStatement::next(void) {
        sword status = OCIStmtFetch2(stmthp, conn->errhp, 1, OCI_FETCH_NEXT, 0, OCI_DEFAULT);
        if (status == OCI_NO_DATA)
            return 0;

        conn->env->checkErr(conn->errhp, status);
        return 1;
    }

    void DatabaseStatement::bindString(uint64_t col, const char *val) {
        OCIBind *bindp = nullptr;
        sword ret = OCIBindByPos(stmthp, &bindp, conn->errhp, col, (void*)val, strlen(val) + 1, SQLT_STR,
                        nullptr, nullptr, nullptr, 0, nullptr, OCI_DEFAULT);
        if (bindp != nullptr)
            binds.push_back(bindp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::bindString(uint64_t col, string &val) {
        OCIBind *bindp = nullptr;
        sword ret = OCIBindByPos(stmthp, &bindp, conn->errhp, col, (void*)val.c_str(), val.length() + 1, SQLT_STR,
                        nullptr, nullptr, nullptr, 0, nullptr, OCI_DEFAULT);
        if (bindp != nullptr)
            binds.push_back(bindp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::bindInt32(uint64_t col, int32_t &val) {
        OCIBind *bindp = nullptr;
        sword ret = OCIBindByPos(stmthp, &bindp, conn->errhp, col, (void*)&val, sizeof(val), SQLT_INT,
                        nullptr, nullptr, nullptr, 0, nullptr, OCI_DEFAULT);
        if (bindp != nullptr)
            binds.push_back(bindp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::bindUInt32(uint64_t col, uint32_t &val) {
        OCIBind *bindp = nullptr;
        sword ret = OCIBindByPos(stmthp, &bindp, conn->errhp, col, (void*)&val, sizeof(val), SQLT_UIN,
                        nullptr, nullptr, nullptr, 0, nullptr, OCI_DEFAULT);
        if (bindp != nullptr)
            binds.push_back(bindp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::bindInt64(uint64_t col, int64_t &val) {
        OCIBind *bindp = nullptr;
        sword ret = OCIBindByPos(stmthp, &bindp, conn->errhp, col, (void*)&val, sizeof(val), SQLT_INT,
                        nullptr, nullptr, nullptr, 0, nullptr, OCI_DEFAULT);
        if (bindp != nullptr)
            binds.push_back(bindp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::bindUInt64(uint64_t col, uint64_t &val) {
        OCIBind *bindp = nullptr;
        sword ret = OCIBindByPos(stmthp, &bindp, conn->errhp, col, (void*)&val, sizeof(val), SQLT_UIN,
                        nullptr, nullptr, nullptr, 0, nullptr, OCI_DEFAULT);
        if (bindp != nullptr)
            binds.push_back(bindp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::bindBinary(uint64_t col, uint8_t *buf, uint64_t size) {
        OCIBind *bindp = nullptr;
        sword ret = OCIBindByPos(stmthp, &bindp, conn->errhp, col, (void*)buf, size, SQLT_BIN,
                        nullptr, nullptr, nullptr, 0, nullptr, OCI_DEFAULT);
        if (bindp != nullptr)
            binds.push_back(bindp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::defineString(uint64_t col, char *val, uint64_t len) {
        OCIDefine* defp = nullptr;
        sword ret = OCIDefineByPos(stmthp, &defp, conn->errhp, col, val, len, SQLT_STR, nullptr, nullptr, nullptr, OCI_DEFAULT);
        if (defp != nullptr)
            defines.push_back(defp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::defineUInt16(uint64_t col, uint16_t &val) {
        OCIDefine* defp = nullptr;
        sword ret = OCIDefineByPos(stmthp, &defp, conn->errhp, col, &val, sizeof(val), SQLT_UIN, nullptr, nullptr, nullptr, OCI_DEFAULT);
        if (defp != nullptr)
            defines.push_back(defp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::defineInt16(uint64_t col, int16_t &val) {
        OCIDefine* defp = nullptr;
        sword ret = OCIDefineByPos(stmthp, &defp, conn->errhp, col, &val, sizeof(val), SQLT_INT, nullptr, nullptr, nullptr, OCI_DEFAULT);
        if (defp != nullptr)
            defines.push_back(defp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::defineUInt32(uint64_t col, uint32_t &val) {
        OCIDefine* defp = nullptr;
        sword ret = OCIDefineByPos(stmthp, &defp, conn->errhp, col, &val, sizeof(val), SQLT_UIN, nullptr, nullptr, nullptr, OCI_DEFAULT);
        if (defp != nullptr)
            defines.push_back(defp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::defineInt32(uint64_t col, int32_t &val) {
        OCIDefine* defp = nullptr;
        sword ret = OCIDefineByPos(stmthp, &defp, conn->errhp, col, &val, sizeof(val), SQLT_INT, nullptr, nullptr, nullptr, OCI_DEFAULT);
        if (defp != nullptr)
            defines.push_back(defp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::defineUInt64(uint64_t col, uint64_t &val) {
        OCIDefine* defp = nullptr;
        sword ret = OCIDefineByPos(stmthp, &defp, conn->errhp, col, &val, sizeof(val), SQLT_UIN, nullptr, nullptr, nullptr, OCI_DEFAULT);
        if (defp != nullptr)
            defines.push_back(defp);
        conn->env->checkErr(conn->errhp, ret);
    }

    void DatabaseStatement::defineInt64(uint64_t col, int64_t &val) {
        OCIDefine* defp = nullptr;
        sword ret = OCIDefineByPos(stmthp, &defp, conn->errhp, col, &val, sizeof(val), SQLT_INT, nullptr, nullptr, nullptr, OCI_DEFAULT);
        if (defp != nullptr)
            defines.push_back(defp);
        conn->env->checkErr(conn->errhp, ret);
    }

    bool DatabaseStatement::isNull(uint64_t col) {
        OCIParam *paramdp;
        conn->env->checkErr(conn->errhp, OCIParamGet(stmthp, OCI_HTYPE_STMT, conn->errhp, (void **)&paramdp, col));
        uint32_t fieldLength = 0;
        conn->env->checkErr(conn->errhp, OCIAttrGet(paramdp, OCI_DTYPE_PARAM, (dvoid *)&fieldLength, nullptr, OCI_ATTR_DATA_SIZE, conn->errhp));
        return (fieldLength == 0);
    }
}
