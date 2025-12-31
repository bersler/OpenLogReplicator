/* Header for DatabaseStatement class
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

#ifndef DATABASE_STATEMENT_H_
#define DATABASE_STATEMENT_H_

#include <cstring>
#include <oci.h>
#include <vector>

#include "../common/types/Types.h"

namespace OpenLogReplicator {
    class DatabaseConnection;

    class DatabaseStatement final {
    protected:
        DatabaseConnection* conn;
        bool executed{false};
        OCIStmt* stmthp{nullptr};
        std::vector<OCIBind*> binds;
        std::vector<OCIDefine*> defines;

    public:
        explicit DatabaseStatement(DatabaseConnection* newConn);
        ~DatabaseStatement();

        void createStatement(const std::string_view& sql);
        void unbindAll();
        int executeQuery();
        int next() const;

        void bindString(uint col, const std::string& val);
        void bindBinary(uint col, uint8_t* buf, uint64_t size);
        void defineString(uint col, char* val, uint64_t len);
        [[nodiscard]] bool isNull(uint col) const;

        template<typename T>
        void bindInt(uint col, T& val) {
            OCIBind* bindp = nullptr;
            const sword ret = OCIBindByPos(stmthp, &bindp, conn->errhp, col, reinterpret_cast<void*>(&val), sizeof(val),
                                           SQLT_INT, nullptr, nullptr, nullptr, 0, nullptr, OCI_DEFAULT);
            if (bindp != nullptr)
                binds.push_back(bindp);
            conn->env->checkErr(conn->errhp, ret);
        }

        template<typename T>
        void bindUInt(uint col, T& val) {
            OCIBind* bindp = nullptr;
            const sword ret = OCIBindByPos(stmthp, &bindp, conn->errhp, col, reinterpret_cast<void*>(&val), sizeof(val),
                                           SQLT_UIN, nullptr, nullptr, nullptr, 0, nullptr, OCI_DEFAULT);
            if (bindp != nullptr)
                binds.push_back(bindp);
            conn->env->checkErr(conn->errhp, ret);
        }

        template<typename T>
        void defineInt(uint col, T& val) {
            OCIDefine* defp = nullptr;
            const sword ret = OCIDefineByPos(stmthp, &defp, conn->errhp, col, &val, sizeof(val), SQLT_INT, nullptr,
                                             nullptr, nullptr, OCI_DEFAULT);
            if (defp != nullptr)
                defines.push_back(defp);
            conn->env->checkErr(conn->errhp, ret);
        }

        template<typename T>
        void defineUInt(uint col, T& val) {
            OCIDefine* defp = nullptr;
            const sword ret = OCIDefineByPos(stmthp, &defp, conn->errhp, col, &val, sizeof(val), SQLT_UIN, nullptr,
                                             nullptr, nullptr, OCI_DEFAULT);
            if (defp != nullptr)
                defines.push_back(defp);
            conn->env->checkErr(conn->errhp, ret);
        }
    };
}

#endif
