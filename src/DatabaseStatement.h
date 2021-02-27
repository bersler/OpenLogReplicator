/* Header for DatabaseStatement class
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

#include <vector>
#include <oci.h>

#include "types.h"

#ifndef DATABASESTATEMENT_H_
#define DATABASESTATEMENT_H_

using namespace std;

namespace OpenLogReplicator {
    class DatabaseConnection;

    class DatabaseStatement {
    protected:
        DatabaseConnection *conn;
        bool isExecuted;
        OCIStmt *stmthp;
        vector<OCIBind *> binds;
        vector<OCIDefine *> defines;

    public:
        DatabaseStatement(DatabaseConnection *conn);
        virtual ~DatabaseStatement();

        void createStatement(const char *sql);
        void unbindAll(void);
        int64_t executeQuery(void);
        int64_t next(void);

        void bindString(uint64_t col, const char *val);
        void bindString(uint64_t col, string &val);
        void bindInt32(uint64_t col, int32_t &val);
        void bindUInt32(uint64_t col, uint32_t &val);
        void bindInt64(uint64_t col, int64_t &val);
        void bindUInt64(uint64_t col, uint64_t &val);
        void bindBinary(uint64_t col, uint8_t *buf, uint64_t size);
        void defineString(uint64_t col, char *val, uint64_t len);
        void defineUInt16(uint64_t col, uint16_t &val);
        void defineInt16(uint64_t col, int16_t &val);
        void defineUInt32(uint64_t col, uint32_t &val);
        void defineInt32(uint64_t col, int32_t &val);
        void defineUInt64(uint64_t col, uint64_t &val);
        void defineInt64(uint64_t col, int64_t &val);
        bool isNull(uint64_t col);
    };
}

#endif
