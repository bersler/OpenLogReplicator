/* Definition of schema SYS.CDEF$
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../types.h"
#include "../typeRowId.h"

#ifndef SYS_CDEF_H_
#define SYS_CDEF_H_

namespace OpenLogReplicator {
    class SysCDefKey final {
    public:
        SysCDefKey(typeObj newObj, typeCon newCon) :
                obj(newObj),
                con(newCon) {
        }

        bool operator<(const SysCDefKey& other) const {
            if (obj < other.obj)
                return true;
            if (other.obj < obj)
                return false;
            if (con < other.con)
                return true;
            return false;
        }

        typeObj obj;
        typeCon con;
    };

    class SysCDef final {
    public:
        static constexpr uint64_t TYPE_TABLE_CHECK = 1;
        static constexpr uint64_t TYPE_PK = 2;
        static constexpr uint64_t TYPE_UNIQUE = 3;
        static constexpr uint64_t TYPE_REFERENTIAL = 4;
        static constexpr uint64_t TYPE_CHECK = 5;
        static constexpr uint64_t TYPE_READ_ONLY = 6;
        static constexpr uint64_t TYPE_CHECK_CONSTR_NOT_NULL = 7;
        static constexpr uint64_t TYPE_HASH = 8;
        static constexpr uint64_t TYPE_SCOPED_REF = 9;
        static constexpr uint64_t TYPE_ROWID = 10;
        static constexpr uint64_t TYPE_REF_NOT_NULL = 11;
        static constexpr uint64_t TYPE_SUPPLEMENTAL_LOG = 12;
        static constexpr uint64_t TYPE_SUPPLEMENTAL_LOG_PK = 14;
        static constexpr uint64_t TYPE_SUPPLEMENTAL_LOG_UNIQUE = 15;
        static constexpr uint64_t TYPE_SUPPLEMENTAL_LOG_FK = 16;
        static constexpr uint64_t TYPE_SUPPLEMENTAL_LOG_ALL = 17;

        SysCDef(typeRowId newRowId, typeCon newCon, typeObj newObj, typeType newType) :
                rowId(newRowId),
                con(newCon),
                obj(newObj),
                type(newType) {
        }

        [[nodiscard]] bool operator!=(const SysCDef& other) const {
            return (other.rowId != rowId) || (other.con != con) || (other.obj != obj) || (other.type != type);
        }

        [[nodiscard]] bool isPK() const {
            return (type == TYPE_PK);
        }

        [[nodiscard]] bool isSupplementalLog() const {
            return (type == TYPE_SUPPLEMENTAL_LOG);
        }

        [[nodiscard]] bool isSupplementalLogPK() const {
            return (type == TYPE_SUPPLEMENTAL_LOG_PK);
        }

        [[nodiscard]] bool isSupplementalLogAll() const {
            return (type == TYPE_SUPPLEMENTAL_LOG_ALL);
        }

        typeRowId rowId;
        typeCon con;
        typeObj obj;
        typeType type;
    };
}

#endif
