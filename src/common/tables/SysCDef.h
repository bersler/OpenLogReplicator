/* Definition of schema SYS.CDEF$
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

#include "../types.h"
#include "../typeRowId.h"

#ifndef SYS_CDEF_H_
#define SYS_CDEF_H_

#define SYS_CDEF_TYPE_TABLE_CHECK               1
#define SYS_CDEF_TYPE_PK                        2
#define SYS_CDEF_TYPE_UNIQUE                    3
#define SYS_CDEF_TYPE_REFERENTIAL               4
#define SYS_CDEF_TYPE_CHECK                     5
#define SYS_CDEF_TYPE_READ_ONLY                 6
#define SYS_CDEF_TYPE_CHECK_CONSTR_NOT_NULL     7
#define SYS_CDEF_TYPE_HASH                      8
#define SYS_CDEF_TYPE_SCOPED_REF                9
#define SYS_CDEF_TYPE_ROWID                     10
#define SYS_CDEF_TYPE_REF_NOT_NULL              11
#define SYS_CDEF_TYPE_SUPPLEMENTAL_LOG          12
#define SYS_CDEF_TYPE_SUPPLEMENTAL_LOG_PK       14
#define SYS_CDEF_TYPE_SUPPLEMENTAL_LOG_UNIQUE   15
#define SYS_CDEF_TYPE_SUPPLEMENTAL_LOG_FK       16
#define SYS_CDEF_TYPE_SUPPLEMENTAL_LOG_ALL      17

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
        SysCDef(typeRowId& newRowId, typeCon newCon, typeObj newObj, typeType newType) :
                rowId(newRowId),
                con(newCon),
                obj(newObj),
                type(newType) {
        }

        [[nodiscard]] bool operator!=(const SysCDef& other) const {
            return (other.rowId != rowId) || (other.con != con) || (other.obj != obj) || (other.type != type);
        }

        [[nodiscard]] bool isPK() const {
            return (type == SYS_CDEF_TYPE_PK);
        }

        [[nodiscard]] bool isSupplementalLog() const {
            return (type == SYS_CDEF_TYPE_SUPPLEMENTAL_LOG);
        }

        [[nodiscard]] bool isSupplementalLogPK() const {
            return (type == SYS_CDEF_TYPE_SUPPLEMENTAL_LOG_PK);
        }

        [[nodiscard]] bool isSupplementalLogAll() const {
            return (type == SYS_CDEF_TYPE_SUPPLEMENTAL_LOG_ALL);
        }

        typeRowId rowId;
        typeCon con;
        typeObj obj;
        typeType type;
    };
}

#endif
