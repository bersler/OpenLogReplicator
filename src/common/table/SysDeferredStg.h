/* Definition of schema SYS.DEFERRED_STG$
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
#include "../typeIntX.h"
#include "../typeRowId.h"

#ifndef SYS_DEFERRED_STG_H_
#define SYS_DEFERRED_STG_H_

namespace OpenLogReplicator {
    class SysDeferredStg final {
    public:
        static constexpr uint64_t FLAGSSTG_COMPRESSED{4};

        typeRowId rowId;
        typeObj obj;
        typeIntX flagsStg;          // NULL

        SysDeferredStg(typeRowId newRowId, typeObj newObj, uint64_t newFlagsStg1, uint64_t newFlagsStg2) :
                rowId(newRowId),
                obj(newObj),
                flagsStg(newFlagsStg1, newFlagsStg2) {
        }

        explicit SysDeferredStg(typeRowId newRowId) :
                rowId(newRowId),
                obj(0),
                flagsStg(0, 0) {
        }

        bool operator!=(const SysDeferredStg& other) const {
            return (other.rowId != rowId) || (other.obj != obj) || (other.flagsStg != flagsStg);
        }

        [[nodiscard]] bool isCompressed() {
            return flagsStg.isSet64(FLAGSSTG_COMPRESSED);
        }

        static std::string tableName() {
            return "SYS.DEFERRED_STG$";
        }

        std::string toString() const {
            return "ROWID: " + rowId.toString() + ", OBJ#: " + std::to_string(obj) + ", FLAGS_STG: " + flagsStg.toString();
        }

        static constexpr bool dependentTable() {
            return true;
        }

        static constexpr bool dependentTableLob() {
            return false;
        }

        static constexpr bool dependentTableLobFrag() {
            return false;
        }

        static constexpr bool dependentTablePart() {
            return false;
        }

        typeObj getDependentTable() const {
            return obj;
        }
    };

    class SysDeferredStgObj final {
    public:
        typeObj obj;

        explicit SysDeferredStgObj(typeObj newObj) :
                obj(newObj) {
        }

        explicit SysDeferredStgObj(const SysDeferredStg* sysDeferredStg) :
                obj(sysDeferredStg->obj) {
        }

        bool operator!=(const SysDeferredStgObj other) const {
            return (other.obj != obj);
        }

        bool operator==(const SysDeferredStgObj other) const {
            return (other.obj == obj);
        }
    };
}

namespace std {
    template<>
    struct hash<OpenLogReplicator::SysDeferredStgObj> {
        size_t operator()(const OpenLogReplicator::SysDeferredStgObj sysDeferredStgObj) const {
            return hash<typeObj>()(sysDeferredStgObj.obj);
        }
    };
}

#endif
