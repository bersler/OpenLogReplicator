/* Definition of schema SYS.USER$
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

#ifndef SYS_USER_H_
#define SYS_USER_H_

#include "../types/IntX.h"
#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    class SysUser final {
    public:
        static constexpr uint NAME_LENGTH{128};

        enum class SPARE1 : unsigned char {
            SUPP_LOG_PRIMARY = 1UL << 0,
            SUPP_LOG_ALL     = 1UL << 3
        };

        RowId rowId;
        typeUser user{0};
        std::string name;
        IntX spare1{0, 0}; // NULL
        bool single{false};

        SysUser(RowId newRowId, typeUser newUser, std::string newName, uint64_t newSpare11, uint64_t newSpare12, bool newSingle):
                rowId(newRowId),
                user(newUser),
                name(std::move(newName)),
                spare1(newSpare11, newSpare12),
                single(newSingle) {}

        explicit SysUser(RowId newRowId):
                rowId(newRowId) {}

        bool operator!=(const SysUser& other) const {
            return (other.rowId != rowId) || (other.user != user) || (other.name != name) || (other.spare1 != spare1);
        }

        [[nodiscard]] bool isSpare1(SPARE1 val) const {
            return spare1.isSet64(static_cast<uint>(val));
        }

        [[nodiscard]] bool isSuppLogPrimary() const {
            return isSpare1(SPARE1::SUPP_LOG_PRIMARY);
        }

        [[nodiscard]] bool isSuppLogAll() const {
            return isSpare1(SPARE1::SUPP_LOG_ALL);
        }

        [[nodiscard]] static std::string tableName() {
            return "SYS.USER$";
        }

        [[nodiscard]] std::string toString() const {
            return "ROWID: " + rowId.toString() + ", USER#: " + std::to_string(user) + ", NAME: '" + name + "', SPARE1: " + spare1.toString();
        }

        [[nodiscard]] static constexpr bool dependentTable() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTableLob() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTableLobFrag() {
            return false;
        }

        [[nodiscard]] static constexpr bool dependentTablePart() {
            return false;
        }
    };

    class SysUserUser final {
    public:
        typeUser user;

        explicit SysUserUser(typeUser newUser):
                user(newUser) {}

        explicit SysUserUser(const SysUser* sysUser):
                user(sysUser->user) {}

        bool operator!=(const SysUserUser other) const {
            return (other.user != user);
        }

        bool operator==(const SysUserUser other) const {
            return (other.user == user);
        }
    };
}

template<>
struct std::hash<OpenLogReplicator::SysUserUser> {
    size_t operator()(const OpenLogReplicator::SysUserUser sysUserUser) const noexcept {
        return hash<typeUser>()(sysUserUser.user);
    }
};

#endif
