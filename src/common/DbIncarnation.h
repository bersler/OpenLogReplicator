/* Header for DbIncarnation class
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

#ifndef DB_INCARNATION_H_
#define DB_INCARNATION_H_

#include "types/Scn.h"
#include "types/Types.h"

namespace OpenLogReplicator {
    class DbIncarnation {
    public:
        uint32_t incarnation;
        Scn resetlogsScn;
        Scn priorResetlogsScn;
        std::string status;
        typeResetlogs resetlogs;
        uint32_t priorIncarnation;

        bool current;

        DbIncarnation(uint32_t newIncarnation, Scn newResetlogsScn, Scn newPriorResetlogsScn, std::string newStatus, typeResetlogs newResetlogs,
                      uint32_t newPriorIncarnation):
                incarnation(newIncarnation),
                resetlogsScn(newResetlogsScn),
                priorResetlogsScn(newPriorResetlogsScn),
                status(std::move(newStatus)),
                resetlogs(newResetlogs),
                priorIncarnation(newPriorIncarnation) {
            current = this->status == "CURRENT";
        }

        friend std::ostream& operator<<(std::ostream& os, const DbIncarnation& i) {
            os << "(" << std::dec << i.incarnation << ", " << i.resetlogsScn.toString() << ", " << i.priorResetlogsScn.toString() << ", " << i.status <<
                    ", " << i.resetlogs << ", " << i.priorIncarnation << ")";
            return os;
        }
    };
}

#endif
