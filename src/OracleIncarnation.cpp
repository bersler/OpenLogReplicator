/* Information about incarnation in Oracle database
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

#include "OracleIncarnation.h"

namespace OpenLogReplicator {
    OracleIncarnation::OracleIncarnation(uint32_t incarnation, typeSCN resetlogsScn, typeSCN priorResetlogsScn, const char* status,
            typeRESETLOGS resetlogs, uint32_t priorIncarnation) :
        incarnation(incarnation),
        resetlogsScn(resetlogsScn),
        priorResetlogsScn(priorResetlogsScn),
        status(status),
        resetlogs(resetlogs),
        priorIncarnation(priorIncarnation) {

        if (this->status.compare("CURRENT") == 0)
            current = true;
        else
            current = false;
    }

    OracleIncarnation::~OracleIncarnation() {
    }

    ostream& operator<<(ostream& os, const OracleIncarnation& i) {
        os << "(" << dec << i.incarnation << ", " << i.resetlogsScn << ", " << i.priorResetlogsScn  << ", " << i.status << ", " <<
                i.resetlogs << ", " << i.priorIncarnation << ")";
        return os;
    }
}
