/* Header for OracleIncarnation class
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

#include "types.h"

#ifndef ORACLEINCARNATION_H_
#define ORACLEINCARNATION_H_

namespace OpenLogReplicator {
    class OracleIncarnation {
    public:
        uint32_t incarnation;
        typeSCN resetlogsScn;
        typeSCN priorResetlogsScn;
        std::string status;
        typeRESETLOGS resetlogs;
        uint32_t priorIncarnation;

        bool current;

        OracleIncarnation(uint32_t incarnation, typeSCN resetlogsScn, typeSCN priorResetlogsScn, const char* status,
                typeRESETLOGS resetlogs, uint32_t priorIncarnation);
        virtual ~OracleIncarnation();

        friend std::ostream& operator<<(std::ostream& os, const OracleIncarnation& i);
    };
}

#endif
