/* Header for OracleStatement class
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <string>

#include "types.h"

#ifdef ONLINE_MODEIMPL_OCCI
#include <occi.h>
#endif /* ONLINE_MODEIMPL_OCCI */

#ifndef ORACLESTATEMENT_H_
#define ORACLESTATEMENT_H_

using namespace std;
#ifdef ONLINE_MODEIMPL_OCCI
using namespace oracle::occi;
#endif /* ONLINE_MODEIMPL_OCCI */

namespace OpenLogReplicator {

    class OracleStatement {
#ifdef ONLINE_MODEIMPL_OCCI
        Connection **conn;
        Environment *env;
#endif /* ONLINE_MODEIMPL_OCCI */
    public:
#ifdef ONLINE_MODEIMPL_OCCI
        Statement *stmt;
        ResultSet *rset;
#endif /* ONLINE_MODEIMPL_OCCI */

        OracleStatement(
#ifdef ONLINE_MODEIMPL_OCCI
            Connection **conn, Environment *env
#endif /* ONLINE_MODEIMPL_OCCI */
        );

        void createStatement(string sql);
        void executeQuery(void);
        virtual ~OracleStatement();
    };
}

#endif
