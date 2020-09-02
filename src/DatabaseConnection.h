/* Header for Database connection class
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

#include "types.h"

#ifdef LINK_LIBRARY_OCI
#include <oci.h>
#endif /* LINK_LIBRARY_OCI */

#ifndef DATABASEECONNECTION_H_
#define DATABASEECONNECTION_H_

using namespace std;

namespace OpenLogReplicator {

    class DatabaseEnvironment;

    class DatabaseConnection {
    protected:

    public:
        DatabaseEnvironment *env;
#ifdef LINK_LIBRARY_OCI
        OCIError *errhp;
        OCIServer *srvhp;
        OCISvcCtx *svchp;
        OCISession *authp;
#endif /* LINK_LIBRARY_OCI */

        DatabaseConnection(DatabaseEnvironment *env, string &user, string &password, string &server, bool sysASM);
        virtual ~DatabaseConnection();
    };
}

#endif
