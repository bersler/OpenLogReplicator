/* Header for Database connection class
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

#ifndef DATABASE_CONNECTION_H_
#define DATABASE_CONNECTION_H_

#include <oci.h>

#include "../common/types/Types.h"

namespace OpenLogReplicator {
    class DatabaseEnvironment;

    class DatabaseConnection final {
    public:
        std::string user;
        std::string password;
        std::string connectString;
        bool sysAsm;
        bool connected{false};

        DatabaseEnvironment* env;
        OCIError* errhp{nullptr};
        OCIServer* srvhp{nullptr};
        OCISvcCtx* svchp{nullptr};
        OCISession* authp{nullptr};

        DatabaseConnection(DatabaseEnvironment* newEnv, std::string newUser, std::string newPassword, std::string newConnectString, bool newSysAsm);
        ~DatabaseConnection();

        void connect();
        void disconnect();
    };
}

#endif
