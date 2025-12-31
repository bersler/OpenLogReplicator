/* Class to handle database connection
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

#include "DatabaseConnection.h"
#include "DatabaseEnvironment.h"

namespace OpenLogReplicator {
    DatabaseConnection::DatabaseConnection(DatabaseEnvironment* newEnv, std::string newUser, std::string newPassword, std::string newConnectString,
                                           bool newSysAsm):
            user(std::move(newUser)),
            password(std::move(newPassword)),
            connectString(std::move(newConnectString)),
            sysAsm(newSysAsm),
            env(newEnv) {}

    void DatabaseConnection::connect() {
        disconnect();

        OCIHandleAlloc(env->envhp, reinterpret_cast<dvoid**>(&errhp), OCI_HTYPE_ERROR, 0, nullptr);
        OCIHandleAlloc(env->envhp, reinterpret_cast<dvoid**>(&srvhp), OCI_HTYPE_SERVER, 0, nullptr);
        OCIHandleAlloc(env->envhp, reinterpret_cast<dvoid**>(&svchp), OCI_HTYPE_SVCCTX, 0, nullptr);
        OCIHandleAlloc(env->envhp, reinterpret_cast<dvoid**>(&authp), OCI_HTYPE_SESSION, 0, nullptr);

        env->checkErr(errhp, OCIServerAttach(srvhp, errhp, reinterpret_cast<const OraText*>(connectString.c_str()),
                                             connectString.length(), OCI_DEFAULT));
        env->checkErr(errhp, OCIAttrSet(svchp, OCI_HTYPE_SVCCTX, srvhp, 0,
                                        OCI_ATTR_SERVER, errhp));
        env->checkErr(errhp, OCIAttrSet(authp, OCI_HTYPE_SESSION,
                                        const_cast<dvoid*>(reinterpret_cast<const dvoid*>(user.c_str())), user.length(),
                                        OCI_ATTR_USERNAME, errhp));
        env->checkErr(errhp, OCIAttrSet(authp, OCI_HTYPE_SESSION,
                                        const_cast<dvoid*>(reinterpret_cast<const dvoid*>(password.c_str())), password.length(),
                                        OCI_ATTR_PASSWORD, errhp));

        if (sysAsm)
            env->checkErr(errhp, OCISessionBegin(svchp, errhp, authp, OCI_CRED_RDBMS, OCI_SYSASM));
        else
            env->checkErr(errhp, OCISessionBegin(svchp, errhp, authp, OCI_CRED_RDBMS, OCI_DEFAULT));

        env->checkErr(errhp, OCIAttrSet(svchp, OCI_HTYPE_SVCCTX,
                                        authp, 0, OCI_ATTR_SESSION, errhp));

        connected = true;
    }

    void DatabaseConnection::disconnect() {
        if (svchp != nullptr && errhp != nullptr)
            OCISessionEnd(svchp, errhp, nullptr, OCI_DEFAULT);

        if (svchp != nullptr && errhp != nullptr)
            OCIServerDetach(srvhp, errhp, OCI_DEFAULT);

        if (authp != nullptr) {
            OCIHandleFree(authp, OCI_HTYPE_SESSION);
            authp = nullptr;
        }

        if (errhp != nullptr) {
            OCIServerDetach(srvhp, errhp, OCI_DEFAULT);
            errhp = nullptr;
        }

        if (svchp != nullptr) {
            OCIHandleFree(svchp, OCI_HTYPE_SVCCTX);
            svchp = nullptr;
        }

        if (authp != nullptr) {
            OCIHandleFree(authp, OCI_HTYPE_SERVER);
            authp = nullptr;
        }

        if (errhp != nullptr) {
            OCIHandleFree(errhp, OCI_HTYPE_ERROR);
            errhp = nullptr;
        }

        connected = false;
    }

    DatabaseConnection::~DatabaseConnection() {
        if (connected)
            disconnect();
    }
}
