/* Class to handle oracle connection environment
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

#include <cstring>

#include "../common/Ctx.h"
#include "../common/exception/RuntimeException.h"
#include "DatabaseEnvironment.h"

namespace OpenLogReplicator {
    DatabaseEnvironment::DatabaseEnvironment(Ctx* newCtx) :
            ctx(newCtx),
            envhp(nullptr) {
    }

    DatabaseEnvironment::~DatabaseEnvironment() {
        if (envhp != nullptr)
            OCIHandleFree(envhp, OCI_HTYPE_ENV);
        // OCITerminate(OCI_DEFAULT);
    }

    void DatabaseEnvironment::initialize() {
        OCIEnvCreate(&envhp, OCI_THREADED, nullptr, nullptr, nullptr, nullptr, 0, nullptr);

        if (envhp == nullptr)
            throw RuntimeException(10050, "can't initialize oracle environment (OCI)");
    }

    void DatabaseEnvironment::checkErr(OCIError* errhp, sword status) {
        sb4 errcode1 = 0;
        sb4 errcode2 = 0;
        uint64_t len;
        text errbuf1[512];
        text errbuf2[512];

        switch (status) {
            case OCI_SUCCESS:
                break;

            case OCI_SUCCESS_WITH_INFO:
                OCIErrorGet(errhp, 1, nullptr, &errcode1, errbuf1, sizeof(errbuf1), OCI_HTYPE_ERROR);
                if (errcode1 != 100)
                    ctx->warning(70006, "OCI: " + std::string(reinterpret_cast<const char*>(errbuf1)));
                OCIErrorGet(errhp, 2, nullptr, &errcode2, errbuf2, sizeof(errbuf2), OCI_HTYPE_ERROR);
                if (errcode2 != 100)
                    ctx->warning(70006, "OCI: " + std::string(reinterpret_cast<const char*>(errbuf1)));
                break;

            case OCI_NEED_DATA:
                throw RuntimeException(10051, "OCI ERROR: OCI_NEED_DATA");

            case OCI_NO_DATA:
                throw RuntimeException(10051, "OCI ERROR: OCI_NODATA");

            case OCI_ERROR:
                OCIErrorGet(errhp, 1, nullptr, &errcode1, errbuf1, sizeof(errbuf1), OCI_HTYPE_ERROR);
                // Fetched column value is NULL
                if (errcode1 == 1405)
                    return;
                len = strlen(reinterpret_cast<const char*>(errbuf1));
                if (len > 0 && errbuf1[len - 1] == '\n')
                    errbuf1[len - 1] = 0;

                OCIErrorGet(errhp, 2, nullptr, &errcode2, errbuf2, sizeof(errbuf2), OCI_HTYPE_ERROR);
                len = strlen(reinterpret_cast<const char*>(errbuf2));
                if (len > 0 && errbuf2[len - 1] == '\n')
                    errbuf2[len - 1] = 0;

                if (errcode2 != 100)
                    ctx->error(10051, "OCI: [" + std::string(reinterpret_cast<const char*>(errbuf2)) + "]");
                throw RuntimeException(10051, "OCI: [" + std::string(reinterpret_cast<const char*>(errbuf1)) + "]", errcode1);

            case OCI_INVALID_HANDLE:
                throw RuntimeException(10051, "OCI: OCI_INVALID_HANDLE");

            case OCI_STILL_EXECUTING:
                throw RuntimeException(10051, "OCI: OCI_STILL_EXECUTING");

            case OCI_CONTINUE:
                throw RuntimeException(10051, "OCI: OCI_CONTINUE");

            case OCI_ROWCBK_DONE:
                throw RuntimeException(10051, "OCI: OCI_CONTINUE");
        }
    }
}
