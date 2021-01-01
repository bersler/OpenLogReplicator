/* Class to handle oracle connection environment
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

#include "DatabaseEnvironment.h"
#include "RuntimeException.h"

using namespace std;

namespace OpenLogReplicator {

    DatabaseEnvironment::DatabaseEnvironment() :
            envhp(nullptr) {
        OCIEnvCreate(&envhp, OCI_THREADED, nullptr, nullptr, nullptr, nullptr, 0, nullptr);
    }

    DatabaseEnvironment::~DatabaseEnvironment() {
        OCIHandleFree(envhp, OCI_HTYPE_ENV);
        OCITerminate(OCI_DEFAULT);
    }

    void DatabaseEnvironment::checkErr(OCIError *errhp, sword status) {
        sb4 errcode = 0;
        uint64_t len;
        text errbuf1[512];
        text errbuf2[512];

        switch (status) {
            case OCI_SUCCESS:
                break;

            case OCI_SUCCESS_WITH_INFO:
                cerr << "ERROR: OCI_SUCCESS_WITH_INFO" << endl;
                OCIErrorGet(errhp, 1, nullptr, &errcode, errbuf1, sizeof(errbuf1), OCI_HTYPE_ERROR);
                if (errcode != 100)
                    cerr << "WARNING: " << errbuf1 << endl;
                OCIErrorGet(errhp, 2, nullptr, &errcode, errbuf2, sizeof(errbuf2), OCI_HTYPE_ERROR);
                if (errcode != 100)
                    cerr << "WARNING: " << errbuf1 << endl;
                break;

            case OCI_NEED_DATA:
                RUNTIME_FAIL("OCI ERROR: OCI_NEED_DATA");
                break;

            case OCI_NO_DATA:
                RUNTIME_FAIL("OCI ERROR: OCI_NODATA");
                break;

            case OCI_ERROR:
                OCIErrorGet(errhp, 1, nullptr, &errcode, errbuf1, sizeof(errbuf1), OCI_HTYPE_ERROR);
                if (errcode == 1405) //fetched column value is NULL
                    return;
                len = strlen((char*)errbuf1);
                if (len > 0 && errbuf1[len - 1] == '\n')
                    errbuf1[len - 1] = 0;

                OCIErrorGet(errhp, 2, nullptr, &errcode, errbuf2, sizeof(errbuf2), OCI_HTYPE_ERROR);
                len = strlen((char*)errbuf2);
                if (len > 0 && errbuf2[len - 1] == '\n')
                    errbuf2[len - 1] = 0;

                if (errcode != 100) {
                    RUNTIME_FAIL("OCI ERROR: [" << errbuf1 << "]" << endl << "[" << errbuf2 << "]");
                } else {
                    RUNTIME_FAIL("OCI ERROR: [" << errbuf1 << "]");
                };
                break;

            case OCI_INVALID_HANDLE:
                RUNTIME_FAIL("OCI ERROR: OCI_INVALID_HANDLE");
                break;

            case OCI_STILL_EXECUTING:
                RUNTIME_FAIL("OCI ERROR: OCI_STILL_EXECUTING");
                break;

            case OCI_CONTINUE:
                RUNTIME_FAIL("OCI ERROR: OCI_CONTINUE");
                break;

            case OCI_ROWCBK_DONE:
                RUNTIME_FAIL("OCI ERROR: OCI_CONTINUE");
                break;
        }
    }
}
