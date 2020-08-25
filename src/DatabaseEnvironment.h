/* Header for DatabaseEnvironment class
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

#ifdef ONLINE_MODEIMPL_OCI
#include <oci.h>
#endif /* ONLINE_MODEIMPL_OCI */

#ifndef DATABASEENVIRONMENT_H_
#define DATABASEENVIRONMENT_H_

using namespace std;

namespace OpenLogReplicator {

    class DatabaseStatement;

    class DatabaseEnvironment {
    public:
#ifdef ONLINE_MODEIMPL_OCI
        OCIEnv *envhp;
#endif /* ONLINE_MODEIMPL_OCI */

        DatabaseEnvironment();
        virtual ~DatabaseEnvironment();

#ifdef ONLINE_MODEIMPL_OCI
        void checkErr(OCIError *errhp, sword status);
#endif /* ONLINE_MODEIMPL_OCI */
    };
}

#endif
