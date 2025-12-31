/* Header for RedoLogException class
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

#ifndef REDO_LOG_EXCEPTION_H_
#define REDO_LOG_EXCEPTION_H_

#include <exception>
#include <sstream>
#include <ctime>
#include <string>

#include "../types/Types.h"

namespace OpenLogReplicator {
    class RedoLogException final : public std::exception {
    public:
        int code;
        std::string msg;

        explicit RedoLogException(int newCode, std::string newMsg);
        RedoLogException(const RedoLogException&) = delete;
        RedoLogException& operator=(const RedoLogException&) = delete;

        friend std::ostream& operator<<(std::ostream& os, const RedoLogException& exception);
    };
}

#endif
