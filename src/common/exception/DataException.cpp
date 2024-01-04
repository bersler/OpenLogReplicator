/* Exception used for parsing input
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

#include <iostream>
#include <utility>

#include "DataException.h"

namespace OpenLogReplicator {
    DataException::DataException(int newCode, const std::string& newMsg) :
            exception(),
            code(newCode),
            msg(std::move(newMsg)) {
    }

    DataException::DataException(int newCode, const char* newMsg) :
            exception(),
            code(newCode),
            msg(newMsg) {
    }

    DataException::~DataException() = default;

    std::ostream& operator<<(std::ostream& os, const DataException& exception) {
        os << exception.msg;
        return os;
    }
}
