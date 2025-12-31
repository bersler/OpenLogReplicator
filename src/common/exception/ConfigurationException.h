/* Header for ConfigurationException class
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

#ifndef CONFIGURATION_EXCEPTION_H_
#define CONFIGURATION_EXCEPTION_H_

#include <ctime>
#include <exception>
#include <sstream>
#include <string>

#include "../types/Types.h"

namespace OpenLogReplicator {
    class ConfigurationException final : public std::exception {
    public:
        int code;
        std::string msg;

        explicit ConfigurationException(int newCode, std::string newMsg);
        ConfigurationException(const ConfigurationException&) = delete;
        ConfigurationException& operator=(const ConfigurationException&) = delete;

        friend std::ostream& operator<<(std::ostream& os, const ConfigurationException& exception);
    };
}

#endif
