/* Header for State class
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <set>

#include "../common/types.h"

#ifndef STATE_H_
#define STATE_H_

#define STATE_TYPE_DISK 0

namespace OpenLogReplicator {
    class State {
    public:
        State();
        virtual ~State();

        virtual void list(std::set<std::string>& namesList) = 0;
        [[nodiscard]] virtual bool read(const std::string& name, uint64_t maxSize, std::string& in) = 0;
        virtual void write(const std::string& name, const std::ostringstream& out) = 0;
        virtual void drop(const std::string& name) = 0;
    };
}

#endif
