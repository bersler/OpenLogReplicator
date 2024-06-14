/* Header for StateDisk class
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

#include "State.h"

#ifndef STATE_DISK_H_
#define STATE_DISK_H_

namespace OpenLogReplicator {
    class StateDisk final : public State {
    protected:
        std::string path;

    public:
        explicit StateDisk(Ctx* newCtx, const char* newPath);
        ~StateDisk() override;

        void list(std::set<std::string>& namesList) const override;
        [[nodiscard]] bool read(const std::string& name, uint64_t maxSize, std::string& in) override;
        void write(const std::string& name, typeScn scn, const std::ostringstream& out) override;
        void drop(const std::string& name) override;
    };
}

#endif
