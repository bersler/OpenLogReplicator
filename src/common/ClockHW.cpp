/* Base class for hardware clockś
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

#include "ClockHW.h"

namespace OpenLogReplicator {
    time_ut ClockHW::getTimeUt() const {
        struct timeval tv = {0, 0};
        gettimeofday(&tv, nullptr);
        return (1000000 * tv.tv_sec) + tv.tv_usec;
    }

    time_t ClockHW::getTimeT() const {
        return time(nullptr);
    }
}
