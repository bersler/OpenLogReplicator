/* Base performance metrics
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
<http:////www.gnu.org/licenses/>.  */

#include <fstream>
#include <thread>
#include <unistd.h>

#include "Metrics.h"

namespace OpenLogReplicator {
    Metrics::Metrics(uint64_t newTagNames) :
            tagNames(newTagNames) {
    }

    Metrics::~Metrics() {
    }

    bool Metrics::isTagNamesFilter() {
        return (tagNames & TAG_NAMES_FILTER) != 0;
    }

    bool Metrics::isTagNamesSys() {
        return (tagNames & TAG_NAMES_SYS) != 0;
    }
}
