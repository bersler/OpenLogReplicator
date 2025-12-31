/* Definition of type Time
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

#ifndef TIME_H_
#define TIME_H_

#include <iomanip>
#include <ostream>

namespace OpenLogReplicator {
    class Time final {
        uint32_t data;

    public:
        Time(): data(0) {}

        explicit Time(uint32_t newData): data(newData) {}

        [[nodiscard]] uint32_t getVal() const {
            return this->data;
        }

        bool operator==(const Time other) const {
            return data == other.data;
        }

        Time& operator=(uint32_t newData) {
            data = newData;
            return *this;
        }

        [[nodiscard]] time_t toEpoch(int64_t hostTimezone) const {
            uint64_t rest = data;
            const uint64_t sec = rest % 60;
            rest /= 60;
            const uint64_t min = rest % 60;
            rest /= 60;
            const uint64_t hour = rest % 24;
            rest /= 24;
            const uint64_t day = (rest % 31) + 1;
            rest /= 31;
            uint64_t mon = (rest % 12) + 1;
            rest /= 12;
            uint64_t year = rest + 1988;

            if (mon <= 2) {
                mon += 10;
                year -= 1;
            } else
                mon -= 2;

            return (((((static_cast<time_t>((year / 4) - (year / 100) + (year / 400) + (367 * mon / 12) + day) + (year * 365) - 719499) * 24
                    + hour) * 60) + min) * 60) + sec - hostTimezone;
        }

        friend std::ostream& operator<<(std::ostream& os, const Time other) {
            uint64_t rest = other.data;
            const uint64_t ss = rest % 60;
            rest /= 60;
            const uint64_t mi = rest % 60;
            rest /= 60;
            const uint64_t hh = rest % 24;
            rest /= 24;
            const uint64_t dd = (rest % 31) + 1;
            rest /= 31;
            const uint64_t mm = (rest % 12) + 1;
            rest /= 12;
            const uint64_t yy = rest + 1988;
            os << std::setfill('0') << std::setw(2) << std::dec << mm << "/" <<
                    std::setfill('0') << std::setw(2) << std::dec << dd << "/" <<
                    yy << " " << std::setfill('0') << std::setw(2) << std::dec << hh << ":" <<
                    std::setfill('0') << std::setw(2) << std::dec << mi << ":" <<
                    std::setfill('0') << std::setw(2) << std::dec << ss;
            return os;
            // 0123456789012345678
            // DDDDDDDDDD HHHHHHHH
            // 10/15/2018 22:25:36
        }
    };
}

#endif
