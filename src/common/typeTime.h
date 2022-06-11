/* Header for type typeTime
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <cstring>
#include <ctime>
#include <iomanip>
#include <ostream>

#ifndef TYPE_TIME_H_
#define TYPE_TIME_H_

namespace OpenLogReplicator {
    class typeTime {
        uint32_t val;
    public:
        typeTime() : val(0) {
        }

        explicit typeTime(uint32_t newVal) : val(newVal) {
        }

        [[nodiscard]] uint32_t getVal() const {
            return this->val;
        }

        bool operator== (const typeTime& other) const {
            return val == other.val;
        }

        typeTime& operator= (uint32_t newVal) {
            val = newVal;
            return *this;
        }

        [[nodiscard]] time_t toTime() const {
            struct tm epochtime = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, nullptr};
            memset((void*)&epochtime, 0, sizeof(epochtime));
            uint64_t rest = val;
            epochtime.tm_sec = (int)(rest % 60);
            rest /= 60;
            epochtime.tm_min = (int)(rest % 60);
            rest /= 60;
            epochtime.tm_hour = (int)(rest % 24);
            rest /= 24;
            epochtime.tm_mday = (int)((rest % 31) + 1);
            rest /= 31;
            epochtime.tm_mon = (int)(rest % 12);
            rest /= 12;
            epochtime.tm_year = (int)(rest + 88);
            return mktime(&epochtime);
        }

        void toIso8601(char* buffer) const {
            uint64_t rest = val;
            uint64_t ss = rest % 60;
            rest /= 60;
            uint64_t mi = rest % 60;
            rest /= 60;
            uint64_t hh = rest % 24;
            rest /= 24;
            uint64_t dd = (rest % 31) + 1;
            rest /= 31;
            uint64_t mm = (rest % 12) + 1;
            rest /= 12;
            uint64_t yy = rest + 1988;
            buffer[3] = '0' + (char)(yy % 10);
            yy /= 10;
            buffer[2] = '0' + (char)(yy % 10);
            yy /= 10;
            buffer[1] = '0' + (char)(yy % 10);
            yy /= 10;
            buffer[0] = '0' + (char)yy;
            buffer[4] = '-';
            buffer[6] = '0' + (char)(mm % 10);
            mm /= 10;
            buffer[5] = '0' + (char)mm;
            buffer[7] = '-';
            buffer[9] = '0' + (char)(dd % 10);
            dd /= 10;
            buffer[8] = '0' + (char)dd;
            buffer[10] = 'T';
            buffer[12] = '0' + (char)(hh % 10);
            hh /= 10;
            buffer[11] = '0' + (char)hh;
            buffer[13] = ':';
            buffer[15] = '0' + (char)(mi % 10);
            mi /= 10;
            buffer[14] = '0' + (char)mi;
            buffer[16] = ':';
            buffer[18] = '0' + (char)(ss % 10);
            ss /= 10;
            buffer[17] = '0' + (char)ss;
            buffer[19] = 'Z';
            buffer[20] = 0;
            //01234567890123456789
            //YYYY-MM-DDThh:mm:ssZ
        }

        friend std::ostream& operator<<(std::ostream& os, const typeTime& time_) {
            uint64_t rest = time_.val;
            uint64_t ss = rest % 60; rest /= 60;
            uint64_t mi = rest % 60; rest /= 60;
            uint64_t hh = rest % 24; rest /= 24;
            uint64_t dd = (rest % 31) + 1; rest /= 31;
            uint64_t mm = (rest % 12) + 1; rest /= 12;
            uint64_t yy = rest + 1988;
            os << std::setfill('0') << std::setw(2) << std::dec << mm << "/" <<
               std::setfill('0') << std::setw(2) << std::dec << dd << "/" <<
               yy << " " << std::setfill('0') << std::setw(2) << std::dec << hh << ":" <<
               std::setfill('0') << std::setw(2) << std::dec << mi << ":" <<
               std::setfill('0') << std::setw(2) << std::dec << ss;
            return os;
            //0123456789012345678
            //DDDDDDDDDD HHHHHHHH
            //10/15/2018 22:25:36
        }
    };
}

#endif
