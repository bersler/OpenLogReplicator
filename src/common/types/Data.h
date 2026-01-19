/* Header for static data values
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

#ifndef DATA_H_
#define DATA_H_

#include <cstdint>

#include "Types.h"

namespace OpenLogReplicator {
    class Data final {
    protected:
        static constexpr time_t UNIX_AD1970_01_01{62167132800L};
        static constexpr time_t UNIX_BC1970_01_01{62167132800L - (static_cast<long>(365 * 24 * 60 * 60))};
        static constexpr time_t UNIX_BC4712_01_01{-210831897600L};
        static constexpr time_t UNIX_AD9999_12_31{253402300799L};

        static int64_t yearToDays(int64_t year, int64_t month) {
            int64_t result = (year * 365) + (year / 4) - (year / 100) + (year / 400);
            if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0) && month < 2)
                --result;

            return result;
        }

        static int64_t yearToDaysBC(int64_t year, int64_t month) {
            int64_t result = (year * 365) + (year / 4) - (year / 100) + (year / 400);
            if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0) && month >= 2)
                --result;

            return result;
        }

    public:
        static const char map64L[65];
        static const char map64R[256];

        static const int64_t cumDays[12];
        static const int64_t cumDaysLeap[12];

        static char map10(uint x) {
            return static_cast<char>('0' + x);
        }

        static char map16(uint x) {
            if (x < 10)
                return static_cast<char>('0' + x);
            return static_cast<char>('a' + (x - 10));
        }

        static char map16U(uint x) {
            if (x < 10)
                return static_cast<char>('0' + x);
            return static_cast<char>('A' + (x - 10));
        }

        static char map64(uint x) {
            return map64L[x];
        }

        static bool parseTimezone(std::string str, int64_t& out);
        static std::string timezoneToString(int64_t tz);
        static time_t valuesToEpoch(int year, int month, int day, int hour, int minute, int second, int tz);
        static uint64_t epochToIso8601(time_t timestamp, char* buffer, bool addT, bool addZ);
        static std::ostringstream& writeEscapeValue(std::ostringstream& ss, const std::string& str);
        static void checkName(const std::string& name);
    };
}

#endif
