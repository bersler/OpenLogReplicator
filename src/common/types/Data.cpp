/* Static data values
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

#include <cctype>
#include <string>

#include "Data.h"
#include "Types.h"
#include "../exception/DataException.h"
#include "../exception/RuntimeException.h"

namespace OpenLogReplicator {
    const char Data::map64L[65]{"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"};

    const char Data::map64R[256]{
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 62, 0, 0, 0, 63,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 0, 0, 0, 0, 0, 0,
        0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 0, 0, 0, 0, 0,
        0, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    };

    const int64_t Data::cumDays[12]{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    const int64_t Data::cumDaysLeap[12]{0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335};

    bool Data::parseTimezone(std::string str, int64_t& out) {
        if (str == "Etc/GMT-14") str = "-14:00";
        else if (str == "Etc/GMT-13") str = "-13:00";
        else if (str == "Etc/GMT-12") str = "-12:00";
        else if (str == "Etc/GMT-11") str = "-11:00";
        else if (str == "HST") str = "-10:00";
        else if (str == "Etc/GMT-10") str = "-10:00";
        else if (str == "Etc/GMT-9") str = "-09:00";
        else if (str == "PST") str = "-08:00";
        else if (str == "PST8PDT") str = "-08:00";
        else if (str == "Etc/GMT-8") str = "-08:00";
        else if (str == "MST") str = "-07:00";
        else if (str == "MST7MDT") str = "-07:00";
        else if (str == "Etc/GMT-7") str = "-07:00";
        else if (str == "CST") str = "-06:00";
        else if (str == "CST6CDT") str = "-06:00";
        else if (str == "Etc/GMT-6") str = "-06:00";
        else if (str == "EST") str = "-05:00";
        else if (str == "EST5EDT") str = "-05:00";
        else if (str == "Etc/GMT-5") str = "-05:00";
        else if (str == "Etc/GMT-4") str = "-04:00";
        else if (str == "Etc/GMT-3") str = "-03:00";
        else if (str == "Etc/GMT-2") str = "-02:00";
        else if (str == "Etc/GMT-1") str = "-01:00";
        else if (str == "GMT") str = "+00:00";
        else if (str == "Etc/GMT") str = "+00:00";
        else if (str == "Greenwich") str = "+00:00";
        else if (str == "Etc/Greenwich") str = "+00:00";
        else if (str == "GMT0") str = "+00:00";
        else if (str == "Etc/GMT0") str = "+00:00";
        else if (str == "GMT+0") str = "+00:00";
        else if (str == "Etc/GMT-0") str = "+00:00";
        else if (str == "GMT+0") str = "+00:00";
        else if (str == "Etc/GMT+0") str = "+00:00";
        else if (str == "UTC") str = "+00:00";
        else if (str == "Etc/UTC") str = "+00:00";
        else if (str == "UCT") str = "+00:00";
        else if (str == "Etc/UCT") str = "+00:00";
        else if (str == "Universal") str = "+00:00";
        else if (str == "Etc/Universal") str = "+00:00";
        else if (str == "WET") str = "+00:00";
        else if (str == "MET") str = "+01:00";
        else if (str == "CET") str = "+01:00";
        else if (str == "Etc/GMT+1") str = "+01:00";
        else if (str == "EET") str = "+02:00";
        else if (str == "Etc/GMT+2") str = "+02:00";
        else if (str == "Etc/GMT+3") str = "+03:00";
        else if (str == "Etc/GMT+4") str = "+04:00";
        else if (str == "Etc/GMT+5") str = "+05:00";
        else if (str == "Etc/GMT+6") str = "+06:00";
        else if (str == "Etc/GMT+7") str = "+07:00";
        else if (str == "PRC") str = "+08:00";
        else if (str == "ROC") str = "+08:00";
        else if (str == "Etc/GMT+8") str = "+08:00";
        else if (str == "Etc/GMT+9") str = "+09:00";
        else if (str == "Etc/GMT+10") str = "+10:00";
        else if (str == "Etc/GMT+11") str = "+11:00";
        else
            if (str == "Etc/GMT+12") str = "+12:00";

        if (str.length() == 5) {
            if (str[1] >= '0' && str[1] <= '9' &&
                str[2] == ':' &&
                str[3] >= '0' && str[3] <= '9' &&
                str[4] >= '0' && str[4] <= '9') {
                out = -(str[1] - '0') * 3600 + (str[3] - '0') * 60 + (str[4] - '0');
            } else
                return false;
        } else if (str.length() == 6) {
            if (str[1] >= '0' && str[1] <= '9' &&
                str[2] >= '0' && str[2] <= '9' &&
                str[3] == ':' &&
                str[4] >= '0' && str[4] <= '9' &&
                str[5] >= '0' && str[5] <= '9') {
                out = -(str[1] - '0') * 36000 + (str[2] - '0') * 3600 + (str[4] - '0') * 60 + (str[5] - '0');
            } else
                return false;
        } else
            return false;

        if (str[0] == '-')
            out = -out;
        else if (str[0] != '+')
            return false;

        return true;
    }

    std::string Data::timezoneToString(int64_t tz) {
        char result[7];

        if (tz < 0) {
            result[0] = '-';
            tz = -tz;
        } else
            result[0] = '+';

        tz /= 60;

        result[6] = 0;
        result[5] = map10(tz % 10);
        tz /= 10;
        result[4] = map10(tz % 6);
        tz /= 6;
        result[3] = ':';
        result[2] = map10(tz % 10);
        tz /= 10;
        result[1] = map10(tz % 10);

        return result;
    }

    time_t Data::valuesToEpoch(int year, int month, int day, int hour, int minute, int second, int tz) {
        time_t result;

        if (year > 0) {
            result = yearToDays(year, month) + cumDays[month % 12] + day;
            result *= 24;
            result += hour;
            result *= 60;
            result += minute;
            result *= 60;
            result += second;
            return result - UNIX_AD1970_01_01 - tz; // adjust to 1970 epoch, 719,527 days
        }

        // treat dates BC with the exact rules as AD for leap years
        result = -yearToDaysBC(-year, month) + cumDays[month % 12] + day;
        result *= 24;
        result += hour;
        result *= 60;
        result += minute;
        result *= 60;
        result += second;
        return result - UNIX_BC1970_01_01 - tz; // adjust to 1970 epoch, 718,798 days (year 0 does not exist)
    }

    uint64_t Data::epochToIso8601(time_t timestamp, char* buffer, bool addT, bool addZ) {
        // (-)YYYY-MM-DD hh:mm:ss or (-)YYYY-MM-DDThh:mm:ssZ

        if (unlikely(timestamp < UNIX_BC4712_01_01 || timestamp > UNIX_AD9999_12_31))
            throw RuntimeException(10069, "invalid timestamp value: " + std::to_string(timestamp));

        timestamp += UNIX_AD1970_01_01;
        if (likely(timestamp >= 365 * 60 * 60 * 24)) {
            // AD
            int64_t second = (timestamp % 60);
            timestamp /= 60;
            int64_t minute = (timestamp % 60);
            timestamp /= 60;
            int64_t hour = (timestamp % 24);
            timestamp /= 24;

            int64_t year = (timestamp / 365) + 1;
            int64_t day = yearToDays(year, 0);

            while (day > timestamp) {
                --year;
                day = yearToDays(year, 0);
            }
            day = timestamp - day;

            int64_t month = day / 27;
            month = std::min<int64_t>(month, 11);

            if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0)) {
                // leap year
                while (cumDaysLeap[month] > day)
                    --month;
                day -= cumDaysLeap[month];
            } else {
                while (cumDays[month] > day)
                    --month;
                day -= cumDays[month];
            }
            ++month;
            ++day;

            buffer[3] = map10(year % 10);
            year /= 10;
            buffer[2] = map10(year % 10);
            year /= 10;
            buffer[1] = map10(year % 10);
            year /= 10;
            buffer[0] = map10(year);
            buffer[4] = '-';
            buffer[6] = map10(month % 10);
            month /= 10;
            buffer[5] = map10(month);
            buffer[7] = '-';
            buffer[9] = map10(day % 10);
            day /= 10;
            buffer[8] = map10(day);
            if (addT)
                buffer[10] = 'T';
            else
                buffer[10] = ' ';
            buffer[12] = map10(hour % 10);
            hour /= 10;
            buffer[11] = map10(hour);
            buffer[13] = ':';
            buffer[15] = map10(minute % 10);
            minute /= 10;
            buffer[14] = map10(minute);
            buffer[16] = ':';
            buffer[18] = map10(second % 10);
            second /= 10;
            buffer[17] = map10(second);
            if (addZ) {
                buffer[19] = 'Z';
                buffer[20] = 0;
                return 20;
            }

            buffer[19] = 0;
            return 19;
        }

        // BC
        timestamp = 365 * 24 * 60 * 60 - timestamp;

        int64_t second = (timestamp % 60);
        timestamp /= 60;
        int64_t minute = (timestamp % 60);
        timestamp /= 60;
        int64_t hour = (timestamp % 24);
        timestamp /= 24;

        int64_t year = (timestamp / 366) - 1;
        year = std::max<int64_t>(year, 0);
        int64_t day = yearToDaysBC(year, 0);

        while (day < timestamp) {
            ++year;
            day = yearToDaysBC(year, 0);
        }
        day -= timestamp;

        int64_t month = day / 27;
        month = std::min<int64_t>(month, 11);

        if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0)) {
            // leap year
            while (cumDaysLeap[month] > day)
                --month;
            day -= cumDaysLeap[month];
        } else {
            while (cumDays[month] > day)
                --month;
            day -= cumDays[month];
        }
        ++month;
        ++day;
        buffer[0] = '-';
        buffer[4] = map10(year % 10);
        year /= 10;
        buffer[3] = map10(year % 10);
        year /= 10;
        buffer[2] = map10(year % 10);
        year /= 10;
        buffer[1] = map10(year);
        buffer[5] = '-';
        buffer[7] = map10(month % 10);
        month /= 10;
        buffer[6] = map10(month);
        buffer[8] = '-';
        buffer[10] = map10(day % 10);
        day /= 10;
        buffer[9] = map10(day);
        if (addT)
            buffer[11] = 'T';
        else
            buffer[11] = ' ';
        buffer[13] = map10(hour % 10);
        hour /= 10;
        buffer[12] = map10(hour);
        buffer[14] = ':';
        buffer[16] = map10(minute % 10);
        minute /= 10;
        buffer[15] = map10(minute);
        buffer[17] = ':';
        buffer[19] = map10(second % 10);
        second /= 10;
        buffer[18] = map10(second);
        if (addZ) {
            buffer[20] = 'Z';
            buffer[21] = 0;
            return 21;
        }

        buffer[20] = 0;
        return 20;
    }

    std::ostringstream& Data::writeEscapeValue(std::ostringstream& ss, const std::string& str) {
        const char* c_str = str.c_str();
        for (uint i = 0; i < str.length(); ++i) {
            switch (*c_str) {
                case '\t':
                    ss << "\\t";
                    break;
                case '\r':
                    ss << "\\r";
                    break;
                case '\n':
                    ss << "\\n";
                    break;
                case '\b':
                    ss << "\\b";
                    break;
                case '\f':
                    ss << "\\f";
                    break;
                case '"':
                case '\\':
                    ss << '\\' << *c_str;
                    break;
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                //case 8: // \b
                //case 9: // \t
                //case 10: // \n
                case 11:
                //case 12: // \f
                //case 13: // \r
                case 14:
                case 15:
                case 16:
                case 17:
                case 18:
                case 19:
                case 20:
                case 21:
                case 22:
                case 23:
                case 24:
                case 25:
                case 26:
                case 27:
                case 28:
                case 29:
                case 30:
                case 31:
                    ss << "\\u00" << map16((*c_str >> 4) & 0x0F) << map16(*c_str & 0x0F);
                    break;
                default:
                    ss << *c_str;
            }
            ++c_str;
        }
        return ss;
    }

    void Data::checkName(const std::string& name) {
        if (unlikely(name.length() >= 1024))
            throw DataException(20004, "identifier '" + std::string(name) + "' is too long");
    }
}
