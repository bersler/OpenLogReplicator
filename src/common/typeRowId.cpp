/* Class to hold typeRowId decoder
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

#include "typeRowId.h"

namespace OpenLogReplicator {
    const char typeRowId::map64[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    const char typeRowId::map64R[256] = {
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
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    typeRowId::typeRowId() : dataObj(0), dba(0), slot(0) {
    }

    typeRowId::typeRowId(const char* rowid) {
        if (strlen(rowid) != 18) {
            ERROR("RowID: incorrect format: " << rowid)
        }
        dataObj = (((typeDba)map64R[(uint8_t)rowid[0]]) << 30) |
                  (((typeDba)map64R[(uint8_t)rowid[1]]) << 24) |
                  (((typeDba)map64R[(uint8_t)rowid[2]]) << 18) |
                  (((typeDba)map64R[(uint8_t)rowid[3]]) << 12) |
                  (((typeDba)map64R[(uint8_t)rowid[4]]) << 6) |
                  ((typeDba)map64R[(uint8_t)rowid[5]]);

        typeAfn afn = (((typeAfn)map64R[(uint8_t)rowid[6]]) << 12) |
                      (((typeAfn)map64R[(uint8_t)rowid[7]]) << 6) |
                      ((typeAfn)map64R[(uint8_t)rowid[8]]);

        dba = (((typeDba)map64R[(uint8_t)rowid[9]]) << 30) |
              (((typeDba)map64R[(uint8_t)rowid[10]]) << 24) |
              (((typeDba)map64R[(uint8_t)rowid[11]]) << 18) |
              (((typeDba)map64R[(uint8_t)rowid[12]]) << 12) |
              (((typeDba)map64R[(uint8_t)rowid[13]]) << 6) |
              ((typeDba)map64R[(uint8_t)rowid[14]]) |
              (((typeDba)afn) << 22);

        slot = (((typeSlot)map64R[(uint8_t)rowid[15]]) << 12) |
               (((typeSlot)map64R[(uint8_t)rowid[16]]) << 6) |
               ((typeSlot)map64R[(uint8_t)rowid[17]]);
    }

    typeRowId::typeRowId(typeDataObj newDataObj, typeDba newDba, typeSlot newSlot):
            dataObj(newDataObj),
            dba(newDba),
            slot(newSlot) {
    }

    bool typeRowId::operator<(const typeRowId& other) const {
        if (dataObj < other.dataObj)
            return true;
        if (other.dataObj == dataObj) {
            if (dba < other.dba)
                return true;
            if (dba == other.dba && slot < other.slot)
                return true;
        }
        return false;
    }

    bool typeRowId::operator==(const typeRowId& other) const {
        return (other.dataObj == dataObj) &&
                (other.dba == dba) &&
                (other.slot == slot);
    }

    bool typeRowId::operator!=(const typeRowId& other) const {
        return (other.dataObj != dataObj) ||
                (other.dba != dba) ||
                (other.slot != slot);
    }

    void typeRowId::toString(char* str) const {
        typeAfn afn = dba >> 22;
        typeDba bdba = dba & 0x003FFFFF;

        str[0] = map64[(dataObj >> 30) & 0x3F];
        str[1] = map64[(dataObj >> 24) & 0x3F];
        str[2] = map64[(dataObj >> 18) & 0x3F];
        str[3] = map64[(dataObj >> 12) & 0x3F];
        str[4] = map64[(dataObj >> 6) & 0x3F];
        str[5] = map64[dataObj & 0x3F];
        str[6] = map64[(afn >> 12) & 0x3F];
        str[7] = map64[(afn >> 6) & 0x3F];
        str[8] = map64[afn & 0x3F];
        str[9] = map64[(bdba >> 30) & 0x3F];
        str[10] = map64[(bdba >> 24) & 0x3F];
        str[11] = map64[(bdba >> 18) & 0x3F];
        str[12] = map64[(bdba >> 12) & 0x3F];
        str[13] = map64[(bdba >> 6) & 0x3F];
        str[14] = map64[bdba & 0x3F];
        str[15] = map64[(slot >> 12) & 0x3F];
        str[16] = map64[(slot >> 6) & 0x3F];
        str[17] = map64[slot & 0x3F];
        str[18] = 0;
    }

    std::string typeRowId::toString() const {
        char str[19];
        typeAfn afn = dba >> 22;
        typeDba bdba = dba & 0x003FFFFF;

        str[0] = map64[(dataObj >> 30) & 0x3F];
        str[1] = map64[(dataObj >> 24) & 0x3F];
        str[2] = map64[(dataObj >> 18) & 0x3F];
        str[3] = map64[(dataObj >> 12) & 0x3F];
        str[4] = map64[(dataObj >> 6) & 0x3F];
        str[5] = map64[dataObj & 0x3F];
        str[6] = map64[(afn >> 12) & 0x3F];
        str[7] = map64[(afn >> 6) & 0x3F];
        str[8] = map64[afn & 0x3F];
        str[9] = map64[(bdba >> 30) & 0x3F];
        str[10] = map64[(bdba >> 24) & 0x3F];
        str[11] = map64[(bdba >> 18) & 0x3F];
        str[12] = map64[(bdba >> 12) & 0x3F];
        str[13] = map64[(bdba >> 6) & 0x3F];
        str[14] = map64[bdba & 0x3F];
        str[15] = map64[(slot >> 12) & 0x3F];
        str[16] = map64[(slot >> 6) & 0x3F];
        str[17] = map64[slot & 0x3F];
        str[18] = 0;
        return std::string(str);
    }

    std::ostream& operator<<(std::ostream& os, const typeRowId& tran) {
        char str[19];
        tran.toString(str);
        os << str;
        return os;
    }
}

namespace std {
    size_t std::hash<OpenLogReplicator::typeRowId>::operator()(const OpenLogReplicator::typeRowId& rowId) const {
        return hash<typeDataObj>()(rowId.dataObj) ^
               hash<typeDba>()(rowId.dba) ^
               hash<typeSlot>()(rowId.slot);
    }
}
