/* Definition of type typeRowId
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

#include <functional>
#include <cstring>

#include "Ctx.h"
#include "types.h"
#include "exception/DataException.h"

#ifndef TYPE_ROWID_H_
#define TYPE_ROWID_H_

#define ROWID_LENGTH 18

namespace OpenLogReplicator {
    class typeRowId final {
    public:
        typeDataObj dataObj;
        typeDba dba;
        typeSlot slot;

        typeRowId() :
                dataObj(0),
                dba(0),
                slot(0) {
        }

        explicit typeRowId(const char* rowid) {
            if (strlen(rowid) != 18)
                throw DataException(20008, "row ID incorrect length: " + std::string(rowid));

            dataObj = (static_cast<typeDataObj>(Ctx::map64R[static_cast<uint8_t>(rowid[0])]) << 30) |
                      (static_cast<typeDataObj>(Ctx::map64R[static_cast<uint8_t>(rowid[1])]) << 24) |
                      (static_cast<typeDataObj>(Ctx::map64R[static_cast<uint8_t>(rowid[2])]) << 18) |
                      (static_cast<typeDataObj>(Ctx::map64R[static_cast<uint8_t>(rowid[3])]) << 12) |
                      (static_cast<typeDataObj>(Ctx::map64R[static_cast<uint8_t>(rowid[4])]) << 6) |
                      static_cast<typeDataObj>(Ctx::map64R[static_cast<uint8_t>(rowid[5])]);

            typeAfn afn = (static_cast<typeAfn>(Ctx::map64R[static_cast<uint8_t>(rowid[6])]) << 12) |
                          (static_cast<typeAfn>(Ctx::map64R[static_cast<uint8_t>(rowid[7])]) << 6) |
                          static_cast<typeAfn>(Ctx::map64R[static_cast<uint8_t>(rowid[8])]);

            dba = (static_cast<typeDba>(Ctx::map64R[static_cast<uint8_t>(rowid[9])]) << 30) |
                  (static_cast<typeDba>(Ctx::map64R[static_cast<uint8_t>(rowid[10])]) << 24) |
                  (static_cast<typeDba>(Ctx::map64R[static_cast<uint8_t>(rowid[11])]) << 18) |
                  (static_cast<typeDba>(Ctx::map64R[static_cast<uint8_t>(rowid[12])]) << 12) |
                  (static_cast<typeDba>(Ctx::map64R[static_cast<uint8_t>(rowid[13])]) << 6) |
                  static_cast<typeDba>(Ctx::map64R[static_cast<uint8_t>(rowid[14])]) |
                  (static_cast<typeDba>(afn) << 22);

            slot = (static_cast<typeSlot>(Ctx::map64R[static_cast<uint8_t>(rowid[15])]) << 12) |
                   (static_cast<typeSlot>(Ctx::map64R[static_cast<uint8_t>(rowid[16])]) << 6) |
                   static_cast<typeSlot>(Ctx::map64R[static_cast<uint8_t>(rowid[17])]);
        }

        typeRowId(typeDataObj newDataObj, typeDba newDba, typeSlot newSlot) :
                dataObj(newDataObj),
                dba(newDba),
                slot(newSlot) {
        }

        bool operator<(const typeRowId other) const {
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

        void decodeFromHex(const uint8_t* data) {
            dataObj = (static_cast<typeDataObj>(data[0]) << 24) |
                      (static_cast<typeDataObj>(data[1]) << 16) |
                      (static_cast<typeDataObj>(data[2]) << 8) |
                      (static_cast<typeDataObj>(data[3]));

            slot = (static_cast<typeSlot>(data[4]) << 8) |
                   (static_cast<typeSlot>(data[5]));

            typeAfn afn = (static_cast<typeAfn>(data[6]) << 8) |
                          (static_cast<typeAfn>(data[7]));

            dba = (static_cast<typeDataObj>(data[8]) << 24) |
                  (static_cast<typeDataObj>(data[9]) << 16) |
                  (static_cast<typeDataObj>(data[10]) << 8) |
                  (static_cast<typeDataObj>(data[11])) |
                  (static_cast<typeDba>(afn) << 22);
        }

        bool operator!=(const typeRowId other) const {
            return (other.dataObj != dataObj) ||
                   (other.dba != dba) ||
                   (other.slot != slot);
        }

        bool operator==(const typeRowId other) const {
            return (other.dataObj == dataObj) &&
                   (other.dba == dba) &&
                   (other.slot == slot);
        }

        void toHex(char* str) const {
            str[0] = Ctx::map16((dba >> 28) & 0x0F);
            str[1] = Ctx::map16((dba >> 24) & 0x0F);
            str[2] = Ctx::map16((dba >> 20) & 0x0F);
            str[3] = Ctx::map16((dba >> 16) & 0x0F);
            str[4] = Ctx::map16((dba >> 12) & 0x0F);
            str[5] = Ctx::map16((dba >> 8) & 0x0F);
            str[6] = Ctx::map16((dba >> 4) & 0x0F);
            str[7] = Ctx::map16((dba) & 0x0F);
            str[8] = '.';
            str[9] = Ctx::map16((dataObj >> 12) & 0x0F);
            str[10] = Ctx::map16((dataObj >> 8) & 0x0F);
            str[11] = Ctx::map16((dataObj >> 4) & 0x0F);
            str[12] = Ctx::map16((dataObj) & 0x0F);
            str[13] = '.';
            str[14] = Ctx::map16((slot >> 12) & 0x0F);
            str[15] = Ctx::map16((slot >> 8) & 0x0F);
            str[16] = Ctx::map16((slot >> 4) & 0x0F);
            str[17] = Ctx::map16((slot) & 0x0F);
            str[18] = 0;
        }

        void toString(char* str) const {
            typeAfn afn = static_cast<typeAfn>(dba >> 22);
            typeDba bdba = dba & 0x003FFFFF;

            str[0] = Ctx::map64[(dataObj >> 30) & 0x3F];
            str[1] = Ctx::map64[(dataObj >> 24) & 0x3F];
            str[2] = Ctx::map64[(dataObj >> 18) & 0x3F];
            str[3] = Ctx::map64[(dataObj >> 12) & 0x3F];
            str[4] = Ctx::map64[(dataObj >> 6) & 0x3F];
            str[5] = Ctx::map64[dataObj & 0x3F];
            str[6] = Ctx::map64[(afn >> 12) & 0x3F];
            str[7] = Ctx::map64[(afn >> 6) & 0x3F];
            str[8] = Ctx::map64[afn & 0x3F];
            str[9] = Ctx::map64[(bdba >> 30) & 0x3F];
            str[10] = Ctx::map64[(bdba >> 24) & 0x3F];
            str[11] = Ctx::map64[(bdba >> 18) & 0x3F];
            str[12] = Ctx::map64[(bdba >> 12) & 0x3F];
            str[13] = Ctx::map64[(bdba >> 6) & 0x3F];
            str[14] = Ctx::map64[bdba & 0x3F];
            str[15] = Ctx::map64[(slot >> 12) & 0x3F];
            str[16] = Ctx::map64[(slot >> 6) & 0x3F];
            str[17] = Ctx::map64[slot & 0x3F];
            str[18] = 0;
        }

        std::string toString() const {
            char str[19];
            typeAfn afn = static_cast<typeAfn>(dba >> 22);
            typeDba bdba = dba & 0x003FFFFF;

            str[0] = Ctx::map64[(dataObj >> 30) & 0x3F];
            str[1] = Ctx::map64[(dataObj >> 24) & 0x3F];
            str[2] = Ctx::map64[(dataObj >> 18) & 0x3F];
            str[3] = Ctx::map64[(dataObj >> 12) & 0x3F];
            str[4] = Ctx::map64[(dataObj >> 6) & 0x3F];
            str[5] = Ctx::map64[dataObj & 0x3F];
            str[6] = Ctx::map64[(afn >> 12) & 0x3F];
            str[7] = Ctx::map64[(afn >> 6) & 0x3F];
            str[8] = Ctx::map64[afn & 0x3F];
            str[9] = Ctx::map64[(bdba >> 30) & 0x3F];
            str[10] = Ctx::map64[(bdba >> 24) & 0x3F];
            str[11] = Ctx::map64[(bdba >> 18) & 0x3F];
            str[12] = Ctx::map64[(bdba >> 12) & 0x3F];
            str[13] = Ctx::map64[(bdba >> 6) & 0x3F];
            str[14] = Ctx::map64[bdba & 0x3F];
            str[15] = Ctx::map64[(slot >> 12) & 0x3F];
            str[16] = Ctx::map64[(slot >> 6) & 0x3F];
            str[17] = Ctx::map64[slot & 0x3F];
            str[18] = 0;
            return std::string(str);
        }

        friend std::ostream& operator<<(std::ostream& os, const typeRowId other) {
            char str[19];
            other.toString(str);
            os << str;
            return os;
        }
    };
}

namespace std {
    template<>
    struct hash<OpenLogReplicator::typeRowId> {
        size_t operator()(const OpenLogReplicator::typeRowId other) const {
            return hash<typeDataObj>()(other.dataObj) ^
                   hash<typeDba>()(other.dba) ^
                   hash<typeSlot>()(other.slot);
        }
    };
}

#endif
