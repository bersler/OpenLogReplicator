/* Header for type typeXid
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
#include <iomanip>
#include <ostream>

#include "DataException.h"
#include "types.h"

#ifndef TYPEXID_H_
#define TYPEXID_H_

//#define USN(__xid)                              ((typeUsn)(((uint64_t)(__xid))>>48))
//#define SLT(__xid)                              ((typeSlt)(((((uint64_t)(__xid))>>32)&0xFFFF)))
//#define SQN(__xid)                              ((typeSqn)(((__xid)&0xFFFFFFFF)))
//#define XID(__usn,__slt,__sqn)                  ((((uint64_t)(__usn))<<48)|(((uint64_t)(__slt))<<32)|((uint64_t)(__sqn)))
//#define PRINTXID(__xid) "0x"<<std::setfill('0')<<std::setw(4)<<std::hex<<USN(__xid)<<"."<<std::setw(3)<<(uint64_t)SLT(__xid)<<"."<<std::setw(8)<<SQN(__xid)

namespace OpenLogReplicator {
    class typeXid {
        uint64_t val;
    public:
        typeXid() : val(0) {
        };

        explicit typeXid(uint64_t val) : val(val) {
        }

        typeXid(typeUsn usn, typeSlt slt, typeSqn sqn) {
            val = (((uint64_t)usn) << 48) | (((uint64_t)slt) << 32) | ((uint64_t)sqn);
        }

        typeXid(const char* str) {
            std::string usn;
            std::string slt;
            std::string sqn;

            uint64_t length = strnlen(str, 25);
            //UUUUSSSSQQQQQQQQ
            if (length == 16) {
                for (uint64_t i = 0; i < 16; ++i)
                    if (!iswxdigit(str[i]))
                        throw DataException(std::string("bad XID value: ") + str);
                usn.assign(str, 4);
                slt.assign(str + 4, 4);
                sqn.assign(str + 8, 8);
            } else
                //UUUU.SSS.QQQQQQQQ
            if (length == 17) {
                for (uint64_t i = 0; i < 17; ++i)
                    if (!iswxdigit(str[i]) && i != 4 && i != 8)
                        throw DataException(std::string("bad XID value: ") + str);
                if (str[4] != '.' || str[8] != '.')
                    throw DataException(std::string("bad XID value: ") + str);
                usn.assign(str, 4);
                slt.assign(str + 5, 3);
                sqn.assign(str + 9, 8);
            } else
                //UUUU.SSSS.QQQQQQQQ
            if (length == 18) {
                for (uint64_t i = 0; i < 18; ++i)
                    if (!iswxdigit(str[i]) && i != 4 && i != 9)
                        throw DataException(std::string("bad XID value: ") + str);
                if (str[4] != '.' || str[9] != '.')
                    throw DataException(std::string("bad XID value: ") + str);
                usn.assign(str, 4);
                slt.assign(str + 5, 4);
                sqn.assign(str + 10, 8);
            } else
                //0xUUUU.SSS.QQQQQQQQ
            if (length == 19) {
                for (uint64_t i = 2; i < 19; ++i)
                    if (!iswxdigit(str[i]) && i != 6 && i != 10)
                        throw DataException(std::string("bad XID value: ") + str);
                if (str[0] != '0' || str[1] != 'x' || str[6] != '.' || str[10] != '.')
                    throw DataException(std::string("bad XID value: ") + str);
                usn.assign(str + 2, 4);
                slt.assign(str + 7, 3);
                sqn.assign(str + 11, 8);
            } else
                //0xUUUU.SSSS.QQQQQQQQ
            if (length == 20) {
                for (uint64_t i = 2; i < 20; ++i)
                    if (!iswxdigit(str[i]) && i != 6 && i != 11)
                        throw DataException(std::string("bad XID value: ") + str);
                if (str[0] != '0' || str[1] != 'x' || str[6] != '.' || str[11] != '.')
                    throw DataException(std::string("bad XID value: ") + str);
                usn.assign(str + 2, 4);
                slt.assign(str + 7, 4);
                sqn.assign(str + 12, 8);
            } else
                throw DataException(std::string("bad XID value: ") + str);

            val = stoul(usn, nullptr, 16), stoul(slt, nullptr, 16), stoul(sqn, nullptr, 16);
        }

        uint64_t getVal() const {
            return val;
        }

        typeUsn usn() const {
            return (typeUsn)(val >> 48);
        }

        typeSlt slt() const {
            return (typeSlt)((val >> 32) & 0xFFFF);
        }

        typeSqn sqn() const {
            return (typeSqn)(val & 0xFFFFFFFF);
        }

        bool operator!=(const typeXid& other) const {
            return val != other.val;
        }

        bool operator<(const typeXid& other) const {
            return val < other.val;
        }

        bool operator== (const typeXid& other) const {
            return val == other.val;
        }

        typeXid& operator=(uint64_t newVal) {
            val = newVal;
            return *this;
        }

        std::string toString() {
            std::stringstream  ss;
            ss << "0x" << std::setfill('0') << std::setw(4) << std::hex << (val >> 48) << "."
               << std::setw(3) << (uint64_t)((val>>32)&0xFFFF) << "."
               << std::setw(8) << (val&0xFFFFFFFF);
            return ss.str();
        }

        friend std::ostream& operator<<(std::ostream& os, const typeXid& xid) {
            os << "0x" << std::setfill('0') << std::setw(4) << std::hex << (xid.val >> 48) << "."
               << std::setw(3) << (uint64_t)((xid.val>>32)&0xFFFF) << "."
               << std::setw(8) << (xid.val&0xFFFFFFFF);
            return os;
        }
    };
}

#endif
