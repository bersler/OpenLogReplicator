/* Definition of types and macros
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

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "../../config.h"

#ifndef TYPES_H_
#define TYPES_H_

typedef uint32_t typeResetlogs;
typedef uint32_t typeActivation;
typedef uint16_t typeSum;
typedef uint16_t typeOp1;
typedef uint32_t typeOp2;
typedef int16_t typeConId;
typedef uint64_t typeUba;
typedef uint32_t typeSeq;
typedef uint64_t typeScn;
typedef uint16_t typeSubScn;
typedef uint64_t typeIdx;
typedef uint16_t typeSlt;
typedef uint32_t typeSqn;
typedef uint8_t typeRci;
typedef int16_t typeUsn;
typedef uint64_t typeXidMap;
typedef uint16_t typeAfn;
typedef uint32_t typeDba;
typedef uint16_t typeSlot;
typedef uint32_t typeBlk;
typedef uint32_t typeObj;
typedef uint32_t typeDataObj;
typedef uint64_t typeObj2;
typedef int16_t typeCol;
typedef uint16_t typeType;
typedef uint32_t typeCon;
typedef uint32_t typeTs;
typedef uint32_t typeUser;
typedef uint8_t typeOptions;
typedef uint16_t typeField;
typedef uint16_t typePos;
typedef uint16_t typeSize;
typedef uint8_t typeCC;
typedef uint16_t typeCCExt;

typedef uint16_t typeUnicode16;
typedef uint32_t typeUnicode32;
typedef uint64_t typeUnicode;
typedef int64_t time_ut;

#define likely(x)                               __builtin_expect(!!(x),1)
#define unlikely(x)                             __builtin_expect(!!(x),0)

#define BLOCK(__uba)                            (static_cast<uint32_t>((__uba)&0xFFFFFFFF))
#define SEQUENCE(__uba)                         (static_cast<uint16_t>(((static_cast<uint64_t>(__uba))>>32)&0xFFFF))
#define RECORD(__uba)                           (static_cast<uint8_t>(((static_cast<uint64_t>(__uba))>>48)&0xFF))
#define PRINTUBA(__uba)                         "0x"<<std::setfill('0')<<std::setw(8)<<std::hex<<BLOCK(__uba)<<"."<<std::setfill('0')<<std::setw(4)<<std::hex<<SEQUENCE(__uba)<<"."<<std::setfill('0')<<std::setw(2)<<std::hex<<static_cast<uint32_t>RECORD(__uba)

#define SCN(__scn1, __scn2)                      (((static_cast<uint64_t>(__scn1))<<32)|(__scn2))
#define PRINTSCN48(__scn)                       "0x"<<std::setfill('0')<<std::setw(4)<<std::hex<<(static_cast<uint32_t>((__scn)>>32)&0xFFFF)<<"."<<std::setw(8)<<((__scn)&0xFFFFFFFF)
#define PRINTSCN64(__scn)                       "0x"<<std::setfill('0')<<std::setw(16)<<std::hex<<(__scn)
#define PRINTSCN64D(__scn)                      "0x"<<std::setfill('0')<<std::setw(4)<<std::hex<<(static_cast<uint32_t>((__scn)>>48)&0xFFFF)<<"."<<std::setw(4)<<(static_cast<uint32_t>((__scn)>>32)&0xFFFF)<<"."<<std::setw(8)<<((__scn)&0xFFFFFFFF)

#endif
