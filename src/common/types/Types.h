/* Definition of types and macros
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

#ifndef TYPES_H_
#define TYPES_H_

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "../../../config.h"

using typeResetlogs = uint32_t;
using typeActivation = uint32_t;
using typeSum = uint16_t;
using typeOp1 = uint16_t;
using typeOp2 = uint32_t;
using typeDbId = uint32_t;
using typeConId = int16_t;
using typeUba = uint64_t;
using typeSubScn = uint16_t;
using typeIdx = uint64_t;
using typeSlt = uint16_t;
using typeSqn = uint32_t;
using typeRci = uint8_t;
using typeUsn = int16_t;
using XidMap = uint64_t;
using typeAfn = uint16_t;
using typeDba = uint32_t;
using typeSlot = uint16_t;
using typeBlk = uint32_t;
using typeObj = uint32_t;
using typeDataObj = uint32_t;
using typeObj2 = uint64_t;
using typeCol = int16_t;
using typeCon = uint32_t;
using typeTs = uint32_t;
using typeUser = uint32_t;
using typeField = uint16_t;
using typePos = uint16_t;
using typeSize = uint16_t;
using typeCC = uint8_t;
using typeCCExt = uint16_t;
using typeLwn = uint16_t;
using typeTransactionSize = uint64_t;
using typeChunkSize = uint32_t;
using typeTag = uint32_t;

using typeUnicode16 = uint16_t;
using typeUnicode32 = uint32_t;
using typeUnicode = uint64_t;
using time_ut = int64_t;
using typeMask = uint64_t;
using uint = unsigned int;

#define likely(x)                               __builtin_expect(!!(x),1)
#define unlikely(x)                             __builtin_expect(!!(x),0)

#define BLOCK(__uba)                            (static_cast<uint32_t>((__uba)&0xFFFFFFFF))
#define SEQUENCE(__uba)                         (static_cast<uint16_t>(((static_cast<uint64_t>(__uba))>>32)&0xFFFF))
#define RECORD(__uba)                           (static_cast<uint8_t>(((static_cast<uint64_t>(__uba))>>48)&0xFF))
#define PRINTUBA(__uba)                         "0x"<<std::setfill('0')<<std::setw(8)<<std::hex<<BLOCK(__uba)<<"."<<std::setfill('0')<<std::setw(4)<<std::hex<<SEQUENCE(__uba)<<"."<<std::setfill('0')<<std::setw(2)<<std::hex<<static_cast<uint32_t>RECORD(__uba)

#endif
