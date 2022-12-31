/* Header for OpCode1801 class
   Copyright (C) 2018-2023 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "OpCode.h"

#ifndef OP_CODE_1A_06_H_
#define OP_CODE_1A_06_H_

#define OP266_OP_REDO           0
#define OP266_OP_UNDO           1
#define OP266_OP_CR             2
#define OP266_OP_FRMT           3
#define OP266_OP_INVL           4
#define OP266_OP_LOAD           5
#define OP266_OP_BIMG           6
#define OP266_OP_SINV           7

#define OP266_TYPE_MASK         120
#define OP266_TYPE_NEW          0
#define OP266_TYPE_LOCK         8
#define OP266_TYPE_LHB          16
#define OP266_TYPE_DATA         32
#define OP266_TYPE_BTREE        48
#define OP266_TYPE_ITREE        64
#define OP266_TYPE_AUX          96
#define OP266_TYPE_VER1         128

#define OP266_FLG2_PFILL        8
#define OP266_FLG2_CMAP         16
#define OP266_FLG2_HASH         32
#define OP266_FLG2_LHB          64
#define OP266_FLG2_VER1         128

namespace OpenLogReplicator {
    class OpCode1A06: public OpCode {
    public:
        static void process(Ctx* ctx, RedoLogRecord* redoLogRecord);
    };
}

#endif
