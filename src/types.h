/* Definition of types and macros
   Copyright (C) 2018-2020 Adam Leszczynski.

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <ostream>
#include <iomanip>
#include <stdint.h>

#ifndef TYPES_H_
#define TYPES_H_

using namespace std;

typedef uint64_t typeuba;
typedef uint64_t typexid;
typedef uint64_t typescn;
typedef uint32_t typeseq;
#define ZERO_SCN ((typescn)0xFFFFFFFFFFFFFFFF)

#define DISK_BUFFER_SIZE (16*1024*1024)
#define REDO_RECORD_MAX_SIZE 1048576
#define REDO_PAGE_SIZE_MIN 512
#define REDO_PAGE_SIZE_MAX 1024
#define READ_CHUNK_MIN_SIZE 8192

#define REDO_OK                      0
#define REDO_WRONG_SEQUENCE          1
#define REDO_WRONG_SEQUENCE_SWITCHED 2
#define REDO_ERROR                   3
#define REDO_EMPTY                   4

#define REDO_SLEEP_RETRY             10000

#define USN(xid) ((uint16_t)(((uint64_t)xid)>>48))
#define SLT(xid) ((uint16_t)(((((uint64_t)xid)>>32)&0xFFFF)))
#define SQN(xid) ((uint32_t)(((xid)&0xFFFFFFFF)))
#define XID(usn,slt,sqn) ((((uint64_t)(usn))<<48)|(((uint64_t)(slt))<<32)|((uint64_t)(sqn)))
#define PRINTXID(xid) "0x"<<setfill('0')<<setw(4)<<hex<<USN(xid)<<"."<<setw(3)<<SLT(xid)<<"."<<setw(8)<<SQN(xid)

#define BLOCK(uba) ((uint32_t)((uba)&0xFFFFFFFF))
#define SEQUENCE(uba) ((uint16_t)((((uint64_t)uba)>>32)&0xFFFF))
#define RECORD(uba) ((uint8_t)((((uint64_t)uba)>>48)&0xFF))
#define PRINTUBA(uba) "0x"<<setfill('0')<<setw(8)<<hex<<BLOCK(uba)<<"."<<setfill('0')<<setw(4)<<hex<<SEQUENCE(uba)<<"."<<setfill('0')<<setw(2)<<hex<<(uint32_t)RECORD(uba)

#define SCN(scn1,scn2) ((((uint64_t)scn1)<<32)|(scn2))
#define PRINTSCN48(scn) "0x"<<setfill('0')<<setw(4)<<hex<<((uint32_t)((scn)>>32)&0xFFFF)<<"."<<setw(8)<<((scn)&0xFFFFFFFF)
#define PRINTSCN64(scn) "0x"<<setfill('0')<<setw(16)<<hex<<(scn)

namespace OpenLogReplicator {

    class typetime {
        uint32_t val;
    public:
        typetime() {
            this->val = 0;
        }

        typetime(uint32_t val) {
            this->val = val;
        }

        uint32_t getVal(void) {
            return this->val;
        }

        typetime& operator= (uint32_t val) {
            this->val = val;
            return *this;
        }

        friend ostream& operator<<(ostream& os, const typetime& time) {
            uint32_t rest = time.val;
            uint32_t ss = rest % 60; rest /= 60;
            uint32_t mi = rest % 60; rest /= 60;
            uint32_t hh = rest % 24; rest /= 24;
            uint32_t dd = (rest % 31) + 1; rest /= 31;
            uint32_t mm = (rest % 12) + 1; rest /= 12;
            uint32_t yy = rest + 1988;
            os << setfill('0') << setw(2) << dec << mm << "/" << setfill('0') << setw(2) << dec << dd << "/" << yy << " " <<
                    setfill('0') << setw(2) << dec << hh << ":" << setfill('0') << setw(2) << dec << mi << ":" << setfill('0') << setw(2) << dec << ss;
            return os;
            //DDDDDDDDDD HHHHHHHH
            //10/15/2018 22:25:36

        }
    };
}

#define CHECKPOINT_SIZE 12

#endif
