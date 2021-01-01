/* Header for ReaderASM class
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "Reader.h"

#ifndef READERASM_H_
#define READERASM_H_

using namespace std;

namespace OpenLogReplicator {

    class DatabaseStatement;
    class OracleAnalyzer;

    class ReaderASM : public Reader {
    protected:
        static const char* SQL_ASM_CLOSE;
        static const char* SQL_ASM_GETFILEATR;
        static const char* SQL_ASM_OPEN;
        static const char* SQL_ASM_READ;

        int32_t fileDes;
        uint64_t fileType;
        uint64_t physicalBlockSize;
        DatabaseStatement *stmtRead;

        virtual void redoClose(void);
        virtual uint64_t redoOpen(void);
        virtual int64_t redoRead(uint8_t *buf, uint64_t pos, uint64_t size);

    public:
        ReaderASM(const char *alias, OracleAnalyzer *oracleAnalyzer, uint64_t group);
        virtual ~ReaderASM();
    };
}

#endif
