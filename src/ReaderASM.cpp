/* Base class for reading redo from ASM
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

#include "DatabaseStatement.h"
#include "OracleAnalyzerOnlineASM.h"
#include "ReaderASM.h"
#include "RuntimeException.h"

using namespace std;

namespace OpenLogReplicator {

    const char* ReaderASM::SQL_ASM_CLOSE("BEGIN dbms_diskgroup.close(:i); END;");
    const char* ReaderASM::SQL_ASM_GETFILEATR("BEGIN dbms_diskgroup.getfileattr(:i, :j, :k, :l); END;");
    const char* ReaderASM::SQL_ASM_OPEN("BEGIN dbms_diskgroup.open(:i, 'r', :j, :k, :l, :m, :n); END;");
    const char* ReaderASM::SQL_ASM_READ("BEGIN dbms_diskgroup.read(:i, :j, :k, :l); END;");

    ReaderASM::ReaderASM(const char *alias, OracleAnalyzer *oracleAnalyzer, uint64_t group) :
        Reader(alias, oracleAnalyzer, group),
        fileDes(-1),
        fileType(0),
        physicalBlockSize(0),
        stmtRead(nullptr) {
    }

    ReaderASM::~ReaderASM() {
        redoClose();
    }

    void ReaderASM::redoClose(void) {
        if (stmtRead != nullptr) {
            delete stmtRead;
            stmtRead = nullptr;
        }

        if (fileDes == -1)
            return;

        try {
            DatabaseStatement stmt(((OracleAnalyzerOnlineASM*)oracleAnalyzer)->connASM);
            TRACE(TRACE2_SQL, "SQL: " << SQL_ASM_CLOSE << endl <<
                    "PARAM1: " << fileDes);
            stmt.createStatement(SQL_ASM_CLOSE);
            stmt.bindInt32(1, fileDes);
            stmt.executeQuery();
            fileDes = -1;
        } catch (RuntimeException &ex) {
        }
    }

    uint64_t ReaderASM::redoOpen(void) {
        uint64_t fileType = -1;

        try {
            blockSize = 0;
            DatabaseStatement stmt(((OracleAnalyzerOnlineASM*)oracleAnalyzer)->connASM);
            TRACE(TRACE2_SQL, "SQL: " << SQL_ASM_GETFILEATR << endl <<
                    "PARAM1: " << pathMapped << endl <<
                    "PARAM2: " << fileType << endl <<
                    "PARAM3: " << fileSize << endl <<
                    "PARAM4: " << blockSize);
            stmt.createStatement(SQL_ASM_GETFILEATR);
            stmt.bindString(1, pathMapped);
            stmt.bindUInt64(2, fileType);
            stmt.bindUInt64(3, fileSize);
            stmt.bindUInt64(4, blockSize);
            stmt.executeQuery();

            physicalBlockSize = -1;
            TRACE(TRACE2_SQL, "SQL: " << SQL_ASM_OPEN << endl <<
                    "PARAM1: " << pathMapped << endl <<
                    "PARAM2: " << fileType << endl <<
                    "PARAM3: " << blockSize << endl <<
                    "PARAM4: " << fileDes << endl <<
                    "PARAM5: " << physicalBlockSize << endl <<
                    "PARAM6: " << fileSize);
            stmt.createStatement(SQL_ASM_OPEN);
            stmt.bindString(1, pathMapped);
            stmt.bindUInt64(2, fileType);
            stmt.bindUInt64(3, blockSize);
            stmt.bindInt32(4, fileDes);
            stmt.bindUInt64(5, physicalBlockSize);
            stmt.bindUInt64(6, fileSize);
            stmt.executeQuery();

            fileSize *= blockSize;
        } catch (RuntimeException &ex) {
            return REDO_ERROR;
        }

        return REDO_OK;
    }

    int64_t ReaderASM::redoRead(uint8_t *buf, uint64_t pos, uint64_t size) {
        if (fileDes == -1)
            return -1;

        pos /= blockSize;
        try {
            if (stmtRead == nullptr) {
                stmtRead = new DatabaseStatement(((OracleAnalyzerOnlineASM*)oracleAnalyzer)->connASM);
                TRACE(TRACE2_SQL, "SQL: " << SQL_ASM_READ << endl <<
                        "PARAM1: " << fileDes << endl <<
                        "PARAM2: " << pos << endl <<
                        "PARAM3: " << size);
                stmtRead->createStatement(SQL_ASM_READ);
            } else
                stmtRead->unbindAll();

            stmtRead->bindInt32(1, fileDes);
            stmtRead->bindUInt64(2, pos);
            stmtRead->bindUInt64(3, size);
            stmtRead->bindBinary(4, buf, 512);
            stmtRead->executeQuery();
        } catch (RuntimeException &ex) {
            return -1;
        }

        return size;
    }

    uint64_t ReaderASM::readSize(uint64_t lastRead) {
        return blockSize;
    }

    uint64_t ReaderASM::reloadHeaderRead(void) {
        int64_t bytes = redoRead(headerBuffer + blockSize, blockSize, blockSize);
        if (bytes != blockSize) {
            ERROR("unable to read file " << pathMapped);
            return REDO_ERROR;
        }
        return REDO_OK;
    }

}
