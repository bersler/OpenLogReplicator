/* Header for ReaderFilesystem class
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

#include "Reader.h"

#ifndef READER_FILESYSTEM_H_
#define READER_FILESYSTEM_H_

namespace OpenLogReplicator {
    class ReaderFilesystem : public Reader {
    protected:
        int fileDes;
        int flags;
        void redoClose() override;
        uint64_t redoOpen() override;
        int64_t redoRead(uint8_t* buf, uint64_t offset, uint64_t size) override;

    public:
        ReaderFilesystem(Ctx* newCtx, const std::string newAlias, const std::string& newDatabase, int64_t newGroup, bool newConfiguredBlockSum);
        ~ReaderFilesystem() override;
    };
}

#endif
