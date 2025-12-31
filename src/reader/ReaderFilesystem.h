/* Header for ReaderFilesystem class
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

#ifndef READER_FILESYSTEM_H_
#define READER_FILESYSTEM_H_

#include "Reader.h"

namespace OpenLogReplicator {
    class ReaderFilesystem final : public Reader {
    protected:
        int fileDes{-1};
        int flags{0};
        void redoClose() override;
        REDO_CODE redoOpen() override;
        int redoRead(uint8_t* buf, uint64_t offset, uint size) override;

    public:
        ReaderFilesystem(Ctx* newCtx, std::string newAlias, std::string newDatabase, int newGroup, bool newConfiguredBlockSum);
        ~ReaderFilesystem() override;

        void showHint(Thread* t, std::string origPath, std::string mappedPath) const override;
    };
}

#endif
