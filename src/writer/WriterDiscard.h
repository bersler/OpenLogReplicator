/* Header for WriterDiscard class
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

#ifndef WRITER_DISCARD_H_
#define WRITER_DISCARD_H_

#include "Writer.h"

namespace OpenLogReplicator {
    class WriterDiscard final : public Writer {
    protected:
        void sendMessage(BuilderMsg* msg) override;
        std::string getType() const override;
        void pollQueue() override;

    public:
        WriterDiscard(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata);
        ~WriterDiscard() override = default;

        void initialize() override;
    };
}

#endif
