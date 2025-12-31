/* Header for WriterStream class
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

#ifndef WRITER_STREAM_H_
#define WRITER_STREAM_H_

#include "Writer.h"
#include "../common/OraProtoBuf.pb.h"

namespace OpenLogReplicator {
    class Stream;

    class WriterStream final : public Writer {
    protected:
        Stream* stream;
        pb::RedoRequest request;
        pb::RedoResponse response;

        std::string getType() const override;
        void processInfo();
        void processStart();
        void processContinue();
        void processConfirm();
        void pollQueue() override;
        void sendMessage(BuilderMsg* msg) override;

    public:
        WriterStream(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata, Stream* newStream);
        ~WriterStream() override;

        void initialize() override;
    };
}

#endif
