/* Header for WriterLua class
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

This program is free software: you can redistribute it and/or
modify it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public
License along with this program; see the file LICENSE;
If not, see <http://www.gnu.org/licenses/>. */

#ifndef WRITER_LUA_H_
#define WRITER_LUA_H_

#include <lua.hpp>

#include "Writer.h"

namespace OpenLogReplicator {
    class WriterLua final : public Writer {
    protected:
        std::string script;
        std::string function;
        lua_State* state{nullptr};

        std::string popError();
        void sendMessage(BuilderMsg* msg) override;
        std::string getType() const override;
        void pollQueue() override;

    public:
        WriterLua(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata, std::string newScript,
                  std::string newFunction);
        ~WriterLua() override;

        void initialize() override;
    };
}

#endif
