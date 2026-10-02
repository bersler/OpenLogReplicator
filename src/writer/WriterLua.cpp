/* Thread passing messages to a Lua script
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

#include "../builder/Builder.h"
#include "../common/exception/ConfigurationException.h"
#include "../common/exception/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "WriterLua.h"

namespace OpenLogReplicator {
    WriterLua::WriterLua(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata, std::string newScript,
                         std::string newFunction):
            Writer(newCtx, std::move(newAlias), std::move(newDatabase), newBuilder, newMetadata),
            script(std::move(newScript)),
            function(std::move(newFunction)) {}

    WriterLua::~WriterLua() {
        if (state != nullptr)
            lua_close(state);
        state = nullptr;
    }

    void WriterLua::initialize() {
        Writer::initialize();

        state = luaL_newstate();
        if (state == nullptr)
            throw RuntimeException(10073, "Lua failed to create state");
        luaL_openlibs(state);

        if (luaL_loadfile(state, script.c_str()) != 0 || lua_pcall(state, 0, 0, 0) != 0)
            throw RuntimeException(10074, "Lua failed to load script: " + script + ", message: " + popError());

        lua_getglobal(state, function.c_str());
        const bool isFunction = lua_isfunction(state, -1);
        lua_pop(state, 1);
        if (!isFunction)
            throw ConfigurationException(30011, "Lua script: " + script + " does not define function: " + function);

        streaming = true;
    }

    std::string WriterLua::popError() {
        const char* message = lua_tostring(state, -1);
        std::string error(message != nullptr ? message : "unknown error");
        lua_pop(state, 1);
        return error;
    }

    void WriterLua::sendMessage(BuilderMsg* msg) {
        lua_getglobal(state, function.c_str());
        lua_pushlstring(state, reinterpret_cast<const char*>(msg->data + msg->tagSize), msg->size - msg->tagSize);

        if (lua_pcall(state, 1, 0, 0) != 0)
            throw RuntimeException(10075, "Lua function: " + function + " failed for message: " + std::to_string(msg->id) + ", message: " +
                                   popError());

        confirmMessage(msg);
    }

    std::string WriterLua::getType() const {
        return "Lua:" + script;
    }

    void WriterLua::pollQueue() {
        if (metadata->status == Metadata::STATUS::READY)
            metadata->setStatusStarting(this);
    }
}
