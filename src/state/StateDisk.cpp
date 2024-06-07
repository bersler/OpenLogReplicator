/* Base class for state in files on disk
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>

#include "../common/Ctx.h"
#include "../common/exception/RuntimeException.h"
#include "StateDisk.h"

namespace OpenLogReplicator {
    StateDisk::StateDisk(Ctx* newCtx, const char* newPath) :
            State(newCtx),
            path(newPath) {
    }

    StateDisk::~StateDisk() {
    }

    void StateDisk::list(std::set<std::string>& namesList) const {
        DIR* dir;
        if ((dir = opendir(path.c_str())) == nullptr)
            throw RuntimeException(10012, "directory: " + path + " - can't read");

        struct dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;

            struct stat fileStat;
            std::string fileName(ent->d_name);

            std::string fullName(path + "/" + ent->d_name);
            if (stat(fullName.c_str(), &fileStat) != 0) {
                ctx->warning(60034, "file: " + fileName + " - stat returned: " + strerror(errno));
                continue;
            }

            if (S_ISDIR(fileStat.st_mode))
                continue;

            std::string suffix(".json");
            if (fileName.length() < suffix.length() || fileName.substr(fileName.length() - suffix.length(), fileName.length()) != suffix)
                continue;

            std::string fileBase(fileName.substr(0, fileName.length() - suffix.length()));
            namesList.insert(fileBase);
        }
        closedir(dir);
    }

    bool StateDisk::read(const std::string& name, uint64_t maxSize, std::string& in) {
        std::string fileName(path + "/" + name + ".json");
        struct stat fileStat;
        if (stat(fileName.c_str(), &fileStat) != 0) {
            ctx->warning(60034, "file: " + fileName + " - stat returned: " + strerror(errno));
            return false;
        }
        if (static_cast<uint64_t>(fileStat.st_size) > maxSize || fileStat.st_size == 0)
            throw RuntimeException(10004, "file: " + fileName + " - wrong size: " + std::to_string(fileStat.st_size));

        std::ifstream inputStream;
        inputStream.open(fileName.c_str(), std::ios::in);

        if (!inputStream.is_open())
            throw RuntimeException(10001, "file: " + fileName + " - open returned: " + strerror(errno));

        in.assign((std::istreambuf_iterator<char>(inputStream)), std::istreambuf_iterator<char>());
        inputStream.close();
        return true;
    }

    void StateDisk::write(const std::string& name, typeScn scn __attribute__((unused)), const std::ostringstream& out) {
        std::string fileName(path + "/" + name + ".json");
        std::ofstream outputStream;

        outputStream.open(fileName.c_str(), std::ios::out | std::ios::trunc);
        if (!outputStream.is_open())
            throw RuntimeException(10006, "file: " + fileName + " - open for write returned: " + strerror(errno));

        outputStream << out.str();
        if (outputStream.bad() || outputStream.fail())
            throw RuntimeException(10007, "file: " + fileName + " - 0 bytes written instead of " +
                                          std::to_string(out.str().length()) + ", code returned: " + strerror(errno));

        outputStream.close();
    }

    void StateDisk::drop(const std::string& name) {
        std::string fileName(path + "/" + name + ".json");
        if (unlink(fileName.c_str()) != 0)
            throw RuntimeException(10010, "file: " + fileName + " - unlink returned: " + strerror(errno));
    }
}
