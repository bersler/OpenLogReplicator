/* Header for SerializerJson class
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

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <set>

#include "Serializer.h"

#ifndef SERIALIZER_JSON_H_
#define SERIALIZER_JSON_H_

#define SERIALIZER_ENDL     <<std::endl

namespace OpenLogReplicator {
    class Ctx;

    class SerializerJson: public Serializer {
    protected:
        void deserializeSysCCol(Metadata* metadata, const std::string& name, const rapidjson::Value& sysCColJson);
        void deserializeSysCDef(Metadata* metadata, const std::string& name, const rapidjson::Value& sysCDefJson);
        void deserializeSysCol(Metadata* metadata, const std::string& name, const rapidjson::Value& sysColJson);
        void deserializeSysDeferredStg(Metadata* metadata, const std::string& name, const rapidjson::Value& sysCDefJson);
        void deserializeSysECol(Metadata* metadata, const std::string& name, const rapidjson::Value& sysEColJson);
        void deserializeSysLob(Metadata* metadata, const std::string& name, const rapidjson::Value& sysLobJson);
        void deserializeSysLobCompPart(Metadata* metadata, const std::string& name, const rapidjson::Value& sysLobCompPartJson);
        void deserializeSysLobFrag(Metadata* metadata, const std::string& name, const rapidjson::Value& sysLobFragJson);
        void deserializeSysObj(Metadata* metadata, const std::string& name, const rapidjson::Value& sysObjJson);
        void deserializeSysTab(Metadata* metadata, const std::string& name, const rapidjson::Value& sysTabJson);
        void deserializeSysTabComPart(Metadata* metadata, const std::string& name, const rapidjson::Value& sysTabComPartJson);
        void deserializeSysTabPart(Metadata* metadata, const std::string& name, const rapidjson::Value& sysTabPartJson);
        void deserializeSysTabSubPart(Metadata* metadata, const std::string& name, const rapidjson::Value& sysTabSubPartJson);
        void deserializeSysTs(Metadata* metadata, const std::string& name, const rapidjson::Value& sysUserJson);
        void deserializeSysUser(Metadata* metadata, const std::string& name, const rapidjson::Value& sysUserJson);

    public:
        SerializerJson();
        ~SerializerJson() override;

        [[nodiscard]] bool deserialize(Metadata* metadata, const std::string& ss, const std::string& name, std::set<std::string>& msgs, bool loadMetadata,
                                       bool storeSchema) override;
        void serialize(Metadata* metadata, std::ostringstream& ss, bool noSchema) override;
    };
}

#endif
