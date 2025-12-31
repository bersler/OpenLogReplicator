/* Header for SerializerJson class
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

#ifndef SERIALIZER_JSON_H_
#define SERIALIZER_JSON_H_

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "Serializer.h"

#define SERIALIZER_ENDL     <<'\n'

namespace OpenLogReplicator {
    class Ctx;
    class XmlCtx;

    class SerializerJson final : public Serializer {
    protected:
        static void deserializeSysCCol(const Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysCColJson);
        static void deserializeSysCDef(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysCDefJson);
        static void deserializeSysCol(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysColJson);
        static void deserializeSysDeferredStg(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysDeferredStgJson);
        static void deserializeSysECol(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysEColJson);
        static void deserializeSysLob(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysLobJson);
        static void deserializeSysLobCompPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysLobCompPartJson);
        static void deserializeSysLobFrag(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysLobFragJson);
        static void deserializeSysObj(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysObjJson);
        static void deserializeSysTab(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabJson);
        static void deserializeSysTabComPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabComPartJson);
        static void deserializeSysTabPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabPartJson);
        static void deserializeSysTabSubPart(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTabSubPartJson);
        static void deserializeSysTs(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysTsJson);
        static void deserializeSysUser(Metadata* metadata, const std::string& fileName, const rapidjson::Value& sysUserJson);
        static void deserializeXdbTtSet(Metadata* metadata, const std::string& fileName, const rapidjson::Value& xdbTtSetJson);
        static void deserializeXdbXNm(Metadata* metadata, XmlCtx* xmlCtx, const std::string& fileName, const rapidjson::Value& xdbXNmJson);
        static void deserializeXdbXPt(Metadata* metadata, XmlCtx* xmlCtx, const std::string& fileName, const rapidjson::Value& xdbXPtJson);
        static void deserializeXdbXQn(Metadata* metadata, XmlCtx* xmlCtx, const std::string& fileName, const rapidjson::Value& xdbXQnJson);

    public:
        SerializerJson() = default;
        ~SerializerJson() override = default;
        SerializerJson(const SerializerJson&) = delete;
        SerializerJson& operator=(const SerializerJson&) = delete;

        [[nodiscard]] bool deserialize(Metadata* metadata, const std::string& ss, const std::string& fileName, std::vector<std::string>& msgs,
                                       std::unordered_map<typeObj, std::string>& tablesUpdated, bool loadMetadata, bool loadSchema) override;
        void serialize(Metadata* metadata, std::ostringstream& ss, bool storeSchema) override;
    };
}

#endif
