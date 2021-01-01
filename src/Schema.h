/* Header for Schema class
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

#include <unordered_map>
#include <vector>

#include "types.h"

#ifndef SCHEMA_H_
#define SCHEMA_H_

using namespace std;

namespace OpenLogReplicator {
    class OracleAnalyzer;
    class OracleObject;
    class SchemaElement;

    class Schema {
    protected:
        stringstream& writeEscapeValue(stringstream &ss, string &str);
        unordered_map<typeOBJ, OracleObject*> objectMap;
        unordered_map<typeOBJ, OracleObject*> partitionMap;

    public:
        OracleObject *object;
        vector<SchemaElement*> elements;

        Schema();
        virtual ~Schema();

        bool readSchema(OracleAnalyzer *oracleAnalyzer);
        void writeSchema(OracleAnalyzer *oracleAnalyzer);
        OracleObject *checkDict(typeOBJ obj, typeDATAOBJ dataObj);
        void addToDict(OracleObject *object);
        SchemaElement* addElement(void);
    };
}

#endif
