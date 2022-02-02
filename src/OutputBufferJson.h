/* Header for OutputBufferJson class
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "OracleObject.h"
#include "OutputBuffer.h"

#ifndef OUTPUTBUFFERJSON_H_
#define OUTPUTBUFFERJSON_H_

namespace OpenLogReplicator {
    class OutputBufferJson : public OutputBuffer {
    protected:
        bool hasPreviousValue;
        bool hasPreviousRedo;
        bool hasPreviousColumn;
        virtual void columnNull(OracleObject* object, typeCOL col);
        virtual void columnFloat(std::string& columnName, float value);
        virtual void columnDouble(std::string& columnName, double value);
        virtual void columnString(std::string& columnName);
        virtual void columnNumber(std::string& columnName, uint64_t precision, uint64_t scale);
        virtual void columnRaw(std::string& columnName, const uint8_t* data, uint64_t length);
        virtual void columnTimestamp(std::string& columnName, struct tm& epochtime, uint64_t fraction, const char* tz);
        virtual void appendRowid(typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot);
        virtual void appendHeader(bool first, bool showXid);
        virtual void appendSchema(OracleObject* object, typeDATAOBJ dataObj);

        void appendHex(uint64_t value, uint64_t length) {
            uint64_t j = (length - 1) * 4;
            for (uint64_t i = 0; i < length; ++i) {
                outputBufferAppend(map16[(value >> j) & 0xF]);
                j -= 4;
            };
        }

        void appendDec(uint64_t value, uint64_t length) {
            char buffer[21];

            for (uint64_t i = 0; i < length; ++i) {
                buffer[i] = '0' + (value % 10);
                value /= 10;
            }

            for (uint64_t i = 0; i < length; ++i)
                outputBufferAppend(buffer[length - i - 1]);
        }

        void appendDec(uint64_t value) {
            char buffer[21];
            uint64_t length = 0;

            if (value == 0) {
                buffer[0] = '0';
                length = 1;
            } else {
                while (value > 0) {
                    buffer[length++] = '0' + (value % 10);
                    value /= 10;
                }
            }

            for (uint64_t i = 0; i < length; ++i)
                outputBufferAppend(buffer[length - i - 1]);
        }

        void appendSDec(int64_t value) {
            char buffer[22];
            uint64_t length = 0;

            if (value == 0) {
                buffer[0] = '0';
                length = 1;
            } else {
                if (value < 0) {
                    value = -value;
                    while (value > 0) {
                        buffer[length++] = '0' + (value % 10);
                        value /= 10;
                    }
                    buffer[length++] = '-';
                } else {
                    while (value > 0) {
                        buffer[length++] = '0' + (value % 10);
                        value /= 10;
                    }
                }
            }

            for (uint64_t i = 0; i < length; ++i)
                outputBufferAppend(buffer[length - i - 1]);
        }

        void appendEscape(const char* str, uint64_t length) {
            while (length > 0) {
                if (*str == '\t') {
                    outputBufferAppend('\\');
                    outputBufferAppend('t');
                } else if (*str == '\r') {
                    outputBufferAppend('\\');
                    outputBufferAppend('r');
                } else if (*str == '\n') {
                    outputBufferAppend('\\');
                    outputBufferAppend('n');
                } else if (*str == '\f') {
                    outputBufferAppend('\\');
                    outputBufferAppend('f');
                } else if (*str == '\b') {
                    outputBufferAppend('\\');
                    outputBufferAppend('b');
                } else if (*str == 0) {
                    outputBufferAppend("\\u0000");
                } else {
                    if (*str == '"' || *str == '\\' || *str == '/')
                        outputBufferAppend('\\');
                    outputBufferAppend(*str);
                }
                ++str;
                --length;
            }
        }

        void appendAfter(OracleObject* object) {
            outputBufferAppend(",\"after\":{");

            hasPreviousColumn = false;
            if (columnFormat > 0 && object != nullptr) {
                for (typeCOL column = 0; column < object->maxSegCol; ++column) {
                    if (values[column][VALUE_AFTER] != nullptr) {
                        if (lengths[column][VALUE_AFTER] > 0)
                            processValue(object, column, values[column][VALUE_AFTER], lengths[column][VALUE_AFTER], compressedAfter);
                        else
                            columnNull(object, column);
                    }
                }
            } else {
                uint64_t baseMax = valuesMax >> 6;
                for (uint64_t base = 0; base <= baseMax; ++base) {
                    typeCOL column = base << 6;
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        if (values[column][VALUE_AFTER] != nullptr) {
                            if (lengths[column][VALUE_AFTER] > 0)
                                processValue(object, column, values[column][VALUE_AFTER], lengths[column][VALUE_AFTER], compressedAfter);
                            else
                                columnNull(object, column);
                        }
                    }
                }
            }
            outputBufferAppend('}');
        }

        void appendBefore(OracleObject* object) {
            outputBufferAppend(",\"before\":{");

            hasPreviousColumn = false;
            if (columnFormat > 0 && object != nullptr) {
                for (typeCOL column = 0; column < object->maxSegCol; ++column) {
                    if (values[column][VALUE_BEFORE] != nullptr) {
                        if (lengths[column][VALUE_BEFORE] > 0)
                            processValue(object, column, values[column][VALUE_BEFORE], lengths[column][VALUE_BEFORE], compressedBefore);
                        else
                            columnNull(object, column);
                    }
                }
            } else {
                uint64_t baseMax = valuesMax >> 6;
                for (uint64_t base = 0; base <= baseMax; ++base) {
                    typeCOL column = base << 6;
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        if (values[column][VALUE_BEFORE] != nullptr) {
                            if (lengths[column][VALUE_BEFORE] > 0)
                                processValue(object, column, values[column][VALUE_BEFORE], lengths[column][VALUE_BEFORE], compressedBefore);
                            else
                                columnNull(object, column);
                        }
                    }
                }
            }
            outputBufferAppend('}');
        }

        time_t tmToEpoch(struct tm* epoch) const;
        virtual void processInsert(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid);
        virtual void processUpdate(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid);
        virtual void processDelete(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid);
        virtual void processDDL(OracleObject* object, typeDATAOBJ dataObj, uint16_t type, uint16_t seq, const char* operation,
                const char* sql, uint64_t sqlLength);
        virtual void processBegin(void);
    public:
        OutputBufferJson(uint64_t messageFormat, uint64_t ridFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat,
                uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat, uint64_t unknownType, uint64_t flushBuffer);
        virtual ~OutputBufferJson();

        virtual void processCommit(void);
        virtual void processCheckpoint(typeSCN scn, typeTIME time_, typeSEQ sequence, uint64_t offset, bool redo);
    };
}

#endif
