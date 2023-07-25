/* Header for BuilderJson class
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

#include "../common/OracleTable.h"
#include "Builder.h"

#ifndef BUILDER_JSON_H_
#define BUILDER_JSON_H_

namespace OpenLogReplicator {
    class BuilderJson : public Builder {
    protected:
        bool hasPreviousValue;
        bool hasPreviousRedo;
        bool hasPreviousColumn;
        void columnNull(OracleTable* table, typeCol col, bool after);
        void columnFloat(const std::string& columnName, double value) override;
        void columnDouble(const std::string& columnName, long double value) override;
        void columnString(const std::string& columnName) override;
        void columnNumber(const std::string& columnName, uint64_t precision, uint64_t scale) override;
        void columnRaw(const std::string& columnName, const uint8_t* data, uint64_t length) override;
        void columnRowId(const std::string& columnName, typeRowId rowId);
        void columnTimestamp(const std::string& columnName, struct tm& epochtime, uint64_t fraction, const char* tz) override;
        void appendRowid(typeDataObj dataObj, typeDba bdba, typeSlot slot);
        void appendHeader(bool first, bool showXid);
        void appendSchema(OracleTable* table, typeObj obj);

        void appendHex(uint64_t value, uint64_t length) {
            uint64_t j = (length - 1) * 4;
            for (uint64_t i = 0; i < length; ++i) {
                builderAppend(ctx->map16[(value >> j) & 0xF]);
                j -= 4;
            }
        }

        void appendDec(uint64_t value, uint64_t length) {
            char buffer[21];

            for (uint64_t i = 0; i < length; ++i) {
                buffer[i] = ctx->map10[value % 10];
                value /= 10;
            }

            for (uint64_t i = 0; i < length; ++i)
                builderAppend(buffer[length - i - 1]);
        }

        void appendDec(uint64_t value) {
            char buffer[21];
            uint64_t length = 0;

            if (value == 0) {
                buffer[0] = '0';
                length = 1;
            } else {
                while (value > 0) {
                    buffer[length++] = ctx->map10[value % 10];
                    value /= 10;
                }
            }

            for (uint64_t i = 0; i < length; ++i)
                builderAppend(buffer[length - i - 1]);
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
                        buffer[length++] = ctx->map10[value % 10];
                        value /= 10;
                    }
                    buffer[length++] = '-';
                } else {
                    while (value > 0) {
                        buffer[length++] = ctx->map10[value % 10];
                        value /= 10;
                    }
                }
            }

            for (uint64_t i = 0; i < length; ++i)
                builderAppend(buffer[length - i - 1]);
        }

        void appendEscape(const char* str, uint64_t length) {
            while (length > 0) {
                if (*str == '\t') {
                    builderAppend("\\t", sizeof("\\t") - 1);
                } else if (*str == '\r') {
                    builderAppend("\\r", sizeof("\\r") - 1);
                } else if (*str == '\n') {
                    builderAppend("\\n", sizeof("\\n") - 1);
                } else if (*str == '\f') {
                    builderAppend("\\f", sizeof("\\f") - 1);
                } else if (*str == '\b') {
                    builderAppend("\\b", sizeof("\\b") - 1);
                } else if (*str == 0) {
                    builderAppend("\\u0000", sizeof("\\u0000") - 1);
                } else {
                    if (*str == '"' || *str == '\\' || *str == '/')
                        builderAppend('\\');
                    builderAppend(*str);
                }
                ++str;
                --length;
            }
        }

        void appendAfter(LobCtx* lobCtx, OracleTable* table, uint64_t offset) {
            builderAppend(R"(,"after":{)", sizeof(R"(,"after":{)") - 1);

            hasPreviousColumn = false;
            if (columnFormat > 0 && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][VALUE_AFTER] != nullptr) {
                        if (lengths[column][VALUE_AFTER] > 0)
                            processValue(lobCtx, table, column, values[column][VALUE_AFTER], lengths[column][VALUE_AFTER], offset, true,
                                         compressedAfter);
                        else
                            columnNull(table, column, true);
                    }
                }
            } else {
                uint64_t baseMax = valuesMax >> 6;
                for (uint64_t base = 0; base <= baseMax; ++base) {
                    auto column = static_cast<typeCol>(base << 6);
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        if (values[column][VALUE_AFTER] != nullptr) {
                            if (lengths[column][VALUE_AFTER] > 0)
                                processValue(lobCtx, table, column, values[column][VALUE_AFTER], lengths[column][VALUE_AFTER], offset,
                                             true, compressedAfter);
                            else
                                columnNull(table, column, true);
                        }
                    }
                }
            }
            builderAppend('}');
        }

        void appendBefore(LobCtx* lobCtx, OracleTable* table, uint64_t offset) {
            builderAppend(R"(,"before":{)", sizeof(R"(,"before":{)") - 1);

            hasPreviousColumn = false;
            if (columnFormat > 0 && table != nullptr) {
                for (typeCol column = 0; column < table->maxSegCol; ++column) {
                    if (values[column][VALUE_BEFORE] != nullptr) {
                        if (lengths[column][VALUE_BEFORE] > 0)
                            processValue(lobCtx, table, column, values[column][VALUE_BEFORE], lengths[column][VALUE_BEFORE], offset,
                                         false, compressedBefore);
                        else
                            columnNull(table, column, false);
                    }
                }
            } else {
                uint64_t baseMax = valuesMax >> 6;
                for (uint64_t base = 0; base <= baseMax; ++base) {
                    auto column = static_cast<typeCol>(base << 6);
                    for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                        if (valuesSet[base] < mask)
                            break;
                        if ((valuesSet[base] & mask) == 0)
                            continue;

                        if (values[column][VALUE_BEFORE] != nullptr) {
                            if (lengths[column][VALUE_BEFORE] > 0)
                                processValue(lobCtx, table, column, values[column][VALUE_BEFORE], lengths[column][VALUE_BEFORE], offset,
                                             false, compressedBefore);
                            else
                                columnNull(table, column, false);
                        }
                    }
                }
            }
            builderAppend('}');
        }

        static time_t tmToEpoch(struct tm*);
        void processInsert(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid,
                           uint64_t offset) override;
        void processUpdate(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid,
                           uint64_t offset) override;
        void processDelete(LobCtx* lobCtx, OracleTable* table, typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid,
                           uint64_t offset) override;
        void processDdl(OracleTable* table, typeDataObj dataObj, uint16_t type, uint16_t seq, const char* operation,
                        const char* sql, uint64_t sqlLength) override;
        void processBeginMessage() override;

    public:
        BuilderJson(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, uint64_t newMessageFormat, uint64_t newRidFormat, uint64_t newXidFormat,
                    uint64_t newTimestampFormat, uint64_t newTimestampAll, uint64_t newCharFormat, uint64_t newScnFormat, uint64_t newScnAll,
                    uint64_t newUnknownFormat, uint64_t newSchemaFormat, uint64_t newColumnFormat, uint64_t newUnknownType, uint64_t newFlushBuffer);

        void processCommit() override;
        void processCheckpoint(typeScn scn, typeTime time_, typeSeq sequence, uint64_t offset, bool redo) override;
    };
}

#endif
