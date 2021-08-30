/* Header for OutputBuffer class
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

#include <map>
#include <unordered_map>
#include <unordered_set>

#include "types.h"
#include "CharacterSet.h"
#include "RowId.h"
#include "RuntimeException.h"

#ifndef OUTPUTBUFFER_H_
#define OUTPUTBUFFER_H_

using namespace std;

namespace OpenLogReplicator {
    class CharacterSet;
    class OracleAnalyzer;
    class OracleObject;
    class RedoLogRecord;
    class Writer;

    struct OutputBufferQueue {
        uint64_t id;
        uint64_t length;
        uint8_t* data;
        OutputBufferQueue* next;
    };

    struct OutputBufferMsg {
        uint64_t id;
        uint64_t queueId;
        uint64_t length;
        typeSCN scn;
        typeSEQ sequence;
        OracleAnalyzer* oracleAnalyzer;
        uint8_t* data;
        typeOBJ obj;
        uint16_t pos;
        uint16_t flags;
    };

    class OutputBuffer {
    protected:
        static const char map64[65];
        static const char map16[17];
        OracleAnalyzer* oracleAnalyzer;
        uint64_t messageFormat;
        uint64_t ridFormat;
        uint64_t xidFormat;
        uint64_t timestampFormat;
        uint64_t charFormat;
        uint64_t scnFormat;
        uint64_t unknownFormat;
        uint64_t schemaFormat;
        uint64_t columnFormat;
        uint64_t unknownType;
        uint64_t unconfirmedLength;
        uint64_t messageLength;
        uint64_t flushBuffer;
        char valueBuffer[MAX_FIELD_LENGTH];
        uint64_t valueLength;
        unordered_map<uint16_t, const char*> timeZoneMap;
        unordered_set<OracleObject*> objects;
        typeTIME lastTime;
        typeSCN lastScn;
        typeSEQ lastSequence;
        typeXID lastXid;

        uint64_t valuesSet[MAX_NO_COLUMNS / sizeof(uint64_t)];
        uint64_t valuesMerge[MAX_NO_COLUMNS / sizeof(uint64_t)];
        uint64_t lengths[MAX_NO_COLUMNS][4];
        uint8_t* values[MAX_NO_COLUMNS][4];
        uint64_t lengthsPart[3][MAX_NO_COLUMNS][4];
        uint8_t* valuesPart[3][MAX_NO_COLUMNS][4];
        uint64_t valuesMax;
        uint8_t* merges[MAX_NO_COLUMNS*4];
        uint64_t mergesMax;
        uint64_t id;
        uint64_t num;
        uint64_t transactionType;
        bool newTran;

        void outputBufferRotate(bool copy);
        void processValue(OracleObject* object, typeCOL col, const uint8_t* data, uint64_t length);

        void valuesRelease() {
            for (uint64_t i = 0; i < mergesMax; ++i)
                delete[] merges[i];
            mergesMax = 0;

            uint64_t baseMax = valuesMax >> 6;
            for (uint64_t base = 0; base <= baseMax; ++base) {
                typeCOL column = base << 6;
                for (uint64_t mask = 1; mask != 0; mask <<= 1, ++column) {
                    if (valuesSet[base] < mask)
                        break;
                    if ((valuesSet[base] & mask) == 0)
                        continue;

                    valuesSet[base] &= ~mask;
                    values[column][VALUE_BEFORE] = nullptr;
                    values[column][VALUE_BEFORE_SUPP] = nullptr;
                    values[column][VALUE_AFTER] = nullptr;
                    values[column][VALUE_AFTER_SUPP] = nullptr;
                }
            }
            valuesMax = 0;
        };

        void valueSet(uint64_t type, uint16_t column, uint8_t* data, uint16_t length, uint8_t fb) {
            if ((trace2 & TRACE2_DML) != 0) {
                stringstream strStr;
                strStr << "DML: value: " << dec << type << "/" << column << "/" << dec << length << "/" <<
                        setfill('0') << setw(2) << hex << (uint64_t)fb << " to: ";
                for (uint64_t i = 0; i < length && i < 10; ++i) {
                    strStr << "0x" << setfill('0') << setw(2) << hex << (uint64_t)data[i] << ", ";
                }
                TRACE(TRACE2_DML, strStr.str());
            }

            uint64_t base = column >> 6;
            uint64_t mask = ((uint64_t)1) << (column & 0x3F);
            //new value
            if ((valuesSet[base] & mask) == 0) {
                valuesSet[base] |= mask;
                if (column >= valuesMax)
                    valuesMax = column + 1;
            }

            switch (fb & (FB_P | FB_N)) {
            case 0:
                lengths[column][type] = length;
                values[column][type] = data;
                break;

            case FB_N:
                lengthsPart[0][column][type] = length;
                valuesPart[0][column][type] = data;
                if ((valuesMerge[base] & mask) == 0)
                    valuesMerge[base] |= mask;
                break;

            case FB_P | FB_N:
                lengthsPart[1][column][type] = length;
                valuesPart[1][column][type] = data;
                if ((valuesMerge[base] & mask) == 0)
                    valuesMerge[base] |= mask;
                break;

            case FB_P:
                lengthsPart[2][column][type] = length;
                valuesPart[2][column][type] = data;
                if ((valuesMerge[base] & mask) == 0)
                    valuesMerge[base] |= mask;
                break;
            }
        };

        void outputBufferShift(uint64_t bytes, bool copy) {
            lastBuffer->length += bytes;

            if (lastBuffer->length >= OUTPUT_BUFFER_DATA_SIZE)
                outputBufferRotate(copy);
        };

        void outputBufferBegin(typeOBJ obj) {
            messageLength = 0;
            transactionType = 0;

            if (lastBuffer->length + sizeof(struct OutputBufferMsg) >= OUTPUT_BUFFER_DATA_SIZE)
                outputBufferRotate(true);

            msg = (OutputBufferMsg*)(lastBuffer->data + lastBuffer->length);
            outputBufferShift(sizeof(struct OutputBufferMsg), true);
            msg->scn = lastScn;
            msg->sequence = lastSequence;
            msg->length = 0;
            msg->id = id++;
            msg->obj = obj;
            msg->oracleAnalyzer = oracleAnalyzer;
            msg->pos = 0;
            msg->flags = 0;
            msg->data = lastBuffer->data + lastBuffer->length;
        };

        void outputBufferCommit(bool force) {
            if (messageLength == 0) {
                WARNING("JSON buffer - commit of empty transaction");
            }

            msg->queueId = lastBuffer->id;
            outputBufferShift((8 - (messageLength & 7)) & 7, false);
            unconfirmedLength += messageLength;
            msg->length = messageLength;

            if (force || flushBuffer == 0 || unconfirmedLength > flushBuffer) {
                {
                    unique_lock<mutex> lck(mtx);
                    writersCond.notify_all();
                }
                unconfirmedLength = 0;
            }
            msg = nullptr;
        };

        void outputBufferAppend(char character) {
            lastBuffer->data[lastBuffer->length] = character;
            ++messageLength;
            outputBufferShift(1, true);
        };

        void outputBufferAppend(const char* str, uint64_t length) {
            for (uint64_t i = 0; i < length; ++i)
                outputBufferAppend(*str++);
        };

        void outputBufferAppend(const char* str) {
            char character = *str++;
            while (character != 0) {
                outputBufferAppend(character);
                character = *str++;
            }
        };

        void outputBufferAppend(string& str) {
            const char* charstr = str.c_str();
            uint64_t length = str.length();
            for (uint64_t i = 0; i < length; ++i)
                outputBufferAppend(*charstr++);
        };

        void columnUnknown(string& columnName, const uint8_t* data, uint64_t length) {
            valueBuffer[0] = '?';
            valueLength = 1;
            columnString(columnName);
            if (unknownFormat == UNKNOWN_FORMAT_DUMP) {
                stringstream ss;
                for (uint64_t j = 0; j < length; ++j)
                    ss << " " << hex << setfill('0') << setw(2) << (uint64_t) data[j];
                WARNING("unknown value (column: " << columnName << "): " << dec << length << " - " << ss.str());
            }
        };

        void valueBufferAppend(uint8_t value) {
            if (valueLength >= MAX_FIELD_LENGTH) {
                RUNTIME_FAIL("length of value exceeded " << MAX_FIELD_LENGTH << ", please increase MAX_FIELD_LENGTH and recompile code");
            }
            valueBuffer[valueLength++] = value;
        };

        void valueBufferAppendHex(typeunicode value, uint64_t length) {
            uint64_t j = (length - 1) * 4;
            for (uint64_t i = 0; i < length; ++i) {
                if (valueLength >= MAX_FIELD_LENGTH) {
                    RUNTIME_FAIL("length of value exceeded " << MAX_FIELD_LENGTH << ", please increase MAX_FIELD_LENGTH and recompile code");
                }
                valueBuffer[valueLength++] = map16[(value >> j) & 0xF];
                j -= 4;
            };
        };

        void parseNumber(const uint8_t* data, uint64_t length) {
            valueLength = 0;

            uint8_t digits = data[0];
            //just zero
            if (digits == 0x80) {
                valueBufferAppend('0');
            } else {
                uint64_t j = 1, jMax = length - 1;

                //positive number
                if (digits > 0x80 && jMax >= 1) {
                    uint64_t value, zeros = 0;
                    //part of the total
                    if (digits <= 0xC0) {
                        valueBufferAppend('0');
                        zeros = 0xC0 - digits;
                    } else {
                        digits -= 0xC0;
                        //part of the total - omitting first zero for first digit
                        value = data[j] - 1;
                        if (value < 10)
                            valueBufferAppend('0' + value);
                        else {
                            valueBufferAppend('0' + (value / 10));
                            valueBufferAppend('0' + (value % 10));
                        }

                        ++j;
                        --digits;

                        while (digits > 0) {
                            value = data[j] - 1;
                            if (j <= jMax) {
                                valueBufferAppend('0' + (value / 10));
                                valueBufferAppend('0' + (value % 10));
                                ++j;
                            } else {
                                valueBufferAppend('0');
                                valueBufferAppend('0');
                            }
                            --digits;
                        }
                    }

                    //fraction part
                    if (j <= jMax) {
                        valueBufferAppend('.');

                        while (zeros > 0) {
                            valueBufferAppend('0');
                            valueBufferAppend('0');
                            --zeros;
                        }

                        while (j <= jMax - 1) {
                            value = data[j] - 1;
                            valueBufferAppend('0' + (value / 10));
                            valueBufferAppend('0' + (value % 10));
                            ++j;
                        }

                        //last digit - omitting 0 at the end
                        value = data[j] - 1;
                        valueBufferAppend('0' + (value / 10));
                        if ((value % 10) != 0)
                            valueBufferAppend('0' + (value % 10));
                    }
                //negative number
                } else if (digits < 0x80 && jMax >= 1) {
                    uint64_t value, zeros = 0;
                    valueBufferAppend('-');

                    if (data[jMax] == 0x66)
                        --jMax;

                    //part of the total
                    if (digits >= 0x3F) {
                        valueBufferAppend('0');
                        zeros = digits - 0x3F;
                    } else {
                        digits = 0x3F - digits;

                        value = 101 - data[j];
                        if (value < 10)
                            valueBufferAppend('0' + value);
                        else {
                            valueBufferAppend('0' + (value / 10));
                            valueBufferAppend('0' + (value % 10));
                        }
                        ++j;
                        --digits;

                        while (digits > 0) {
                            if (j <= jMax) {
                                value = 101 - data[j];
                                valueBufferAppend('0' + (value / 10));
                                valueBufferAppend('0' + (value % 10));
                                ++j;
                            } else {
                                valueBufferAppend('0');
                                valueBufferAppend('0');
                            }
                            --digits;
                        }
                    }

                    if (j <= jMax) {
                        valueBufferAppend('.');

                        while (zeros > 0) {
                            valueBufferAppend('0');
                            valueBufferAppend('0');
                            --zeros;
                        }

                        while (j <= jMax - 1) {
                            value = 101 - data[j];
                            valueBufferAppend('0' + (value / 10));
                            valueBufferAppend('0' + (value % 10));
                            ++j;
                        }

                        value = 101 - data[j];
                        valueBufferAppend('0' + (value / 10));
                        if ((value % 10) != 0)
                            valueBufferAppend('0' + (value % 10));
                    }
                } else {
                    RUNTIME_FAIL("got unknown numeric value");
                }
            }
        };

        void parseString(const uint8_t* data, uint64_t length, uint64_t charsetId) {
            CharacterSet* characterSet = characterMap[charsetId];
            if (characterSet == nullptr && (charFormat & CHAR_FORMAT_NOMAPPING) == 0) {
                RUNTIME_FAIL("can't find character set map for id = " << dec << charsetId);
            }
            valueLength = 0;

            while (length > 0) {
                typeunicode unicodeCharacter;
                uint64_t unicodeCharacterLength;

                if ((charFormat & CHAR_FORMAT_NOMAPPING) == 0) {
                    unicodeCharacter = characterSet->decode(data, length);
                    unicodeCharacterLength = 8;
                } else {
                    unicodeCharacter = *data++;
                    --length;
                    unicodeCharacterLength = 2;
                }

                if ((charFormat & CHAR_FORMAT_HEX) != 0) {
                    valueBufferAppendHex(unicodeCharacter, unicodeCharacterLength);
                } else {
                    //0xxxxxxx
                    if (unicodeCharacter <= 0x7F) {
                        valueBufferAppend(unicodeCharacter);

                    //110xxxxx 10xxxxxx
                    } else if (unicodeCharacter <= 0x7FF) {
                        valueBufferAppend(0xC0 | (uint8_t)(unicodeCharacter >> 6));
                        valueBufferAppend(0x80 | (uint8_t)(unicodeCharacter & 0x3F));

                    //1110xxxx 10xxxxxx 10xxxxxx
                    } else if (unicodeCharacter <= 0xFFFF) {
                        valueBufferAppend(0xE0 | (uint8_t)(unicodeCharacter >> 12));
                        valueBufferAppend(0x80 | (uint8_t)((unicodeCharacter >> 6) & 0x3F));
                        valueBufferAppend(0x80 | (uint8_t)(unicodeCharacter & 0x3F));

                    //11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                    } else if (unicodeCharacter <= 0x10FFFF) {
                        valueBufferAppend(0xF0 | (uint8_t)(unicodeCharacter >> 18));
                        valueBufferAppend(0x80 | (uint8_t)((unicodeCharacter >> 12) & 0x3F));
                        valueBufferAppend(0x80 | (uint8_t)((unicodeCharacter >> 6) & 0x3F));
                        valueBufferAppend(0x80 | (uint8_t)(unicodeCharacter & 0x3F));

                    } else {
                        RUNTIME_FAIL("got character code: U+" << dec << unicodeCharacter);
                    }
                }
            }
        };

        virtual void columnNull(OracleObject* object, typeCOL col) = 0;
        virtual void columnFloat(string& columnName, float value) = 0;
        virtual void columnDouble(string& columnName, double value) = 0;
        virtual void columnString(string& columnName) = 0;
        virtual void columnNumber(string& columnName, uint64_t precision, uint64_t scale) = 0;
        virtual void columnRaw(string& columnName, const uint8_t* data, uint64_t length) = 0;
        virtual void columnTimestamp(string& columnName, struct tm &time_, uint64_t fraction, const char* tz) = 0;
        virtual void appendRowid(typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot) = 0;
        virtual void appendHeader(bool first, bool showXid) = 0;
        virtual void appendSchema(OracleObject* object, typeDATAOBJ dataObj) = 0;
        virtual void processInsert(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) = 0;
        virtual void processUpdate(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) = 0;
        virtual void processDelete(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) = 0;
        virtual void processDDL(OracleObject* object, typeDATAOBJ dataObj, uint16_t type, uint16_t seq, const char* operation,
                const char* sql, uint64_t sqlLength) = 0;
        virtual void processBegin(void) = 0;

    public:
        uint64_t defaultCharacterMapId;
        uint64_t defaultCharacterNcharMapId;
        unordered_map<uint64_t, CharacterSet*> characterMap;
        Writer* writer;
        mutex mtx;
        condition_variable writersCond;

        uint64_t buffersAllocated;
        OutputBufferQueue* firstBuffer;
        OutputBufferQueue* lastBuffer;
        OutputBufferMsg* msg;

        OutputBuffer(uint64_t messageFormat, uint64_t ridFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat,
                uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat, uint64_t unknownType, uint64_t flushBuffer);
        virtual ~OutputBuffer();

        void initialize(OracleAnalyzer* oracleAnalyzer);
        uint64_t outputBufferSize(void) const;
        void setWriter(Writer* writer);
        void setNlsCharset(string& nlsCharset, string& nlsNcharCharset);

        void processBegin(typeSCN scn, typeTIME time_, typeSEQ sequence, typeXID xid);
        virtual void processCommit(void) = 0;
        void processInsertMultiple(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2, bool system);
        void processDeleteMultiple(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2, bool system);
        void processDML(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2, uint64_t type, bool system);
        void processDDLheader(RedoLogRecord* redoLogRecord1);
        virtual void processCheckpoint(typeSCN scn, typeTIME time_, typeSEQ sequence, uint64_t offset, bool redo) = 0;

        friend class SystemTransaction;
    };
}

#endif
