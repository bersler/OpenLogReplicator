/* Memory buffer for handling output data in JSON format
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "CharacterSet.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OutputBufferJson.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"
#include "Writer.h"

namespace OpenLogReplicator {

    OutputBufferJson::OutputBufferJson(uint64_t messageFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat,
            uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat) :
            OutputBuffer(messageFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat, schemaFormat, columnFormat),
            hasPreviousRedo(false),
            hasPreviousColumn(false) {
    }

    OutputBufferJson::~OutputBufferJson() {
    }

    void OutputBufferJson::appendHex(uint64_t val, uint64_t length) {
        uint64_t j = (length - 1) * 4;
        for (uint64_t i = 0; i < length; ++i) {
            bufferAppend(map16[(val >> j) & 0xF]);
            j -= 4;
        };
    }

    void OutputBufferJson::appendDec(uint64_t val, uint64_t length) {
        char buffer[21];

        for (uint i = 0; i < length; ++i) {
            buffer[i] = '0' + (val % 10);
            val /= 10;
        }

        for (uint64_t i = 0; i < length; ++i)
            bufferAppend(buffer[length - i - 1]);
    }

    void OutputBufferJson::appendDec(uint64_t val) {
        char buffer[21];
        uint64_t length = 0;

        if (val == 0) {
            buffer[0] = '0';
            length = 1;
        } else {
            while (val > 0) {
                buffer[length++] = '0' + (val % 10);
                val /= 10;
            }
        }

        for (uint64_t i = 0; i < length; ++i)
            bufferAppend(buffer[length - i - 1]);
    }

    void OutputBufferJson::appendSDec(int64_t val) {
        char buffer[22];
        uint64_t length = 0;

        if (val == 0) {
            buffer[0] = '0';
            length = 1;
        } else {
            if (val < 0) {
                val = -val;
                while (val > 0) {
                    buffer[length++] = '0' + (val % 10);
                    val /= 10;
                }
                buffer[length++] = '-';
            } else {
                while (val > 0) {
                    buffer[length++] = '0' + (val % 10);
                    val /= 10;
                }
            }
        }

        for (uint64_t i = 0; i < length; ++i)
            bufferAppend(buffer[length - i - 1]);
    }

    void OutputBufferJson::appendEscape(const char *str, uint64_t length) {
        while (length > 0) {
            if (*str == '\t') {
                bufferAppend('\\');
                bufferAppend('t');
            } else if (*str == '\r') {
                bufferAppend('\\');
                bufferAppend('r');
            } else if (*str == '\n') {
                bufferAppend('\\');
                bufferAppend('n');
            } else if (*str == '\f') {
                bufferAppend('\\');
                bufferAppend('f');
            } else if (*str == '\b') {
                bufferAppend('\\');
                bufferAppend('b');
            } else {
                if (*str == '"' || *str == '\\' || *str == '/')
                    bufferAppend('\\');
                bufferAppend(*(str++));
            }
            --length;
        }
    }

    void OutputBufferJson::appendEscapeMap(const uint8_t *str, uint64_t length, uint64_t charsetId) {
        bool isNext = false;

        CharacterSet *characterSet = characterMap[charsetId];
        if (characterSet == nullptr && (charFormat & 1) == 0) {
            RUNTIME_FAIL("can't find character set map for id = " << dec << charsetId);
        }

        while (length > 0) {
            typeunicode unicodeCharacter;
            uint64_t unicodeCharacterLength;

            if ((charFormat & 1) == 0) {
                unicodeCharacter = characterSet->decode(str, length);
                unicodeCharacterLength = 8;
            } else {
                unicodeCharacter = *str++;
                --length;
                unicodeCharacterLength = 2;
            }

            if ((charFormat & 2) == 2) {
                if (isNext)
                    bufferAppend(',');
                else
                    isNext = true;
                bufferAppend('0');
                bufferAppend('x');
                appendHex(unicodeCharacter, unicodeCharacterLength);
            } else
            if (unicodeCharacter == '\t') {
                bufferAppend('\\');
                bufferAppend('t');
            } else if (unicodeCharacter == '\r') {
                bufferAppend('\\');
                bufferAppend('r');
            } else if (unicodeCharacter == '\n') {
                bufferAppend('\\');
                bufferAppend('n');
            } else if (unicodeCharacter == '\f') {
                bufferAppend('\\');
                bufferAppend('f');
            } else if (unicodeCharacter == '\b') {
                bufferAppend('\\');
                bufferAppend('b');
            } else if (unicodeCharacter == '"') {
                bufferAppend('\\');
                bufferAppend('"');
            } else if (unicodeCharacter == '\\') {
                bufferAppend('\\');
                bufferAppend('\\');
            } else if (unicodeCharacter == '/') {
                bufferAppend('\\');
                bufferAppend('/');
            } else {
                //0xxxxxxx
                if (unicodeCharacter <= 0x7F) {
                    bufferAppend(unicodeCharacter);

                //110xxxxx 10xxxxxx
                } else if (unicodeCharacter <= 0x7FF) {
                    bufferAppend(0xC0 | (uint8_t)(unicodeCharacter >> 6));
                    bufferAppend(0x80 | (uint8_t)(unicodeCharacter & 0x3F));

                //1110xxxx 10xxxxxx 10xxxxxx
                } else if (unicodeCharacter <= 0xFFFF) {
                    bufferAppend(0xE0 | (uint8_t)(unicodeCharacter >> 12));
                    bufferAppend(0x80 | (uint8_t)((unicodeCharacter >> 6) & 0x3F));
                    bufferAppend(0x80 | (uint8_t)(unicodeCharacter & 0x3F));

                //11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                } else if (unicodeCharacter <= 0x10FFFF) {
                    bufferAppend(0xF0 | (uint8_t)(unicodeCharacter >> 18));
                    bufferAppend(0x80 | (uint8_t)((unicodeCharacter >> 12) & 0x3F));
                    bufferAppend(0x80 | (uint8_t)((unicodeCharacter >> 6) & 0x3F));
                    bufferAppend(0x80 | (uint8_t)(unicodeCharacter & 0x3F));

                } else {
                    RUNTIME_FAIL("got character code: U+" << dec << unicodeCharacter);
                }
            }
        }
    }

    void OutputBufferJson::appendNull(string &columnName) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        append(columnName);
        append("\":null");
    }

    void OutputBufferJson::appendUnknown(string &columnName, const uint8_t *data, uint64_t length) {
        append("\"?\"");
        if (unknownFormat == UNKNOWN_FORMAT_DUMP) {
            stringstream ss;
            for (uint64_t j = 0; j < length; ++j)
                ss << " " << hex << setfill('0') << setw(2) << (uint64_t) data[j];
            WARNING("unknown value (column: " << columnName << "): " << dec << length << " - " << ss.str());
        }
    }

    void OutputBufferJson::appendString(string &columnName, const uint8_t *data, uint64_t length, uint64_t charsetId) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        append(columnName);
        append("\":\"");
        appendEscapeMap(data, length, charsetId);
        append('"');
    }

    void OutputBufferJson::appendTimestamp(string &columnName, struct tm &epochtime, uint64_t fraction, const char *tz) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        append(columnName);
        append("\":");

        if ((timestampFormat & TIMESTAMP_FORMAT_ISO8601) != 0) {
            //2012-04-23T18:25:43.511Z - ISO 8601 format
            append('"');
            if (epochtime.tm_year > 0) {
                appendDec((uint64_t)epochtime.tm_year);
            } else {
                appendDec((uint64_t)(-epochtime.tm_year));
                append("BC");
            }
            append('-');
            appendDec(epochtime.tm_mon, 2);
            append('-');
            appendDec(epochtime.tm_mday, 2);
            append('T');
            appendDec(epochtime.tm_hour, 2);
            append(':');
            appendDec(epochtime.tm_min, 2);
            append(':');
            appendDec(epochtime.tm_sec, 2);

            if (fraction > 0) {
                append('.');
                appendDec(fraction, 9);
            }

            if (tz != nullptr) {
                append(' ');
                append(tz);
            }
            append('"');
        } else {
            //unix epoch format

            if (epochtime.tm_year >= 1900) {
                --epochtime.tm_mon;
                epochtime.tm_year -= 1900;
                appendDec(mktime(&epochtime) * 1000 + ((fraction + 500000) / 1000000));
            } else
                appendDec(0);
        }
    }

    void OutputBufferJson::appendNumber(string &columnName, const uint8_t *data, uint64_t length) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        append(columnName);
        append("\":");

        uint8_t digits = data[0];
        //just zero
        if (digits == 0x80) {
            append('0');
            return;
        }

        uint64_t j = 1, jMax = length - 1;

        //positive number
        if (digits > 0x80 && jMax >= 1) {
            uint64_t val, zeros = 0;
            //part of the total
            if (digits <= 0xC0) {
                append('0');
                zeros = 0xC0 - digits;
            } else {
                digits -= 0xC0;
                //part of the total - omitting first zero for first digit
                val = data[j] - 1;
                if (val < 10)
                    append('0' + val);
                else {
                    append('0' + (val / 10));
                    append('0' + (val % 10));
                }

                ++j;
                --digits;

                while (digits > 0) {
                    val = data[j] - 1;
                    if (j <= jMax) {
                        append('0' + (val / 10));
                        append('0' + (val % 10));
                        ++j;
                    } else {
                        append('0');
                        append('0');
                    }
                    --digits;
                }
            }

            //fraction part
            if (j <= jMax) {
                append('.');

                while (zeros > 0) {
                    append('0');
                    append('0');
                    --zeros;
                }

                while (j <= jMax - 1) {
                    val = data[j] - 1;
                    append('0' + (val / 10));
                    append('0' + (val % 10));
                    ++j;
                }

                //last digit - omitting 0 at the end
                val = data[j] - 1;
                append('0' + (val / 10));
                if ((val % 10) != 0)
                    append('0' + (val % 10));
            }
        //negative number
        } else if (digits < 0x80 && jMax >= 1) {
            uint64_t val, zeros = 0;
            append('-');

            if (data[jMax] == 0x66)
                --jMax;

            //part of the total
            if (digits >= 0x3F) {
                append('0');
                zeros = digits - 0x3F;
            } else {
                digits = 0x3F - digits;

                val = 101 - data[j];
                if (val < 10)
                    append('0' + val);
                else {
                    append('0' + (val / 10));
                    append('0' + (val % 10));
                }
                ++j;
                --digits;

                while (digits > 0) {
                    if (j <= jMax) {
                        val = 101 - data[j];
                        append('0' + (val / 10));
                        append('0' + (val % 10));
                        ++j;
                    } else {
                        append('0');
                        append('0');
                    }
                    --digits;
                }
            }

            if (j <= jMax) {
                append('.');

                while (zeros > 0) {
                    append('0');
                    append('0');
                    --zeros;
                }

                while (j <= jMax - 1) {
                    val = 101 - data[j];
                    append('0' + (val / 10));
                    append('0' + (val % 10));
                    ++j;
                }

                val = 101 - data[j];
                append('0' + (val / 10));
                if ((val % 10) != 0)
                    append('0' + (val % 10));
            }
        } else
            appendUnknown(columnName, data, length);
    }

    void OutputBufferJson::appendFloat(string &columnName, float val) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        append(columnName);
        append("\":");

        stringstream valStringStream;
        valStringStream << val;
        string valString = valStringStream.str();
        append(valString);
    }

    void OutputBufferJson::appendDouble(string &columnName, double val) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        append(columnName);
        append("\":");

        stringstream valStringStream;
        valStringStream << val;
        string valString = valStringStream.str();
        append(valString);
    }



    void OutputBufferJson::appendRaw(string &columnName, const uint8_t *data, uint64_t length) {
        append('"');
        append(columnName);
        append("\":\"");
        for (uint64_t j = 0; j < length; ++j)
            appendHex(*(data + j), 2);
        append('"');
    }


    void OutputBufferJson::appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot) {
        uint32_t afn = bdba >> 22;
        bdba &= 0x003FFFFF;
        append("\"rid\":\"");
        append(map64[(objd >> 30) & 0x3F]);
        append(map64[(objd >> 24) & 0x3F]);
        append(map64[(objd >> 18) & 0x3F]);
        append(map64[(objd >> 12) & 0x3F]);
        append(map64[(objd >> 6) & 0x3F]);
        append(map64[objd & 0x3F]);
        append(map64[(afn >> 12) & 0x3F]);
        append(map64[(afn >> 6) & 0x3F]);
        append(map64[afn & 0x3F]);
        append(map64[(bdba >> 30) & 0x3F]);
        append(map64[(bdba >> 24) & 0x3F]);
        append(map64[(bdba >> 18) & 0x3F]);
        append(map64[(bdba >> 12) & 0x3F]);
        append(map64[(bdba >> 6) & 0x3F]);
        append(map64[bdba & 0x3F]);
        append(map64[(slot >> 12) & 0x3F]);
        append(map64[(slot >> 6) & 0x3F]);
        append(map64[slot & 0x3F]);
        append('"');
    }

    void OutputBufferJson::appendHeader(bool first) {
        if (first || (scnFormat & SCN_FORMAT_ALL_PAYLOADS) != 0) {
            if ((scnFormat & SCN_FORMAT_HEX) != 0) {
                append("\"scns\":\"0x");
                appendHex(lastScn, 16);
                append("\",");
            } else {
                append("\"scn\":");
                appendDec(lastScn);
                append(',');
            }
        }

        if (first || (timestampFormat & TIMESTAMP_FORMAT_ALL_PAYLOADS) != 0) {
            if ((timestampFormat & TIMESTAMP_FORMAT_ISO8601) != 0) {
                append("\"tms\":\"");
                char iso[21];
                lastTime.toISO8601(iso);
                append(iso);
                append("\",");
            } else {
                append("\"tm\":");
                appendDec(lastTime.toTime() * 1000);
                append(',');
            }
        }

        if (xidFormat == XID_FORMAT_TEXT) {
            append("\"xid\":\"");
            appendDec(USN(lastXid));
            append('.');
            appendDec(SLT(lastXid));
            append('.');
            appendDec(SQN(lastXid));
            append('"');
        } else {
            append("\"xidn\":");
            appendDec(lastXid);
        }
    }

    void OutputBufferJson::appendSchema(OracleObject *object) {
        append("\"schema\":{\"owner\":\"");
        append(object->owner);
        append("\",\"table\":\"");
        append(object->name);
        append('"');

        if ((schemaFormat & SCHEMA_FORMAT_OBJN) != 0) {
            append(",\"objn\":");
            appendDec(object->objn);
        }

        if ((schemaFormat & SCHEMA_FORMAT_FULL) != 0) {
            if ((schemaFormat & SCHEMA_FORMAT_REPEATED) == 0) {
                if (objects.count(object) > 0)
                    return;
                else
                    objects.insert(object);
            }

            append(",\"columns\":[");

            bool hasPrev = false;
            for (uint64_t i = 0; i < object->columns.size(); ++i) {
                if (object->columns[i] == nullptr)
                    continue;

                if (hasPrev)
                    append(',');
                else
                    hasPrev = true;

                append("{\"name\":\"");
                append(object->columns[i]->columnName);

                append("\",\"type\":");
                switch(object->columns[i]->typeNo) {
                case 1: //varchar2(n), nvarchar(n)
                    append("\"varchar2\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 2: //number(p, s), float(p)
                    append("\"number\",\"precision\":");
                    appendSDec(object->columns[i]->precision);
                    append(",\"scale\":");
                    appendSDec(object->columns[i]->scale);
                    break;

                case 8: //long, not supported
                    append("\"long\"");
                    break;

                case 12: //date
                    append("\"date\"");
                    break;

                case 23: //raw(n)
                    append("\"raw\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 24: //long raw, not supported
                    append("\"long raw\"");
                    break;

                case 69: //rowid, not supported
                    append("\"rowid\"");
                    break;

                case 96: //char(n), nchar(n)
                    append("\"char\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 100: //binary_float
                    append("\"binary_float\"");
                    break;

                case 101: //binary_double
                    append("\"binary_double\"");
                    break;

                case 112: //clob, nclob, not supported
                    append("\"clob\"");
                    break;

                case 113: //blob, not supported
                    append("\"blob\"");
                    break;

                case 180: //timestamp(n)
                    append("\"timestamp\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 181: //timestamp with time zone(n)
                    append("\"timestamp with time zone\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 182: //interval year to month(n)
                    append("\"interval year to month\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 183: //interval day to second(n)
                    append("\"interval day to second\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 208: //urawid(n)
                    append("\"urawid\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 231: //timestamp with local time zone(n), not supported
                    append("\"timestamp with local time zone\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                default:
                    append("\"unknown\"");
                    break;
                }

                append(",\"nullable\":");
                if (object->columns[i]->nullable)
                    append('1');
                else
                    append('0');

                append("}");
            }
            append(']');
        }

        append('}');
    }

    void OutputBufferJson::processBegin(typescn scn, typetime time, typexid xid) {
        lastTime = time;
        lastScn = scn;
        lastXid = xid;
        hasPreviousRedo = false;

        beginMessage();
        append('{');
        appendHeader(true);

        if (messageFormat == MESSAGE_FORMAT_FULL)
            append(",\"payload\":[");
        else {
            append(",\"payload\":[{\"op\":\"begin\"}]}");
            commitMessage();
        }
    }

    void OutputBufferJson::processCommit(void) {
        if (messageFormat == MESSAGE_FORMAT_FULL)
            append("]}");
        else {
            beginMessage();
            append('{');
            appendHeader(false);
            append(",\"payload\":[{\"op\":\"commit\"}]}");
        }
        commitMessage();
    }

    void OutputBufferJson::processInsert(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            beginMessage();
            append('{');
            appendHeader(false);
            append(",\"payload\":[");
        }

        append("{\"op\":\"c\",");
        appendSchema(object);
        append(',');
        appendRowid(object->objn, object->objd, bdba, slot);
        append(",\"after\":{");

        hasPreviousColumn = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (afterPos[i] != nullptr && afterLen[i] > 0)
                processValue(object->columns[i]->columnName, afterPos[i], afterLen[i], object->columns[i]->typeNo, object->columns[i]->charsetId);
            else
            if (columnFormat >= COLUMN_FORMAT_INS_DEC || object->columns[i]->numPk > 0)
                appendNull(object->columns[i]->columnName);
        }
        append("}}");

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            append("]}");
            commitMessage();
        }
    }

    void OutputBufferJson::processUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        checkUpdate(object, bdba, slot, xid);

        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            beginMessage();
            append('{');
            appendHeader(false);
            append(",\"payload\":[");
        }

        append("{\"op\":\"u\",");
        appendSchema(object);
        append(',');
        appendRowid(object->objn, object->objd, bdba, slot);
        append(",\"before\":{");

        hasPreviousColumn = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (beforePos[i] != nullptr && beforeLen[i] > 0)
                processValue(object->columns[i]->columnName, beforePos[i], beforeLen[i], object->columns[i]->typeNo, object->columns[i]->charsetId);
            else
            if (afterPos[i] > 0 || beforePos[i] > 0)
                appendNull(object->columns[i]->columnName);
        }

        append("},\"after\":{");

        hasPreviousColumn = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (afterPos[i] != nullptr && afterLen[i] > 0)
                processValue(object->columns[i]->columnName, afterPos[i], afterLen[i], object->columns[i]->typeNo, object->columns[i]->charsetId);
            else
            if (afterPos[i] > 0 || beforePos[i] > 0)
                appendNull(object->columns[i]->columnName);
        }
        append("}}");

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            append("]}");
            commitMessage();
        }
    }

    void OutputBufferJson::processDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            beginMessage();
            append('{');
            appendHeader(false);
            append(",\"payload\":[");
        }

        append("{\"op\":\"u\",");
        appendSchema(object);
        append(',');
        appendRowid(object->objn, object->objd, bdba, slot);
        append(",\"before\":{");

        hasPreviousColumn = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            //value present before
            if (beforePos[i] != nullptr && beforeLen[i] > 0)
                processValue(object->columns[i]->columnName, beforePos[i], beforeLen[i], object->columns[i]->typeNo, object->columns[i]->charsetId);
            else
            if (columnFormat >= COLUMN_FORMAT_INS_DEC || object->columns[i]->numPk > 0)
                appendNull(object->columns[i]->columnName);
        }
        append("}}");

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            append("]}");
            commitMessage();
        }
    }

    void OutputBufferJson::processDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const char *sql, uint64_t sqlLength) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            beginMessage();
            append('{');
            appendHeader(false);
            append(",\"payload\":[");
        }

        append("{\"op\":\"ddl\",");
        appendSchema(object);
        append(",\"sql\":\"");
        appendEscape(sql, sqlLength);
        append("\"}");

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            append("]}");
            commitMessage();
        }
    }
}
