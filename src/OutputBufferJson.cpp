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
            hasPrevious(false) {
    }

    OutputBufferJson::~OutputBufferJson() {
    }

    void OutputBufferJson::appendHex(uint64_t val, uint64_t length) {
        static const char* digits = "0123456789abcdef";

        uint64_t j = (length - 1) * 4;
        for (uint64_t i = 0; i < length; ++i) {
            bufferAppend(digits[(val >> j) & 0xF]);
            j -= 4;
        };
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

    void OutputBufferJson::appendTimestamp(const uint8_t *data, uint64_t length) {
        if (timestampFormat == 0 || timestampFormat == 1) {
            //2012-04-23T18:25:43.511Z - ISO 8601 format
            uint64_t val1 = data[0],
                     val2 = data[1];
            bool bc = false;

            //AD
            if (val1 >= 100 && val2 >= 100) {
                val1 -= 100;
                val2 -= 100;
            //BC
            } else {
                val1 = 100 - val1;
                val2 = 100 - val2;
                bc = true;
            }
            if (val1 > 0) {
                if (val1 > 10) {
                    append('0' + (val1 / 10));
                    append('0' + (val1 % 10));
                    append('0' + (val2 / 10));
                    append('0' + (val2 % 10));
                } else {
                    append('0' + val1);
                    append('0' + (val2 / 10));
                    append('0' + (val2 % 10));
                }
            } else {
                if (val2 > 10) {
                    append('0' + (val2 / 10));
                    append('0' + (val2 % 10));
                } else
                    append('0' + val2);
            }

            if (bc)
                append("BC");

            append('-');
            append('0' + (data[2] / 10));
            append('0' + (data[2] % 10));
            append('-');
            append('0' + (data[3] / 10));
            append('0' + (data[3] % 10));
            append('T');
            append('0' + ((data[4] - 1) / 10));
            append('0' + ((data[4] - 1) % 10));
            append(':');
            append('0' + ((data[5] - 1) / 10));
            append('0' + ((data[5] - 1) % 10));
            append(':');
            append('0' + ((data[6] - 1) / 10));
            append('0' + ((data[6] - 1) % 10));

            if (length == 11) {
                uint64_t digits = 0;
                uint8_t buffer[10];
                uint64_t val = oracleAnalyser->read32Big(data + 7);

                for (int64_t i = 9; i > 0; --i) {
                    buffer[i] = val % 10;
                    val /= 10;
                    if (buffer[i] != 0 && digits == 0)
                        digits = i;
                }

                if (digits > 0) {
                    append('.');
                    for (uint64_t i = 1; i <= digits; ++i)
                        append(buffer[i] + '0');
                }
            }
        } else if (timestampFormat == 2) {
            //unix epoch format
            struct tm epochtime;
            uint64_t val1 = data[0],
                     val2 = data[1];

            //AD
            if (val1 >= 100 && val2 >= 100) {
                val1 -= 100;
                val2 -= 100;
                uint64_t year;
                year = val1 * 100 + val2;
                if (year >= 1900) {
                    epochtime.tm_sec = data[6] - 1;
                    epochtime.tm_min = data[5] - 1;
                    epochtime.tm_hour = data[4] - 1;
                    epochtime.tm_mday = data[3];
                    epochtime.tm_mon = data[2] - 1;
                    epochtime.tm_year = year - 1900;

                    uint64_t fraction = 0;
                    if (length == 11)
                        fraction = oracleAnalyser->read32Big(data + 7);

                    appendDec(mktime(&epochtime) * 1000 + ((fraction + 500000) / 1000000));
                }
            }
        }
    }

    void OutputBufferJson::appendEscape(const uint8_t *str, uint64_t length) {
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

    void OutputBufferJson::appendNull(string &columnName, bool &prevValue) {
        if (prevValue)
            append(',');
        else
            prevValue = true;

        append('"');
        append(columnName);
        append("\":null");
    }

    void OutputBufferJson::appendUnknown(string &columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t fieldPos, uint64_t fieldLength) {
        append("\"?\"");
        stringstream ss;
        for (uint64_t j = 0; j < fieldLength; ++j)
            ss << " " << hex << setfill('0') << setw(2) << (uint64_t) redoLogRecord->data[fieldPos + j];
        WARNING("unknown value (table: " << redoLogRecord->object->owner << "." << redoLogRecord->object->name << " column: " << columnName << " type: " << dec << typeNo << "): " << dec << fieldLength << " - " << ss.str());
    }

    void OutputBufferJson::appendValue(string &columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t charsetId, uint64_t fieldPos, uint64_t fieldLength, bool &prevValue) {
        uint64_t j, jMax;
        uint8_t digits;

        if (redoLogRecord->length == 0) {
            RUNTIME_FAIL("ERROR, trying to output null data for column: " << columnName);
        }

        if (prevValue)
            append(',');
        else
            prevValue = true;

        append('"');
        append(columnName);
        append("\":");

        switch(typeNo) {
        case 1: //varchar2/nvarchar2
        case 96: //char/nchar
            append('"');
            appendEscapeMap(redoLogRecord->data + fieldPos, fieldLength, charsetId);
            append('"');
            break;

        case 2: //number/float
            digits = redoLogRecord->data[fieldPos + 0];
            //just zero
            if (digits == 0x80) {
                append('0');
                break;
            }

            j = 1;
            jMax = fieldLength - 1;

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
                    val = redoLogRecord->data[fieldPos + j] - 1;
                    if (val < 10)
                        append('0' + val);
                    else {
                        append('0' + (val / 10));
                        append('0' + (val % 10));
                    }

                    ++j;
                    --digits;

                    while (digits > 0) {
                        val = redoLogRecord->data[fieldPos + j] - 1;
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
                        val = redoLogRecord->data[fieldPos + j] - 1;
                        append('0' + (val / 10));
                        append('0' + (val % 10));
                        ++j;
                    }

                    //last digit - omitting 0 at the end
                    val = redoLogRecord->data[fieldPos + j] - 1;
                    append('0' + (val / 10));
                    if ((val % 10) != 0)
                        append('0' + (val % 10));
                }
            //negative number
            } else if (digits < 0x80 && jMax >= 1) {
                uint64_t val, zeros = 0;
                append('-');

                if (redoLogRecord->data[fieldPos + jMax] == 0x66)
                    --jMax;

                //part of the total
                if (digits >= 0x3F) {
                    append('0');
                    zeros = digits - 0x3F;
                } else {
                    digits = 0x3F - digits;

                    val = 101 - redoLogRecord->data[fieldPos + j];
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
                            val = 101 - redoLogRecord->data[fieldPos + j];
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
                        val = 101 - redoLogRecord->data[fieldPos + j];
                        append('0' + (val / 10));
                        append('0' + (val % 10));
                        ++j;
                    }

                    val = 101 - redoLogRecord->data[fieldPos + j];
                    append('0' + (val / 10));
                    if ((val % 10) != 0)
                        append('0' + (val % 10));
                }
            } else
                appendUnknown(columnName, redoLogRecord, typeNo, fieldPos, fieldLength);
            break;

        case 12:  //date
        case 180: //timestamp
            if (fieldLength != 7 && fieldLength != 11)
                appendUnknown(columnName, redoLogRecord, typeNo, fieldPos, fieldLength);
            else {
                append('"');
                appendTimestamp(redoLogRecord->data + fieldPos, fieldLength);
                append('"');
            }
            break;

        case 23: //raw
            append('"');
            for (uint64_t j = 0; j < fieldLength; ++j)
                appendHex(*(redoLogRecord->data + fieldPos + j), 2);
            append('"');
            break;

        case 100: //binary_float
            if (fieldLength == 4) {
                stringstream valStringStream;
                float *valFloat = (float *)redoLogRecord->data + fieldPos;
                valStringStream << *valFloat;
                string valString = valStringStream.str();
                append(valString);
            } else
                appendUnknown(columnName, redoLogRecord, typeNo, fieldPos, fieldLength);
            break;

        case 101: //binary_double
            if (fieldLength == 8) {
                stringstream valStringStream;
                double *valDouble = (double *)redoLogRecord->data + fieldPos;
                valStringStream << *valDouble;
                string valString = valStringStream.str();
                append(valString);
            } else
                appendUnknown(columnName, redoLogRecord, typeNo, fieldPos, fieldLength);
            break;

        //case 231: //timestamp with local time zone
        case 181: //timestamp with time zone
            if (fieldLength != 13) {
                appendUnknown(columnName, redoLogRecord, typeNo, fieldPos, fieldLength);
            } else {
                append('"');
                appendTimestamp(redoLogRecord->data + fieldPos, fieldLength - 2);

                //append time zone information, but leave time in UTC
                if (timestampFormat == 1) {
                    if (redoLogRecord->data[fieldPos + 11] >= 5 && redoLogRecord->data[fieldPos + 11] <= 36) {
                        append(' ');
                        if (redoLogRecord->data[fieldPos + 11] < 20 ||
                                (redoLogRecord->data[fieldPos + 11] == 20 && redoLogRecord->data[fieldPos + 12] < 60))
                            append('-');
                        else
                            append('+');

                        if (redoLogRecord->data[fieldPos + 11] < 20) {
                            if (20 - redoLogRecord->data[fieldPos + 11] < 10)
                                append('0');
                            appendDec(20 - redoLogRecord->data[fieldPos + 11]);
                        } else {
                            if (redoLogRecord->data[fieldPos + 11] - 20 < 10)
                                append('0');
                            appendDec(redoLogRecord->data[fieldPos + 11] - 20);
                        }

                        append(':');

                        if (redoLogRecord->data[fieldPos + 12] < 60) {
                            if (60 - redoLogRecord->data[fieldPos + 12] < 10)
                                append('0');
                            appendDec(60 - redoLogRecord->data[fieldPos + 12]);
                        } else {
                            if (redoLogRecord->data[fieldPos + 12] - 60 < 10)
                                append('0');
                            appendDec(redoLogRecord->data[fieldPos + 12] - 60);
                        }
                    } else {
                        append(' ');

                        uint16_t tzkey = (redoLogRecord->data[fieldPos + 11] << 8) | redoLogRecord->data[fieldPos + 12];
                        const char *tz = timeZoneMap[tzkey];
                        if (tz == nullptr)
                            append("TZ?");
                        else
                            append(tz);
                    }
                }

                append('"');
            }
            break;

        default:
            if (unknownFormat == UNKNOWN_FORMAT_DUMP)
                appendUnknown(columnName, redoLogRecord, typeNo, fieldPos, fieldLength);
            else
                append("\"?\"");
        }
    }

    void OutputBufferJson::appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot) {
        uint32_t afn =  bdba >> 22;
        bdba &= 0x003FFFFF;
        append("\"rid\":\"");
        append(translationMap[(objd >> 30) & 0x3F]);
        append(translationMap[(objd >> 24) & 0x3F]);
        append(translationMap[(objd >> 18) & 0x3F]);
        append(translationMap[(objd >> 12) & 0x3F]);
        append(translationMap[(objd >> 6) & 0x3F]);
        append(translationMap[objd & 0x3F]);
        append(translationMap[(afn >> 12) & 0x3F]);
        append(translationMap[(afn >> 6) & 0x3F]);
        append(translationMap[afn & 0x3F]);
        append(translationMap[(bdba >> 30) & 0x3F]);
        append(translationMap[(bdba >> 24) & 0x3F]);
        append(translationMap[(bdba >> 18) & 0x3F]);
        append(translationMap[(bdba >> 12) & 0x3F]);
        append(translationMap[(bdba >> 6) & 0x3F]);
        append(translationMap[bdba & 0x3F]);
        append(translationMap[(slot >> 12) & 0x3F]);
        append(translationMap[(slot >> 6) & 0x3F]);
        append(translationMap[slot & 0x3F]);
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
            //objects;

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
                case 1:
                    append("\"varchar2\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 2:
                    append("\"numeric\",\"precision\":");
                    appendSDec(object->columns[i]->precision);
                    append(",\"scale\":");
                    appendSDec(object->columns[i]->scale);
                    break;

                case 12:
                    append("\"date\"");
                    break;

                case 23:
                    append("\"raw\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 96:
                    append("\"char\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 100:
                    append("\"binary_float\"");
                    break;

                case 101:
                    append("\"binary_double\"");
                    break;

                case 180:
                    append("\"timestamp\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 181:
                    append("\"timestamp with time zone\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                default:
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
        hasPrevious = false;

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
            if (hasPrevious)
                append(',');
            else
                hasPrevious = true;
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

        bool prevValue = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (afterPos[i] > 0 && afterLen[i] > 0)
                appendValue(object->columns[i]->columnName, afterRecord[i], object->columns[i]->typeNo,
                        object->columns[i]->charsetId, afterPos[i], afterLen[i], prevValue);
            else
            if (columnFormat >= COLUMN_FORMAT_INS_DEC || object->columns[i]->numPk > 0)
                appendNull(object->columns[i]->columnName, prevValue);
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
            if (hasPrevious)
                append(',');
            else
                hasPrevious = true;
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

        bool prevValue = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (beforePos[i] > 0 && beforeLen[i] > 0)
                appendValue(object->columns[i]->columnName, beforeRecord[i], object->columns[i]->typeNo,
                        object->columns[i]->charsetId, beforePos[i], beforeLen[i], prevValue);
            else
            if (afterPos[i] > 0 || beforePos[i] > 0)
                appendNull(object->columns[i]->columnName, prevValue);
        }

        append("},\"after\":{");

        prevValue = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (afterPos[i] > 0 && afterLen[i] > 0)
                appendValue(object->columns[i]->columnName, afterRecord[i], object->columns[i]->typeNo,
                        object->columns[i]->charsetId, afterPos[i], afterLen[i], prevValue);
            else
            if (afterPos[i] > 0 || beforePos[i] > 0)
                appendNull(object->columns[i]->columnName, prevValue);
        }
        append("}}");

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            append("]}");
            commitMessage();
        }
    }

    void OutputBufferJson::processDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (hasPrevious)
                append(',');
            else
                hasPrevious = true;
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

        bool prevValue = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            //value present before
            if (beforePos[i] > 0 && beforeLen[i] > 0)
                appendValue(object->columns[i]->columnName, beforeRecord[i], object->columns[i]->typeNo,
                        object->columns[i]->charsetId, beforePos[i], beforeLen[i], prevValue);
            else
            if (columnFormat >= COLUMN_FORMAT_INS_DEC || object->columns[i]->numPk > 0)
                appendNull(object->columns[i]->columnName, prevValue);
        }
        append("}}");

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            append("]}");
            commitMessage();
        }
    }

    void OutputBufferJson::processDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const uint8_t *sql, uint64_t sqlLength) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (hasPrevious)
                append(',');
            else
                hasPrevious = true;
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
