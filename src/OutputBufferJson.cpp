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

    OutputBufferJson::OutputBufferJson(uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat, uint64_t unknownFormat, uint64_t showColumns) :
            OutputBuffer(timestampFormat, charFormat, scnFormat, unknownFormat, showColumns) {
    }

    OutputBufferJson::~OutputBufferJson() {
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
        if (characterSet == nullptr && (charFormat & 1 == 0)) {
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

    void OutputBufferJson::appendScn(typescn scn) {
        if (scnFormat == SCN_FORMAT_HEX) {
            appendChr("\"scn\":\"0x");
            appendHex(scn, 16);
            append('"');
        } else {
            appendChr("\"scn\":");
            string scnStr = to_string(scn);
            appendStr(scnStr);
        }
    }

    void OutputBufferJson::appendOperation(const char *operation) {
        appendChr("\"operation\":\"");
        appendChr(operation);
        append('"');
    }

    void OutputBufferJson::appendTable(string &owner, string &table) {
        appendChr("\"table\":\"");
        appendStr(owner);
        append('.');
        appendStr(table);
        append('"');
    }

    void OutputBufferJson::appendNull(string &columnName, bool &prevValue) {
        if (prevValue)
            append(',');
        else
            prevValue = true;

        append('"');
        appendStr(columnName);
        appendChr("\":null");
    }

    void OutputBufferJson::appendMs(const char *name, uint64_t time) {
        append('"');
        appendChr(name);
        appendChr("\":");
        appendDec(time);
    }

    void OutputBufferJson::appendXid(typexid xid) {
        appendChr("\"xid\":\"");
        appendDec(USN(xid));
        append('.');
        appendDec(SLT(xid));
        append('.');
        appendDec(SQN(xid));
        append('"');
    }

    void OutputBufferJson::appendUnknown(string &columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t fieldPos, uint64_t fieldLength) {
        appendChr("\"?\"");
        stringstream ss;
        for (uint64_t j = 0; j < fieldLength; ++j)
            ss << " " << hex << setfill('0') << setw(2) << (uint64_t) redoLogRecord->data[fieldPos + j];
        WARNING("unknown value (table: " << redoLogRecord->object->owner << "." << redoLogRecord->object->objectName << " column: " << columnName << " type: " << dec << typeNo << "): " << dec << fieldLength << " - " << ss.str());
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
        appendStr(columnName);
        appendChr("\":");

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
                appendStr(valString);
            } else
                appendUnknown(columnName, redoLogRecord, typeNo, fieldPos, fieldLength);
            break;

        case 101: //binary_double
            if (fieldLength == 8) {
                stringstream valStringStream;
                double *valDouble = (double *)redoLogRecord->data + fieldPos;
                valStringStream << *valDouble;
                string valString = valStringStream.str();
                appendStr(valString);
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
                            appendChr("TZ?");
                        else
                            appendChr(tz);
                    }
                }

                append('"');
            }
            break;

        default:
            if (unknownFormat == UNKNOWN_FORMAT_DUMP)
                appendUnknown(columnName, redoLogRecord, typeNo, fieldPos, fieldLength);
            else
                appendChr("\"?\"");
        }
    }

    void OutputBufferJson::appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot) {
        uint32_t afn =  bdba >> 22;
        bdba &= 0x003FFFFF;
        appendChr("\"rowid\":\"");
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

    void OutputBufferJson::appendInsert(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        append('{');
        appendOperation("insert");
        append(',');
        appendTable(object->owner, object->objectName);
        append(',');
        appendRowid(object->objn, object->objd, bdba, slot);
        appendChr(",\"after\":{");

        bool prevValue = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            if (afterPos[i] > 0 && afterLen[i] > 0)
                appendValue(object->columns[i]->columnName, afterRecord[i], object->columns[i]->typeNo,
                        object->columns[i]->charsetId, afterPos[i], afterLen[i], prevValue);
            else
            if (showColumns >= 1 || object->columns[i]->numPk > 0)
                appendNull(object->columns[i]->columnName, prevValue);
        }

        appendChr("}}");
    }

    void OutputBufferJson::appendUpdate(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        OutputBuffer::appendUpdate(object, bdba, slot, xid);

        append('{');
        appendOperation("update");
        append(',');
        appendTable(object->owner, object->objectName);
        append(',');
        appendRowid(object->objn, object->objd, bdba, slot);
        appendChr(",\"before\":{");

        bool prevValue = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            //value present before
            if (beforePos[i] > 0 && beforeLen[i] > 0)
                appendValue(object->columns[i]->columnName, beforeRecord[i], object->columns[i]->typeNo,
                        object->columns[i]->charsetId, beforePos[i], beforeLen[i], prevValue);
            else
            if (afterPos[i] > 0 || beforePos[i] > 0)
                appendNull(object->columns[i]->columnName, prevValue);
        }

        appendChr("},\"after\":{");

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

        appendChr("}}");
    }

    void OutputBufferJson::appendDelete(OracleObject *object, typedba bdba, typeslot slot, typexid xid) {
        append('{');
        appendOperation("delete");
        append(',');
        appendTable(object->owner, object->objectName);
        append(',');
        appendRowid(object->objn, object->objd, bdba, slot);
        appendChr(",\"before\":{");

        bool prevValue = false;
        for (uint64_t i = 0; i < object->maxSegCol; ++i) {
            if (object->columns[i] == nullptr)
                continue;

            //value present before
            if (beforePos[i] > 0 && beforeLen[i] > 0)
                appendValue(object->columns[i]->columnName, beforeRecord[i], object->columns[i]->typeNo,
                        object->columns[i]->charsetId, beforePos[i], beforeLen[i], prevValue);
            else
            if (showColumns >= 1 || object->columns[i]->numPk > 0)
                appendNull(object->columns[i]->columnName, prevValue);
        }

        appendChr("}}");
    }

    void OutputBufferJson::appendDDL(OracleObject *object, uint16_t type, uint16_t seq, const char *operation, const uint8_t *sql, uint64_t sqlLength) {
        append('{');
        appendTable(object->owner, object->objectName);
        appendChr(",\"type\":");
        appendDec(type);
        appendChr(",\"seq\":");
        appendDec(seq);
        append(',');
        appendOperation(operation);
        appendChr(",\"sql\":\"");
        appendEscape(sql, sqlLength);
        appendChr("\"}");
    }

    void OutputBufferJson::next(void) {
        append(',');
    }

    void OutputBufferJson::beginTran(typescn scn, typetime time, typexid xid) {
        OutputBuffer::beginTran(scn, time, xid);

        append('{');
        appendScn(scn);
        append(',');
        appendMs("timestamp", time.toTime() * 1000);
        append(',');
        appendXid(xid);
        appendChr(",\"dml\":[");
    }

    void OutputBufferJson::commitTran(void) {
        appendChr("]}");
        commitMessage();
    }
}
