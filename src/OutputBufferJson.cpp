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

#include "OracleAnalyzer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OutputBufferJson.h"
#include "RowId.h"

namespace OpenLogReplicator {

    OutputBufferJson::OutputBufferJson(uint64_t messageFormat, uint64_t xidFormat, uint64_t timestampFormat, uint64_t charFormat, uint64_t scnFormat,
            uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat) :
            OutputBuffer(messageFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat, schemaFormat, columnFormat),
            hasPreviousRedo(false),
            hasPreviousColumn(false) {
    }

    OutputBufferJson::~OutputBufferJson() {
    }

    void OutputBufferJson::columnNull(OracleObject *object, typeCOL col) {
        if (hasPreviousColumn)
            outputBufferAppend(',');
        else
            hasPreviousColumn = true;

        outputBufferAppend('"');
        if (object != nullptr)
            outputBufferAppend(object->columns[col]->name);
        else {
            string columnName = "COL_" + to_string(col);
            outputBufferAppend(columnName);
        }
        outputBufferAppend("\":null");
    }

    void OutputBufferJson::columnFloat(string &columnName, float value) {
        if (hasPreviousColumn)
            outputBufferAppend(',');
        else
            hasPreviousColumn = true;

        outputBufferAppend('"');
        outputBufferAppend(columnName);
        outputBufferAppend("\":");

        stringstream valStringStream;
        valStringStream << value;
        string valString = valStringStream.str();
        outputBufferAppend(valString);
    }

    void OutputBufferJson::columnDouble(string &columnName, double value) {
        if (hasPreviousColumn)
            outputBufferAppend(',');
        else
            hasPreviousColumn = true;

        outputBufferAppend('"');
        outputBufferAppend(columnName);
        outputBufferAppend("\":");

        stringstream valStringStream;
        valStringStream << value;
        string valString = valStringStream.str();
        outputBufferAppend(valString);
    }

    void OutputBufferJson::columnString(string &columnName) {
        if (hasPreviousColumn)
            outputBufferAppend(',');
        else
            hasPreviousColumn = true;

        outputBufferAppend('"');
        outputBufferAppend(columnName);
        outputBufferAppend("\":\"");
        appendEscape(valueBuffer, valueLength);
        outputBufferAppend('"');
    }

    void OutputBufferJson::columnNumber(string &columnName, uint64_t precision, uint64_t scale) {
        if (hasPreviousColumn)
            outputBufferAppend(',');
        else
            hasPreviousColumn = true;

        outputBufferAppend('"');
        outputBufferAppend(columnName);
        outputBufferAppend("\":");
        outputBufferAppend(valueBuffer, valueLength);
    }

    void OutputBufferJson::columnRaw(string &columnName, const uint8_t *data, uint64_t length) {
        if (hasPreviousColumn)
            outputBufferAppend(',');
        else
            hasPreviousColumn = true;

        outputBufferAppend('"');
        outputBufferAppend(columnName);
        outputBufferAppend("\":\"");
        for (uint64_t j = 0; j < length; ++j)
            appendHex(*(data + j), 2);
        outputBufferAppend('"');
    }

    void OutputBufferJson::columnTimestamp(string &columnName, struct tm &epochtime, uint64_t fraction, const char *tz) {
        if (hasPreviousColumn)
            outputBufferAppend(',');
        else
            hasPreviousColumn = true;

        outputBufferAppend('"');
        outputBufferAppend(columnName);
        outputBufferAppend("\":");

        if ((timestampFormat & TIMESTAMP_FORMAT_ISO8601) != 0) {
            //2012-04-23T18:25:43.511Z - ISO 8601 format
            outputBufferAppend('"');
            if (epochtime.tm_year > 0) {
                appendDec((uint64_t)epochtime.tm_year);
            } else {
                appendDec((uint64_t)(-epochtime.tm_year));
                outputBufferAppend("BC");
            }
            outputBufferAppend('-');
            appendDec(epochtime.tm_mon, 2);
            outputBufferAppend('-');
            appendDec(epochtime.tm_mday, 2);
            outputBufferAppend('T');
            appendDec(epochtime.tm_hour, 2);
            outputBufferAppend(':');
            appendDec(epochtime.tm_min, 2);
            outputBufferAppend(':');
            appendDec(epochtime.tm_sec, 2);

            if (fraction > 0) {
                outputBufferAppend('.');
                appendDec(fraction, 9);
            }

            if (tz != nullptr) {
                outputBufferAppend(' ');
                outputBufferAppend(tz);
            }
            outputBufferAppend('"');
        } else {
            //unix epoch format
            if (epochtime.tm_year >= 1900) {
                --epochtime.tm_mon;
                epochtime.tm_year -= 1900;
                appendDec(tmToEpoch(&epochtime) * 1000 + ((fraction + 500000) / 1000000));
            } else
                appendDec(0);
        }
    }

    void OutputBufferJson::appendRowid(typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot) {
        RowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        outputBufferAppend("\"rid\":\"");
        outputBufferAppend(str);
        outputBufferAppend('"');
    }

    void OutputBufferJson::appendHeader(bool first) {
        if (first || (scnFormat & SCN_FORMAT_ALL_PAYLOADS) != 0) {
            if ((scnFormat & SCN_FORMAT_HEX) != 0) {
                outputBufferAppend("\"scns\":\"0x");
                appendHex(lastScn, 16);
                outputBufferAppend("\",");
            } else {
                outputBufferAppend("\"scn\":");
                appendDec(lastScn);
                outputBufferAppend(',');
            }
        }

        if (first || (timestampFormat & TIMESTAMP_FORMAT_ALL_PAYLOADS) != 0) {
            if ((timestampFormat & TIMESTAMP_FORMAT_ISO8601) != 0) {
                outputBufferAppend("\"tms\":\"");
                char iso[21];
                lastTime.toISO8601(iso);
                outputBufferAppend(iso);
                outputBufferAppend("\",");
            } else {
                outputBufferAppend("\"tm\":");
                appendDec(lastTime.toTime() * 1000);
                outputBufferAppend(',');
            }
        }

        if (xidFormat == XID_FORMAT_TEXT) {
            outputBufferAppend("\"xid\":\"");
            appendDec(USN(lastXid));
            outputBufferAppend('.');
            appendDec(SLT(lastXid));
            outputBufferAppend('.');
            appendDec(SQN(lastXid));
            outputBufferAppend('"');
        } else {
            outputBufferAppend("\"xidn\":");
            appendDec(lastXid);
        }
    }

    void OutputBufferJson::appendSchema(OracleObject *object, typeDATAOBJ dataObj) {
        if (object == nullptr) {
            outputBufferAppend("\"schema\":{\"table\":\"");
            string objectName = "OBJ_" + to_string(dataObj);
            outputBufferAppend(objectName);
            outputBufferAppend('"');
            return;
        }

        outputBufferAppend("\"schema\":{\"owner\":\"");
        outputBufferAppend(object->owner);
        outputBufferAppend("\",\"table\":\"");
        outputBufferAppend(object->name);
        outputBufferAppend('"');

        if ((schemaFormat & SCHEMA_FORMAT_OBJ) != 0) {
            outputBufferAppend(",\"obj\":");
            appendDec(object->obj);
        }

        if ((schemaFormat & SCHEMA_FORMAT_FULL) != 0) {
            if ((schemaFormat & SCHEMA_FORMAT_REPEATED) == 0) {
                if (objects.count(object) > 0)
                    return;
                else
                    objects.insert(object);
            }

            outputBufferAppend(",\"columns\":[");

            bool hasPrev = false;
            for (uint64_t i = 0; i < object->columns.size(); ++i) {
                if (object->columns[i] == nullptr)
                    continue;

                if (hasPrev)
                    outputBufferAppend(',');
                else
                    hasPrev = true;

                outputBufferAppend("{\"name\":\"");
                outputBufferAppend(object->columns[i]->name);

                outputBufferAppend("\",\"type\":");
                switch(object->columns[i]->typeNo) {
                case 1: //varchar2(n), nvarchar(n)
                    outputBufferAppend("\"varchar2\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 2: //number(p, s), float(p)
                    outputBufferAppend("\"number\",\"precision\":");
                    appendSDec(object->columns[i]->precision);
                    outputBufferAppend(",\"scale\":");
                    appendSDec(object->columns[i]->scale);
                    break;

                case 8: //long, not supported
                    outputBufferAppend("\"long\"");
                    break;

                case 12: //date
                    outputBufferAppend("\"date\"");
                    break;

                case 23: //raw(n)
                    outputBufferAppend("\"raw\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 24: //long raw, not supported
                    outputBufferAppend("\"long raw\"");
                    break;

                case 69: //rowid, not supported
                    outputBufferAppend("\"rowid\"");
                    break;

                case 96: //char(n), nchar(n)
                    outputBufferAppend("\"char\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 100: //binary_float
                    outputBufferAppend("\"binary_float\"");
                    break;

                case 101: //binary_double
                    outputBufferAppend("\"binary_double\"");
                    break;

                case 112: //clob, nclob, not supported
                    outputBufferAppend("\"clob\"");
                    break;

                case 113: //blob, not supported
                    outputBufferAppend("\"blob\"");
                    break;

                case 180: //timestamp(n)
                    outputBufferAppend("\"timestamp\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 181: //timestamp with time zone(n)
                    outputBufferAppend("\"timestamp with time zone\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 182: //interval year to month(n)
                    outputBufferAppend("\"interval year to month\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 183: //interval day to second(n)
                    outputBufferAppend("\"interval day to second\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 208: //urawid(n)
                    outputBufferAppend("\"urawid\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                case 231: //timestamp with local time zone(n), not supported
                    outputBufferAppend("\"timestamp with local time zone\",\"length\":");
                    appendDec(object->columns[i]->length);
                    break;

                default:
                    outputBufferAppend("\"unknown\"");
                    break;
                }

                outputBufferAppend(",\"nullable\":");
                if (object->columns[i]->nullable)
                    outputBufferAppend('1');
                else
                    outputBufferAppend('0');

                outputBufferAppend("}");
            }
            outputBufferAppend(']');
        }

        outputBufferAppend('}');
    }

    void OutputBufferJson::appendHex(uint64_t value, uint64_t length) {
        uint64_t j = (length - 1) * 4;
        for (uint64_t i = 0; i < length; ++i) {
            outputBufferAppend(map16[(value >> j) & 0xF]);
            j -= 4;
        };
    }

    void OutputBufferJson::appendDec(uint64_t value, uint64_t length) {
        char buffer[21];

        for (uint i = 0; i < length; ++i) {
            buffer[i] = '0' + (value % 10);
            value /= 10;
        }

        for (uint64_t i = 0; i < length; ++i)
            outputBufferAppend(buffer[length - i - 1]);
    }

    void OutputBufferJson::appendDec(uint64_t value) {
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

    void OutputBufferJson::appendSDec(int64_t value) {
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

    void OutputBufferJson::appendEscape(const char *str, uint64_t length) {
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
            } else {
                if (*str == '"' || *str == '\\' || *str == '/')
                    outputBufferAppend('\\');
                outputBufferAppend(*(str++));
            }
            --length;
        }
    }

    time_t OutputBufferJson::tmToEpoch(struct tm *epoch) const {
        static const int cumdays[12] = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };
        uint64_t year;
        time_t result;

        year = 1900 + epoch->tm_year + epoch->tm_mon / 12;
        result = (year - 1970) * 365 + cumdays[epoch->tm_mon % 12];
        result += (year - 1968) / 4;
        result -= (year - 1900) / 100;
        result += (year - 1600) / 400;
        if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0) &&
            (epoch->tm_mon % 12) < 2)
            result--;
        result += epoch->tm_mday - 1;
        result *= 24;
        result += epoch->tm_hour;
        result *= 60;
        result += epoch->tm_min;
        result *= 60;
        result += epoch->tm_sec;
        return result;
    }

    void OutputBufferJson::processBegin(typeSCN scn, typetime time, typeXID xid) {
        lastTime = time;
        lastScn = scn;
        lastXid = xid;
        hasPreviousRedo = false;

        outputBufferBegin(0);
        outputBufferAppend('{');
        appendHeader(true);

        if (messageFormat == MESSAGE_FORMAT_FULL)
            outputBufferAppend(",\"payload\":[");
        else {
            outputBufferAppend(",\"payload\":[{\"op\":\"begin\"}]}");
            outputBufferCommit();
        }
    }

    void OutputBufferJson::processCommit(void) {
        if (messageFormat == MESSAGE_FORMAT_FULL)
            outputBufferAppend("]}");
        else {
            outputBufferBegin(0);
            outputBufferAppend('{');
            appendHeader(false);
            outputBufferAppend(",\"payload\":[{\"op\":\"commit\"}]}");
        }
        outputBufferCommit();
    }

    void OutputBufferJson::processInsert(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (hasPreviousRedo)
                outputBufferAppend(',');
            else
                hasPreviousRedo = true;
        } else {
            if (object != nullptr)
                outputBufferBegin(object->obj);
            else
                outputBufferBegin(0);
            outputBufferAppend('{');
            appendHeader(false);
            outputBufferAppend(",\"payload\":[");
        }

        outputBufferAppend("{\"op\":\"c\",");
        appendSchema(object, dataObj);
        outputBufferAppend(',');
        appendRowid(dataObj, bdba, slot);
        outputBufferAppend(",\"after\":{");

        hasPreviousColumn = false;
        for (auto it = valuesMap.cbegin(); it != valuesMap.cend(); ++it) {
            uint16_t i = (*it).first;
            uint16_t pos = (*it).second;

            if (object != nullptr) {
                if (object->columns[i]->constraint && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS) == 0)
                    continue;
                if (object->columns[i]->invisible && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_INVISIBLE_COLUMNS) == 0)
                    continue;

                if (values[pos][VALUE_AFTER].data[0] != nullptr && values[pos][VALUE_AFTER].length[0] > 0)
                    processValue(object, i, values[pos][VALUE_AFTER].data[0], values[pos][VALUE_AFTER].length[0], object->columns[i]->typeNo, object->columns[i]->charsetId);
                else
                if (columnFormat >= COLUMN_FORMAT_INS_DEC || object->columns[i]->numPk > 0)
                    columnNull(object, i);
            } else {
                if (values[pos][VALUE_AFTER].data[0] != nullptr && values[pos][VALUE_AFTER].length[0] > 0)
                    processValue(nullptr, i, values[pos][VALUE_AFTER].data[0], values[pos][VALUE_AFTER].length[0], 0, 0);
                else
                if (columnFormat >= COLUMN_FORMAT_INS_DEC)
                    columnNull(nullptr, i);
            }
        }
        outputBufferAppend("}}");

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            outputBufferAppend("]}");
            outputBufferCommit();
        }
    }

    void OutputBufferJson::processUpdate(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (hasPreviousRedo)
                outputBufferAppend(',');
            else
                hasPreviousRedo = true;
        } else {
            if (object != nullptr)
                outputBufferBegin(object->obj);
            else
                outputBufferBegin(0);
            outputBufferAppend('{');
            appendHeader(false);
            outputBufferAppend(",\"payload\":[");
        }

        outputBufferAppend("{\"op\":\"u\",");
        appendSchema(object, dataObj);
        outputBufferAppend(',');
        appendRowid(dataObj, bdba, slot);
        outputBufferAppend(",\"before\":{");

        hasPreviousColumn = false;
        for (auto it = valuesMap.cbegin(); it != valuesMap.cend(); ++it) {
            uint16_t i = (*it).first;
            uint16_t pos = (*it).second;

            if (object != nullptr) {
                if (object->columns[i]->constraint && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS) == 0)
                    continue;
                if (object->columns[i]->invisible && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_INVISIBLE_COLUMNS) == 0)
                    continue;

                if (values[pos][VALUE_BEFORE].data[0] != nullptr && values[pos][VALUE_BEFORE].length[0] > 0)
                    processValue(object, i, values[pos][VALUE_BEFORE].data[0], values[pos][VALUE_BEFORE].length[0], object->columns[i]->typeNo, object->columns[i]->charsetId);
                else
                if (values[pos][VALUE_AFTER].data[0] != nullptr || values[pos][VALUE_BEFORE].data[0] != nullptr)
                    columnNull(object, i);
            } else {
                if (values[pos][VALUE_BEFORE].data[0] != nullptr && values[pos][VALUE_BEFORE].length[0] > 0)
                    processValue(nullptr, i, values[pos][VALUE_BEFORE].data[0], values[pos][VALUE_BEFORE].length[0], 0, 0);
                else
                if (values[pos][VALUE_AFTER].data[0] != nullptr || values[pos][VALUE_BEFORE].data[0] != nullptr)
                    columnNull(nullptr, i);
            }
        }

        outputBufferAppend("},\"after\":{");

        hasPreviousColumn = false;
        for (auto it = valuesMap.cbegin(); it != valuesMap.cend(); ++it) {
            uint16_t i = (*it).first;
            uint16_t pos = (*it).second;

            if (object != nullptr) {
                if (object->columns[i]->constraint && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS) == 0)
                    continue;
                if (object->columns[i]->invisible && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_INVISIBLE_COLUMNS) == 0)
                    continue;

                if (values[pos][VALUE_AFTER].data[0] != nullptr && values[pos][VALUE_AFTER].length[0] > 0)
                    processValue(object, i, values[pos][VALUE_AFTER].data[0], values[pos][VALUE_AFTER].length[0], object->columns[i]->typeNo, object->columns[i]->charsetId);
                else
                if (values[pos][VALUE_AFTER].data[0] != nullptr || values[pos][VALUE_BEFORE].data[0] != nullptr)
                    columnNull(object, i);
            } else {
                if (values[pos][VALUE_AFTER].data[0] != nullptr && values[pos][VALUE_AFTER].length[0] > 0)
                    processValue(nullptr, i, values[pos][VALUE_AFTER].data[0], values[pos][VALUE_AFTER].length[0], 0, 0);
                else
                if (values[pos][VALUE_AFTER].data[0] != nullptr || values[pos][VALUE_BEFORE].data[0] != nullptr)
                    columnNull(nullptr, i);
            }
        }
        outputBufferAppend("}}");

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            outputBufferAppend("]}");
            outputBufferCommit();
        }
    }

    void OutputBufferJson::processDelete(OracleObject *object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (hasPreviousRedo)
                outputBufferAppend(',');
            else
                hasPreviousRedo = true;
        } else {
            if (object != nullptr)
                outputBufferBegin(object->obj);
            else
                outputBufferBegin(0);
            outputBufferAppend('{');
            appendHeader(false);
            outputBufferAppend(",\"payload\":[");
        }

        outputBufferAppend("{\"op\":\"d\",");
        appendSchema(object, dataObj);
        outputBufferAppend(',');
        appendRowid(dataObj, bdba, slot);
        outputBufferAppend(",\"before\":{");

        hasPreviousColumn = false;
        for (auto it = valuesMap.cbegin(); it != valuesMap.cend(); ++it) {
            uint16_t i = (*it).first;
            uint16_t pos = (*it).second;

            if (object != nullptr) {
                if (object->columns[i]->constraint && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS) == 0)
                    continue;
                if (object->columns[i]->invisible && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_INVISIBLE_COLUMNS) == 0)
                    continue;

                if (values[pos][VALUE_BEFORE].data[0] != nullptr && values[pos][VALUE_BEFORE].length[0] > 0)
                    processValue(object, i, values[pos][VALUE_BEFORE].data[0], values[pos][VALUE_BEFORE].length[0], object->columns[i]->typeNo, object->columns[i]->charsetId);
                else
                if (columnFormat >= COLUMN_FORMAT_INS_DEC || object->columns[i]->numPk > 0)
                    columnNull(object, i);
            } else {
                if (values[pos][VALUE_BEFORE].data[0] != nullptr && values[pos][VALUE_BEFORE].length[0] > 0)
                    processValue(nullptr, i, values[pos][VALUE_BEFORE].data[0], values[pos][VALUE_BEFORE].length[0], 0, 0);
                else
                if (columnFormat >= COLUMN_FORMAT_INS_DEC)
                    columnNull(nullptr, i);
            }
        }
        outputBufferAppend("}}");

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            outputBufferAppend("]}");
            outputBufferCommit();
        }
    }

    void OutputBufferJson::processDDL(OracleObject *object, typeDATAOBJ dataObj, uint16_t type, uint16_t seq, const char *operation, const char *sql, uint64_t sqlLength) {
        if (messageFormat == MESSAGE_FORMAT_FULL) {
            if (hasPreviousRedo)
                outputBufferAppend(',');
            else
                hasPreviousRedo = true;
        } else {
            if (object != nullptr)
                outputBufferBegin(object->obj);
            else
                outputBufferBegin(0);
            outputBufferAppend('{');
            appendHeader(false);
            outputBufferAppend(",\"payload\":[");
        }

        outputBufferAppend("{\"op\":\"ddl\",");
        appendSchema(object, dataObj);
        outputBufferAppend(",\"sql\":\"");
        appendEscape(sql, sqlLength);
        outputBufferAppend("\"}");

        if (messageFormat == MESSAGE_FORMAT_SHORT) {
            outputBufferAppend("]}");
            outputBufferCommit();
        }
    }
}
