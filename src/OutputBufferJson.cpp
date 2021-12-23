/* Memory buffer for handling output data in JSON format
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

#include "OracleAnalyzer.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "OutputBufferJson.h"
#include "RowId.h"

namespace OpenLogReplicator {
    OutputBufferJson::OutputBufferJson(uint64_t messageFormat, uint64_t ridFormat, uint64_t xidFormat, uint64_t timestampFormat,
            uint64_t charFormat, uint64_t scnFormat, uint64_t unknownFormat, uint64_t schemaFormat, uint64_t columnFormat, uint64_t unknownType,
            uint64_t flushBuffer) :
        OutputBuffer(messageFormat, ridFormat, xidFormat, timestampFormat, charFormat, scnFormat, unknownFormat, schemaFormat, columnFormat,
                unknownType, flushBuffer),
        hasPreviousValue(false),
        hasPreviousRedo(false),
        hasPreviousColumn(false) {
    }

    OutputBufferJson::~OutputBufferJson() {
    }

    void OutputBufferJson::columnNull(OracleObject* object, typeCOL col) {
        if (object != nullptr && unknownType == UNKNOWN_TYPE_HIDE) {
            OracleColumn* column = object->columns[col];
            if (column->storedAsLob)
                return;
            if (column->constraint && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS) == 0)
                return;
            if (column->nested && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_NESTED_COLUMNS) == 0)
                return;
            if (column->invisible && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_INVISIBLE_COLUMNS) == 0)
                return;
            if (column->unused && (oracleAnalyzer->flags & REDO_FLAGS_SHOW_UNUSED_COLUMNS) == 0)
                return;

            uint64_t typeNo = object->columns[col]->typeNo;
            if (typeNo != 1 //varchar2/nvarchar2
                    && typeNo != 96 //char/nchar
                    && typeNo != 2 //number/float
                    && typeNo != 12 //date
                    && typeNo != 180 //timestamp
                    && typeNo != 23 //raw
                    && typeNo != 100 //binary_float
                    && typeNo != 101 //binary_double
                    && typeNo != 181) //timestamp with time zone
                return;
        }

        if (hasPreviousColumn)
            outputBufferAppend(',');
        else
            hasPreviousColumn = true;

        outputBufferAppend('"');
        if (object != nullptr)
            outputBufferAppend(object->columns[col]->name);
        else {
            std::string columnName("COL_" + std::to_string(col));
            outputBufferAppend(columnName);
        }
        outputBufferAppend("\":null");
    }

    void OutputBufferJson::columnFloat(std::string& columnName, float value) {
        if (hasPreviousColumn)
            outputBufferAppend(',');
        else
            hasPreviousColumn = true;

        outputBufferAppend('"');
        outputBufferAppend(columnName);
        outputBufferAppend("\":");

        std::string valString(std::to_string(value));
        outputBufferAppend(valString);
    }

    void OutputBufferJson::columnDouble(std::string& columnName, double value) {
        if (hasPreviousColumn)
            outputBufferAppend(',');
        else
            hasPreviousColumn = true;

        outputBufferAppend('"');
        outputBufferAppend(columnName);
        outputBufferAppend("\":");

        std::string valString(std::to_string(value));
        outputBufferAppend(valString);
    }

    void OutputBufferJson::columnString(std::string& columnName) {
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

    void OutputBufferJson::columnNumber(std::string& columnName, uint64_t precision, uint64_t scale) {
        if (hasPreviousColumn)
            outputBufferAppend(',');
        else
            hasPreviousColumn = true;

        outputBufferAppend('"');
        outputBufferAppend(columnName);
        outputBufferAppend("\":");
        outputBufferAppend(valueBuffer, valueLength);
    }

    void OutputBufferJson::columnRaw(std::string& columnName, const uint8_t* data, uint64_t length) {
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

    void OutputBufferJson::columnTimestamp(std::string& columnName, struct tm &epochTime, uint64_t fraction, const char* tz) {
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
            if (epochTime.tm_year > 0) {
                appendDec((uint64_t)epochTime.tm_year);
            } else {
                appendDec((uint64_t)(-epochTime.tm_year));
                outputBufferAppend("BC");
            }
            outputBufferAppend('-');
            appendDec(epochTime.tm_mon, 2);
            outputBufferAppend('-');
            appendDec(epochTime.tm_mday, 2);
            outputBufferAppend('T');
            appendDec(epochTime.tm_hour, 2);
            outputBufferAppend(':');
            appendDec(epochTime.tm_min, 2);
            outputBufferAppend(':');
            appendDec(epochTime.tm_sec, 2);

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
            if (epochTime.tm_year >= 1900) {
                --epochTime.tm_mon;
                epochTime.tm_year -= 1900;
                appendDec(tmToEpoch(&epochTime) * 1000 + ((fraction + 500000) / 1000000));
            } else
                appendDec(0);
        }
    }

    void OutputBufferJson::appendRowid(typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot) {
        if ((messageFormat & MESSAGE_FORMAT_ADD_SEQUENCES) != 0) {
            outputBufferAppend(",\"num\":");
            appendDec(num);
        }

        if (ridFormat == RID_FORMAT_SKIP)
            return;

        RowId rowId(dataObj, bdba, slot);
        char str[19];
        rowId.toString(str);
        outputBufferAppend(",\"rid\":\"");
        outputBufferAppend(str);
        outputBufferAppend('"');
    }

    void OutputBufferJson::appendHeader(bool first, bool showXid) {
        if (first || (scnFormat & SCN_FORMAT_ALL_PAYLOADS) != 0) {
            if (hasPreviousValue)
                outputBufferAppend(',');
            else
                hasPreviousValue = true;

            if ((scnFormat & SCN_FORMAT_HEX) != 0) {
                outputBufferAppend("\"scns\":\"0x");
                appendHex(lastScn, 16);
                outputBufferAppend('"');
            } else {
                outputBufferAppend("\"scn\":");
                appendDec(lastScn);
            }
        }

        if (first || (timestampFormat & TIMESTAMP_FORMAT_ALL_PAYLOADS) != 0) {
            if (hasPreviousValue)
                outputBufferAppend(',');
            else
                hasPreviousValue = true;

            if ((timestampFormat & TIMESTAMP_FORMAT_ISO8601) != 0) {
                outputBufferAppend("\"tms\":\"");
                char iso[21];
                lastTime.toISO8601(iso);
                outputBufferAppend(iso);
                outputBufferAppend('"');
            } else {
                outputBufferAppend("\"tm\":");
                appendDec(lastTime.toTime() * 1000);
            }
        }

        if (showXid) {
            if (hasPreviousValue)
                outputBufferAppend(',');
            else
                hasPreviousValue = true;

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
    }

    void OutputBufferJson::appendSchema(OracleObject* object, typeDATAOBJ dataObj) {
        if (object == nullptr) {
            outputBufferAppend("\"schema\":{\"table\":\"");
            std::string objectName("OBJ_" + std::to_string(dataObj));
            outputBufferAppend(objectName);
            outputBufferAppend('"}');
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
            for (typeCOL column = 0; column < object->columns.size(); ++column) {
                if (object->columns[column] == nullptr)
                    continue;

                if (hasPrev)
                    outputBufferAppend(',');
                else
                    hasPrev = true;

                outputBufferAppend("{\"name\":\"");
                outputBufferAppend(object->columns[column]->name);

                outputBufferAppend("\",\"type\":");
                switch(object->columns[column]->typeNo) {
                case 1: //varchar2(n), nvarchar(n)
                    outputBufferAppend("\"varchar2\",\"length\":");
                    appendDec(object->columns[column]->length);
                    break;

                case 2: //number(p, s), float(p)
                    outputBufferAppend("\"number\",\"precision\":");
                    appendSDec(object->columns[column]->precision);
                    outputBufferAppend(",\"scale\":");
                    appendSDec(object->columns[column]->scale);
                    break;

                case 8: //long, not supported
                    outputBufferAppend("\"long\"");
                    break;

                case 12: //date
                    outputBufferAppend("\"date\"");
                    break;

                case 23: //raw(n)
                    outputBufferAppend("\"raw\",\"length\":");
                    appendDec(object->columns[column]->length);
                    break;

                case 24: //long raw, not supported
                    outputBufferAppend("\"long raw\"");
                    break;

                case 69: //rowid, not supported
                    outputBufferAppend("\"rowid\"");
                    break;

                case 96: //char(n), nchar(n)
                    outputBufferAppend("\"char\",\"length\":");
                    appendDec(object->columns[column]->length);
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
                    appendDec(object->columns[column]->length);
                    break;

                case 181: //timestamp with time zone(n)
                    outputBufferAppend("\"timestamp with time zone\",\"length\":");
                    appendDec(object->columns[column]->length);
                    break;

                case 182: //interval year to month(n)
                    outputBufferAppend("\"interval year to month\",\"length\":");
                    appendDec(object->columns[column]->length);
                    break;

                case 183: //interval day to second(n)
                    outputBufferAppend("\"interval day to second\",\"length\":");
                    appendDec(object->columns[column]->length);
                    break;

                case 208: //urawid(n)
                    outputBufferAppend("\"urawid\",\"length\":");
                    appendDec(object->columns[column]->length);
                    break;

                case 231: //timestamp with local time zone(n), not supported
                    outputBufferAppend("\"timestamp with local time zone\",\"length\":");
                    appendDec(object->columns[column]->length);
                    break;

                default:
                    outputBufferAppend("\"unknown\"");
                    break;
                }

                outputBufferAppend(",\"nullable\":");
                if (object->columns[column]->nullable)
                    outputBufferAppend('1');
                else
                    outputBufferAppend('0');

                outputBufferAppend("}");
            }
            outputBufferAppend(']');
        }

        outputBufferAppend('}');
    }

    time_t OutputBufferJson::tmToEpoch(struct tm* epoch) const {
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

    void OutputBufferJson::processBegin(void) {
        newTran = false;
        hasPreviousRedo = false;

        if ((messageFormat & MESSAGE_FORMAT_SKIP_BEGIN) != 0)
            return;

        outputBufferBegin(0);
        outputBufferAppend('{');
        hasPreviousValue = false;
        appendHeader(true, true);

        if (hasPreviousValue)
            outputBufferAppend(',');
        else
            hasPreviousValue = true;

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            outputBufferAppend("\"payload\":[");
        } else {
            outputBufferAppend("\"payload\":[{\"op\":\"begin\"}]}");
            outputBufferCommit(false);
        }
    }

    void OutputBufferJson::processCommit(void) {
        //skip empty transaction
        if (newTran) {
            newTran = false;
            return;
        }

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
            outputBufferAppend("]}");
            outputBufferCommit(true);
        } else
        if ((messageFormat & MESSAGE_FORMAT_SKIP_COMMIT) == 0) {
            outputBufferBegin(0);
            outputBufferAppend('{');

            hasPreviousValue = false;
            appendHeader(false, true);

            if (hasPreviousValue)
                outputBufferAppend(',');
            else
                hasPreviousValue = true;

            outputBufferAppend("\"payload\":[{\"op\":\"commit\"}]}");
            outputBufferCommit(true);
        }
        num = 0;
    }

    void OutputBufferJson::processInsert(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (newTran)
            processBegin();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
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
            hasPreviousValue = false;
            appendHeader(false, true);

            if (hasPreviousValue)
                outputBufferAppend(',');
            else
                hasPreviousValue = true;

            outputBufferAppend("\"payload\":[");
        }

        outputBufferAppend("{\"op\":\"c\",");
        appendSchema(object, dataObj);
        appendRowid(dataObj, bdba, slot);
        appendAfter(object);
        outputBufferAppend('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            outputBufferAppend("]}");
            outputBufferCommit(false);
        }
        ++num;
    }

    void OutputBufferJson::processUpdate(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (newTran)
            processBegin();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
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
            hasPreviousValue = false;
            appendHeader(false, true);

            if (hasPreviousValue)
                outputBufferAppend(',');
            else
                hasPreviousValue = true;

            outputBufferAppend("\"payload\":[");
        }

        outputBufferAppend("{\"op\":\"u\",");
        appendSchema(object, dataObj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(object);
        appendAfter(object);
        outputBufferAppend('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            outputBufferAppend("]}");
            outputBufferCommit(false);
        }
        ++num;
    }

    void OutputBufferJson::processDelete(OracleObject* object, typeDATAOBJ dataObj, typeDBA bdba, typeSLOT slot, typeXID xid) {
        if (newTran)
            processBegin();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
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
            hasPreviousValue = false;
            appendHeader(false, true);

            if (hasPreviousValue)
                outputBufferAppend(',');
            else
                hasPreviousValue = true;

            outputBufferAppend("\"payload\":[");
        }

        outputBufferAppend("{\"op\":\"d\",");
        appendSchema(object, dataObj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(object);
        outputBufferAppend('}');

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            outputBufferAppend("]}");
            outputBufferCommit(false);
        }
        ++num;
    }

    void OutputBufferJson::processDDL(OracleObject* object, typeDATAOBJ dataObj, uint16_t type, uint16_t seq, const char* operation, const char* sql, uint64_t sqlLength) {
        if (newTran)
            processBegin();

        if ((messageFormat & MESSAGE_FORMAT_FULL) != 0) {
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
            hasPreviousValue = false;
            appendHeader(false, true);

            if (hasPreviousValue)
                outputBufferAppend(',');
            else
                hasPreviousValue = true;

            outputBufferAppend("\"payload\":[");
        }

        outputBufferAppend("{\"op\":\"ddl\",\"sql\":\"");
        appendEscape(sql, sqlLength);
        outputBufferAppend("\"}");

        if ((messageFormat & MESSAGE_FORMAT_FULL) == 0) {
            outputBufferAppend("]}");
            outputBufferCommit(true);
        }
        ++num;
    }

    void OutputBufferJson::processCheckpoint(typeSCN scn, typeTIME time_, typeSEQ sequence, uint64_t offset, bool redo) {
        lastTime = time_;
        lastScn = scn;
        lastSequence = sequence;
        outputBufferBegin(0);
        outputBufferAppend('{');
        hasPreviousValue = false;
        appendHeader(true, false);

        if (hasPreviousValue)
            outputBufferAppend(',');
        else
            hasPreviousValue = true;

        outputBufferAppend("\"payload\":[{\"op\":\"chkpt\",\"seq\":");
        appendDec(sequence);
        outputBufferAppend(",\"offset\":");
        appendDec(offset);
        if (redo)
            outputBufferAppend(",\"redo\":true");
        outputBufferAppend("}]}");
        outputBufferCommit(true);
    }
}
