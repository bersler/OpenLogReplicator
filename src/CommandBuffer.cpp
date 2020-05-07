/* Memory buffer for handling JSON data
   Copyright (C) 2018-2020 Adam Leszczynski.

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <iostream>
#include <string.h>

#include "types.h"
#include "CommandBuffer.h"
#include "OracleReader.h"
#include "OracleObject.h"
#include "OracleColumn.h"
#include "RedoLogRecord.h"
#include "MemoryException.h"

namespace OpenLogReplicator {

    CommandBuffer::CommandBuffer(uint64_t outputBufferSize) :
            oracleReader(nullptr),
            shutdown(false),
            writer(nullptr),
            posStart(0),
            posEnd(0),
            posEndTmp(0),
            posSize(0),
            test(0),
            timestampFormat(0),
            outputBufferSize(outputBufferSize) {
        intraThreadBuffer = new uint8_t[outputBufferSize];

        if (intraThreadBuffer == nullptr) {
            cerr << "ERROR: could not allocate memory for output buffer (" << dec << outputBufferSize << " bytes)" << endl;
            throw MemoryException("out of memory");
        }

    }

    void CommandBuffer::stop(void) {
        this->shutdown = true;
    }

    void CommandBuffer::setOracleReader(OracleReader *oracleReader) {
        this->oracleReader = oracleReader;
    }


    CommandBuffer* CommandBuffer::appendEscape(const uint8_t *str, uint64_t length) {
        if (this->shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length * 2 >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (1)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }
            if (this->shutdown)
                return this;
        }

        if (posEndTmp + length * 2 >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (1)" << endl;
            return this;
        }

        while (length > 0) {
            if (*str == '\t') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 't';
            } else if (*str == '\r') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'r';
            } else if (*str == '\n') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'n';
            } else if (*str == '\f') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'f';
            } else if (*str == '\b') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'b';
            } else {
                if (*str == '"' || *str == '\\' || *str == '/')
                    intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = *(str++);
            }
            --length;
        }

        return this;
    }

    CommandBuffer* CommandBuffer::appendHex(uint64_t val, uint64_t length) {
        static const char* digits = "0123456789abcdef";
        if (this->shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (2)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }
        }

        if (posEndTmp + length >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (5)" << endl;
            return this;
        }

        for (uint64_t i = 0, j = (length - 1) * 4; i < length; ++i, j -= 4)
            intraThreadBuffer[posEndTmp + i] = digits[(val >> j) & 0xF];
        posEndTmp += length;

        return this;
    }

    CommandBuffer* CommandBuffer::appendDec(uint64_t val) {
        if (this->shutdown)
            return this;
        char buffer[21];
        uint64_t length = 0;

        if (val == 0) {
            buffer[0] = '0';
            length = 1;
        } else {
            while (val > 0) {
                buffer[length] = '0' + (val % 10);
                val /= 10;
                ++length;
            }
        }

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (2)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }
        }

        if (posEndTmp + length >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (5)" << endl;
            return this;
        }

        for (uint64_t i = 0; i < length; ++i)
            intraThreadBuffer[posEndTmp + i] = buffer[length - i - 1];
        posEndTmp += length;

        return this;
    }

    CommandBuffer* CommandBuffer::appendScn(typescn scn) {
        if (test >= 2) {
            append("\"scn\":\"");
            appendHex(scn, 16);
            append('"');
        } else {
            append("\"scn\":");
            append(to_string(scn));
        }

        return this;
    }

    CommandBuffer* CommandBuffer::appendOperation(string operation) {
        append("\"operation\":\"");
        append(operation);
        append('"');

        return this;
    }

    CommandBuffer* CommandBuffer::appendTable(string owner, string table) {
        append("\"table\":\"");
        append(owner);
        append('.');
        append(table);
        append('"');

        return this;
    }

    CommandBuffer* CommandBuffer::appendNull(string columnName) {
        append('"');
        append(columnName);
        append("\":null");

        return this;
    }

    CommandBuffer* CommandBuffer::appendMs(string name, uint64_t time) {
        append('"');
        append(name);
        append("\":");
        appendDec(time);

        return this;
    }

    CommandBuffer* CommandBuffer::appendXid(typexid xid) {
        append("\"xid\":\"0x");
        appendHex(USN(xid), 4);
        append('.');
        appendHex(SLT(xid), 3);
        append('.');
        appendHex(SQN(xid), 8);
        append('"');

        return this;
    }

    CommandBuffer* CommandBuffer::appendValue(string columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t fieldPos, uint64_t fieldLength) {
        uint64_t j, jMax;
        uint8_t digits;

        append('"');
        append(columnName);
        append("\":");

        switch(typeNo) {
        case 1: //varchar(2)
        case 96: //char
            append('\"');
            appendEscape(redoLogRecord->data + fieldPos, fieldLength);
            append('\"');
            break;

        case 2: //numeric
            digits = redoLogRecord->data[fieldPos + 0];
            //just zero
            if (digits == 0x80) {
                append('0');
                break;
            }

            j = 1;
            jMax = fieldLength - 1;

            //positive number
            if (digits >= 0xC0 && jMax >= 1) {
                uint64_t val;
                //part of the total
                if (digits == 0xC0)
                    append('0');
                else {
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
            } else if (digits <= 0x3F && fieldLength >= 2) {
                uint64_t val;
                append('-');

                if (redoLogRecord->data[fieldPos + jMax] == 0x66)
                    --jMax;

                //part of the total
                if (digits == 0x3F)
                    append('0');
                else {
                    digits = 0x3F - digits;

                    val = 101 - redoLogRecord->data[fieldPos + j];
                    if (val < 10)
                        append('0' + val);
                    else
                        append('0' + (val / 10));
                        append('0' + (val % 10));
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
            } else {
                cerr << "ERROR: unknown value (type: " << typeNo << "): " << dec << fieldLength << " - ";
                for (uint64_t j = 0; j < fieldLength; ++j)
                    cout << " " << hex << setw(2) << (uint64_t) redoLogRecord->data[fieldPos + j];
                cout << endl;
            }
            break;

        case 12:
        case 180:
            if (fieldLength != 7 && fieldLength != 11) {
                cerr << "ERROR: unknown value (type: " << typeNo << "): ";
                for (uint64_t j = 0; j < fieldLength; ++j)
                    cout << " " << hex << setfill('0') << setw(2) << (uint64_t)redoLogRecord->data[fieldPos + j];
                cout << endl;
                append('null');
            } else if (timestampFormat == 0) {
                //2012-04-23T18:25:43.511Z - ISO 8601 format
                append('\"');
                uint64_t val1 = redoLogRecord->data[fieldPos + 0],
                         val2 = redoLogRecord->data[fieldPos + 1];
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
                append('0' + (redoLogRecord->data[fieldPos + 2] / 10));
                append('0' + (redoLogRecord->data[fieldPos + 2] % 10));
                append('-');
                append('0' + (redoLogRecord->data[fieldPos + 3] / 10));
                append('0' + (redoLogRecord->data[fieldPos + 3] % 10));
                append('T');
                append('0' + ((redoLogRecord->data[fieldPos + 4] - 1) / 10));
                append('0' + ((redoLogRecord->data[fieldPos + 4] - 1) % 10));
                append(':');
                append('0' + ((redoLogRecord->data[fieldPos + 5] - 1) / 10));
                append('0' + ((redoLogRecord->data[fieldPos + 5] - 1) % 10));
                append(':');
                append('0' + ((redoLogRecord->data[fieldPos + 6] - 1) / 10));
                append('0' + ((redoLogRecord->data[fieldPos + 6] - 1) % 10));

                if (fieldLength == 11) {
                    uint64_t digits = 0;
                    uint8_t buffer[10];
                    uint64_t val = oracleReader->read32Big(redoLogRecord->data + fieldPos + 7);

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
                append('\"');
            } else if (timestampFormat == 1) {
                //unix epoch format
                struct tm epochtime;
                uint64_t val1 = redoLogRecord->data[fieldPos + 0],
                         val2 = redoLogRecord->data[fieldPos + 1];

                //AD
                if (val1 >= 100 && val2 >= 100) {
                    val1 -= 100;
                    val2 -= 100;
                    uint64_t year;
                    year = val1 * 100 + val2;
                    if (year >= 1900) {
                        epochtime.tm_sec = redoLogRecord->data[fieldPos + 6] - 1;
                        epochtime.tm_min = redoLogRecord->data[fieldPos + 5] - 1;
                        epochtime.tm_hour = redoLogRecord->data[fieldPos + 4] - 1;
                        epochtime.tm_mday = redoLogRecord->data[fieldPos + 3];
                        epochtime.tm_mon = redoLogRecord->data[fieldPos + 2] - 1;
                        epochtime.tm_year = year - 1900;

                        uint64_t fraction = 0;
                        if (fieldLength == 11)
                            fraction = oracleReader->read32Big(redoLogRecord->data + fieldPos + 7);

                        appendDec(mktime(&epochtime) * 1000 + ((fraction + 500000) / 1000000));
                    } else {
                        append('null');
                    }
                } else {
                    append('null');
                }
            }
            break;

        default:
            append("\"?\"");
        }

        return this;
    }

    CommandBuffer* CommandBuffer::append(const string str) {
        if (this->shutdown)
            return this;

        uint64_t length = str.length();
        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (2)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }
        }

        if (posEndTmp + length >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (2)" << endl;
            return this;
        }

        memcpy(intraThreadBuffer + posEndTmp, str.c_str(), length);
        posEndTmp += length;

        return this;
    }

    char CommandBuffer::translationMap[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    CommandBuffer* CommandBuffer::appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot) {
        uint32_t afn =  bdba >> 22;
        bdba &= 0x003FFFFF;
        append("\"rowid\":\"");
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
        return this;
    }

    CommandBuffer* CommandBuffer::append(char chr) {
        if (this->shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + 1 >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (3)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }
        }

        if (posEndTmp + 1 >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (3)" << endl;
            return this;
        }

        intraThreadBuffer[posEndTmp++] = chr;

        return this;
    }

    CommandBuffer* CommandBuffer::appendDbzCols(OracleObject *object) {
        for (uint64_t i = 0; i < object->columns.size(); ++i) {
            bool microTimestamp = false;

            if (i > 0)
                append(',');

            append("{\"type\":\"");
            switch(object->columns[i]->typeNo) {
            case 1: //varchar(2)
            case 96: //char
                append("string");
                break;

            case 2: //numeric
                if (object->columns[i]->scale > 0)
                    append("Decimal");
                else {
                    uint64_t digits = object->columns[i]->precision - object->columns[i]->scale;
                    if (digits < 3)
                        append("int8");
                    else if (digits < 5)
                        append("int16");
                    else if (digits < 10)
                        append("int32");
                    else if (digits < 19)
                        append("int64");
                    else
                        append("Decimal");
                }
                break;

            case 12:
            case 180:
                if (timestampFormat == 0)
                    append("datetime");
                else if (timestampFormat == 1) {
                    append("int64");
                    microTimestamp = true;
                }
                break;
            }
            append("\",\"optional\":");
            if (object->columns[i]->nullable)
                append("true");
            else
                append("false");

            if (microTimestamp)
                append(",\"name\":\"io.debezium.time.MicroTimestamp\",\"version\":1");
            append(",\"field\":\"");
            append(object->columns[i]->columnName);
            append("\"}");
        }
    }

    CommandBuffer* CommandBuffer::appendDbzHead(OracleObject *object) {
        append("{\"schema\":{\"type\":\"struct\",\"fields\":[");
        append("{\"type\":\"struct\",\"fields\":[");
        appendDbzCols(object);
        append("],\"optional\":true,\"name\":\"");
        append(oracleReader->alias);
        append('.');
        append(object->owner);
        append('.');
        append(object->objectName);
        append(".Value\",\"field\":\"before\"},");
        append("{\"type\":\"struct\",\"fields\":[");
        appendDbzCols(object);
        append("],\"optional\":true,\"name\":\"");
        append(oracleReader->alias);
        append('.');
        append(object->owner);
        append('.');
        append(object->objectName);
        append(".Value\",\"field\":\"after\"},"
                "{\"type\":\"struct\",\"fields\":["
                "{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},"
                "{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},"
                "{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"schema\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"table\"},"
                "{\"type\":\"string\",\"optional\":true,\"field\":\"txId\"},"
                "{\"type\":\"int64\",\"optional\":true,\"field\":\"scn\"},"
                "{\"type\":\"string\",\"optional\":true,\"field\":\"lcr_position\"}],"
                "\"optional\":false,\"name\":\"io.debezium.connector.oracle.Source\",\"field\":\"source\"},"
                "{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},"
                "{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},"
                "{\"type\":\"struct\",\"fields\":["
                "{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},"
                "{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},"
                "{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"},"
                "{\"type\":\"string\",\"optional\":true,\"field\":\"messagetopic\"},"
                "{\"type\":\"string\",\"optional\":true,\"field\":\"messagesource\"}],\"optional\":false,\"name\":\"asgard.DEBEZIUM.CUSTOMERS.Envelope\"},\"payload\":{");
        return this;
    }

    CommandBuffer* CommandBuffer::appendDbzTail(OracleObject *object, uint64_t time, typescn scn, char op, typexid xid) {
        append(",\"source\":{\"version\":\"" PROGRAM_VERSION "\",\"connector\":\"oracle\",\"name\":\"");
        append(oracleReader->alias);
        append("\",");
        appendMs("ts_ms", time);
        append(",\"snapshot\":\"false\",\"db\":\"");
        append(oracleReader->databaseContext);
        append("\",\"schema\":\"");
        append(object->owner);
        append("\",\"table\":\"");
        append(object->objectName);
        append("\",\"txId\":\"");
        appendDec(USN(xid));
        append('.');
        appendDec(SLT(xid));
        append('.');
        appendDec(SQN(xid));
        append("\",");
        appendScn(scn);
        append(",\"lcr_position\":null},\"op\":\"");
        append(op);
        append("\",");
        appendMs("ts_ms", time);
        append(",\"transaction\":null,\"messagetopic\":\"");
        append(oracleReader->alias);
        append('.');
        append(object->owner);
        append('.');
        append(object->objectName);
        append("\",\"messagesource\":\"OpenLogReplicator from Oracle on ");
        append(oracleReader->alias);
        append("\"}}");
        return this;
    }

    CommandBuffer* CommandBuffer::beginTran() {
        if (this->shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + 8 >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (8)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }
        }

        if (posEndTmp + 8 >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (8)" << endl;
            return this;
        }

        *((uint64_t*)(intraThreadBuffer + posEndTmp)) = 0;
        posEndTmp += 8;

        return this;
    }

    CommandBuffer* CommandBuffer::commitTran() {
        if (posEndTmp == posEnd) {
            cerr << "WARNING: JSON buffer - commit of empty transaction" << endl;
            return this;
        }

        {
            unique_lock<mutex> lck(mtx);
            *((uint64_t*)(intraThreadBuffer + posEnd)) = posEndTmp - posEnd;
            posEndTmp = (posEndTmp + 7) & 0xFFFFFFFFFFFFFFF8;
            posEnd = posEndTmp;

            readersCond.notify_all();
        }

        if (posEndTmp + 1 >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (8)" << endl;
            return this;
        }

        return this;
    }

    CommandBuffer* CommandBuffer::rewind() {
        if (this->shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 || posStart == 0) {
                cerr << "WARNING, JSON buffer full, log reader suspended (5)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }

            posSize = posEnd;
            posEnd = 0;
            posEndTmp = 0;
        }

        return this;
    }

    uint64_t CommandBuffer::currentTranSize() {
        return posEndTmp - posEnd;
    }

    CommandBuffer::~CommandBuffer() {
        if (intraThreadBuffer != nullptr) {
            delete[] intraThreadBuffer;
            intraThreadBuffer = nullptr;
        }
    }
}
