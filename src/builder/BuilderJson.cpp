/* Memory buffer for handling output buffer in JSON format
   Copyright (C) 2018-2024 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "../common/DbTable.h"
#include "../common/typeRowId.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "BuilderJson.h"

namespace OpenLogReplicator {
    BuilderJson::BuilderJson(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata, Format& newFormat, uint64_t newFlushBuffer) :
            Builder(newCtx, newLocales, newMetadata, newFormat, newFlushBuffer),
            hasPreviousValue(false),
            hasPreviousRedo(false),
            hasPreviousColumn(false) {
    }

    void BuilderJson::columnFloat(const std::string& columnName, double value) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);

        std::ostringstream ss;
        ss << value;
        append(ss.str());
    }

    void BuilderJson::columnDouble(const std::string& columnName, long double value) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);

        std::ostringstream ss;
        ss << value;
        append(ss.str());
    }

    void BuilderJson::columnString(const std::string& columnName) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":")", sizeof(R"(":")") - 1);
        appendEscape(valueBuffer, valueSize);
        append('"');
    }

    void BuilderJson::columnNumber(const std::string& columnName, int precision __attribute__((unused)), int scale __attribute__((unused))) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);
        append(valueBuffer, valueSize);
    }

    void BuilderJson::columnRowId(const std::string& columnName, typeRowId rowId) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":")", sizeof(R"(":")") - 1);
        char str[19];
        rowId.toHex(str);
        append(str, 18);
        append('"');
    }

    void BuilderJson::columnRaw(const std::string& columnName, const uint8_t* data, uint64_t size) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":")", sizeof(R"(":")") - 1);
        for (uint64_t j = 0; j < size; ++j)
            appendHex2(*(data + j));
        append('"');
    }

    void BuilderJson::columnTimestamp(const std::string& columnName, time_t timestamp, uint64_t fraction) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);
        char buffer[22];

        switch (format.timestampFormat) {
            case Format::TIMESTAMP_FORMAT::UNIX_NANO:
                // 1712345678123456789
                if (timestamp < 1000000000 && timestamp > -1000000000)
                    appendSDec(timestamp * 1000000000L + fraction);
                else {
                    // Big number
                    int64_t firstDigits = timestamp / 1000000000;
                    if (timestamp < 0) {
                        timestamp = -timestamp;
                        fraction = -fraction;
                    }
                    timestamp %= 1000000000;
                    appendSDec(firstDigits);
                    appendDecN<18>(timestamp * 1000000000L + fraction);
                }
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_MICRO:
                // 1712345678123457
                appendSDec(timestamp * 1000000L + ((fraction + 500) / 1000));
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_MILLI:
                // 1712345678123
                appendSDec(timestamp * 1000L + ((fraction + 500000) / 1000000));
                break;

            case Format::TIMESTAMP_FORMAT::UNIX:
                // 1712345678
                appendSDec(timestamp + ((fraction + 500000000) / 1000000000));
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_NANO_STRING:
                // "1712345678123456789"
                append('"');
                if (timestamp < 1000000000 && timestamp > -1000000000)
                    appendSDec(timestamp * 1000000000L + fraction);
                else {
                    // Big number
                    int64_t firstDigits = timestamp / 1000000000;
                    if (timestamp < 0) {
                        timestamp = -timestamp;
                        fraction = -fraction;
                    }
                    timestamp %= 1000000000;
                    appendSDec(firstDigits);
                    appendDecN<18>(timestamp * 1000000000L + fraction);
                }
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_MICRO_STRING:
                // "1712345678123457"
                append('"');
                appendSDec(timestamp * 1000000L + ((fraction + 500) / 1000));
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_MILLI_STRING:
                // "1712345678123"
                append('"');
                appendSDec(timestamp * 1000L + ((fraction + 500000) / 1000000));
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::UNIX_STRING:
                // "1712345678"
                append('"');
                appendSDec(timestamp + ((fraction + 500000000) / 1000000000));
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_NANO_TZ:
                // "2024-04-05T19:34:38.123456789Z"
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<9>(fraction);
                append(R"(Z")", sizeof(R"(Z")") - 1);
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_MICRO_TZ:
                // "2024-04-05T19:34:38.123456Z"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<6>(fraction);
                append(R"(Z")", sizeof(R"(Z")") - 1);
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_MILLI_TZ:
                // "2024-04-05T19:34:38.123Z"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<3>(fraction);
                append(R"(Z")", sizeof(R"(Z")") - 1);
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_TZ:
                // "2024-04-05T19:34:38Z"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append(R"(Z")", sizeof(R"(Z")") - 1);
                break;
            case Format::TIMESTAMP_FORMAT::ISO8601_NANO:
                // "2024-04-05 19:34:38.123456789"
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<9>(fraction);
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_MICRO:
                // "2024-04-05 19:34:38.123456"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<6>(fraction);
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601_MILLI:
                // "2024-04-05 19:34:38.123"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<3>(fraction);
                append('"');
                break;

            case Format::TIMESTAMP_FORMAT::ISO8601:
                // "2024-04-05 19:34:38"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('"');
                break;
        }
    }

    void BuilderJson::columnTimestampTz(const std::string& columnName, time_t timestamp, uint64_t fraction, const char* tz) {
        if (hasPreviousColumn)
            append(',');
        else
            hasPreviousColumn = true;

        append('"');
        appendEscape(columnName);
        append(R"(":)", sizeof(R"(":)") - 1);
        char buffer[22];

        switch (format.timestampTzFormat) {
            case Format::TIMESTAMP_TZ_FORMAT::UNIX_NANO_STRING:
                // "1700000000.123456789,Europe/Warsaw"
                append('"');
                if (timestamp < 1000000000 && timestamp > -1000000000)
                    appendSDec(timestamp * 1000000000L + fraction);
                else {
                    // Big number
                    int64_t firstDigits = timestamp / 1000000000;
                    if (timestamp < 0) {
                        timestamp = -timestamp;
                        fraction = -fraction;
                    }
                    timestamp %= 1000000000;
                    appendSDec(firstDigits);
                    appendDecN<18>(timestamp * 1000000000L + fraction);
                }
                append(',');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::UNIX_MICRO_STRING:
                // "1700000000.123456,Europe/Warsaw"
                append('"');
                appendSDec(timestamp * 1000000L + ((fraction + 500) / 1000));
                append(',');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::UNIX_MILLI_STRING:
                // "1700000000.123,Europe/Warsaw"
                append('"');
                appendSDec(timestamp * 1000L + ((fraction + 500000) / 1000000));
                append(',');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::UNIX_STRING:
                // "1700000000,Europe/Warsaw"
                append('"');
                appendSDec(timestamp + ((fraction + 500000000) / 1000000000));
                append(',');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_NANO_TZ:
                // "2024-04-05T19:34:38.123456789Z Europe/Warsaw"
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<9>(fraction);
                append("Z ", sizeof("Z ") - 1);
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_MICRO_TZ:
                // "2024-04-05T19:34:38.123456Z Europe/Warsaw"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<6>(fraction);
                append("Z ", sizeof("Z ") - 1);
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_MILLI_TZ:
                // "2024-04-05T19:34:38.123Z Europe/Warsaw"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append('.');
                appendDecN<3>(fraction);
                append("Z ", sizeof("Z ") - 1);
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_TZ:
                // "2024-04-05T19:34:38Z Europe/Warsaw"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, true, false));
                append("Z ", sizeof("Z ") - 1);
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_NANO:
                // "2024-04-05 19:34:38.123456789,Europe/Warsaw"
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<9>(fraction);
                append(' ');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_MICRO:
                // "2024-04-05 19:34:38.123456,Europe/Warsaw"
                fraction += 500;
                fraction /= 1000;
                if (fraction >= 1000000) {
                    fraction -= 1000000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<6>(fraction);
                append(' ');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601_MILLI:
                // "2024-04-05 19:34:38.123 Europe/Warsaw"
                fraction += 500000;
                fraction /= 1000000;
                if (fraction >= 1000) {
                    fraction -= 1000;
                    ++timestamp;
                }
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append('.');
                appendDecN<3>(fraction);
                append(' ');
                append(tz);
                append('"');
                break;

            case Format::TIMESTAMP_TZ_FORMAT::ISO8601:
                // "2024-04-05 19:34:38 Europe/Warsaw"
                if (fraction >= 500000000)
                    ++timestamp;
                append('"');
                append(buffer, ctx->epochToIso8601(timestamp, buffer, false, false));
                append(' ');
                append(tz);
                append('"');
                break;
        }
    }

    void BuilderJson::processBeginMessage(typeScn scn, typeSeq sequence, time_t timestamp) {
        newTran = false;
        hasPreviousRedo = false;

        if (format.isMessageFormatSkipBegin())
            return;

        builderBegin(scn, sequence, 0, BuilderMsg::OUTPUT_BUFFER::NONE);
        append('{');
        hasPreviousValue = false;
        appendHeader(scn, timestamp, true, format.isDbFormatAddDml(), true);

        if (hasPreviousValue)
            append(',');
        else
            hasPreviousValue = true;

        if (format.isAttributesFormatBegin())
            appendAttributes();

        if (format.isMessageFormatFull()) {
            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        } else {
            append(R"("payload":[{"op":"begin"}]})", sizeof(R"("payload":[{"op":"begin"}]})") - 1);
            builderCommit();
        }
    }

    void BuilderJson::processCommit(typeScn scn, typeSeq sequence, time_t timestamp) {
        // Skip empty transaction
        if (newTran) {
            newTran = false;
            return;
        }

        if (format.isMessageFormatFull()) {
            append("]}", sizeof("]}") - 1);
            builderCommit();
        } else if (!format.isMessageFormatSkipCommit()) {
            builderBegin(scn, sequence, 0, BuilderMsg::OUTPUT_BUFFER::NONE);
            append('{');

            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, format.isDbFormatAddDml(), true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if (format.isAttributesFormatCommit())
                appendAttributes();

            append(R"("payload":[{"op":"commit"}]})", sizeof(R"("payload":[{"op":"commit"}]})") - 1);
            builderCommit();
        }
        num = 0;
    }

    void BuilderJson::processInsert(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table,
                                    typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid  __attribute__((unused)),
                                    uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if (format.isMessageFormatFull()) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            addTagData(lobCtx, xmlCtx, table, Format::VALUE_TYPE::AFTER, offset);

            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, format.isDbFormatAddDml(), true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if (format.isAttributesFormatDml())
                appendAttributes();

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"c",)", sizeof(R"({"op":"c",)") - 1);
        if (format.isMessageFormatAddOffset()) {
            append(R"("offset":)", sizeof(R"("offset":)") - 1);
            appendDec(offset);
            append(',');
        }
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendAfter(lobCtx, xmlCtx, table, offset);
        append('}');

        if (!format.isMessageFormatFull()) {
            append("]}", sizeof("]}") - 1);
            builderCommit();
        }
        ++num;
    }

    void BuilderJson::processUpdate(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table,
                                    typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid  __attribute__((unused)),
                                    uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if (format.isMessageFormatFull()) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            addTagData(lobCtx, xmlCtx, table, Format::VALUE_TYPE::AFTER, offset);

            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, format.isDbFormatAddDml(), true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if (format.isAttributesFormatDml())
                appendAttributes();

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"u",)", sizeof(R"({"op":"u",)") - 1);
        if (format.isMessageFormatAddOffset()) {
            append(R"("offset":)", sizeof(R"("offset":)") - 1);
            appendDec(offset);
            append(',');
        }
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, xmlCtx, table, offset);
        appendAfter(lobCtx, xmlCtx, table, offset);
        append('}');

        if (!format.isMessageFormatFull()) {
            append("]}", sizeof("]}") - 1);
            builderCommit();
        }
        ++num;
    }

    void BuilderJson::processDelete(typeScn scn, typeSeq sequence, time_t timestamp, LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table,
                                    typeObj obj, typeDataObj dataObj, typeDba bdba, typeSlot slot, typeXid xid __attribute__((unused)),
                                    uint64_t offset) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if (format.isMessageFormatFull()) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            addTagData(lobCtx, xmlCtx, table, Format::VALUE_TYPE::BEFORE, offset);

            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, format.isDbFormatAddDml(), true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if (format.isAttributesFormatDml())
                appendAttributes();

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"d",)", sizeof(R"({"op":"d",)") - 1);
        if (format.isMessageFormatAddOffset()) {
            append(R"("offset":)", sizeof(R"("offset":)") - 1);
            appendDec(offset);
            append(',');
        }
        appendSchema(table, obj);
        appendRowid(dataObj, bdba, slot);
        appendBefore(lobCtx, xmlCtx, table, offset);
        append('}');

        if (!format.isMessageFormatFull()) {
            append("]}", sizeof("]}") - 1);
            builderCommit();
        }
        ++num;
    }

    void BuilderJson::processDdl(typeScn scn, typeSeq sequence, time_t timestamp, const DbTable* table, typeObj obj,
                                 typeDataObj dataObj __attribute__((unused)), uint16_t ddlType __attribute__((unused)), uint16_t seq __attribute__((unused)),
                                 const char* sql, uint64_t sqlSize) {
        if (newTran)
            processBeginMessage(scn, sequence, timestamp);

        if (format.isMessageFormatFull()) {
            if (hasPreviousRedo)
                append(',');
            else
                hasPreviousRedo = true;
        } else {
            builderBegin(scn, sequence, obj, BuilderMsg::OUTPUT_BUFFER::NONE);
            append('{');
            hasPreviousValue = false;
            appendHeader(scn, timestamp, false, format.isDbFormatAddDdl(), true);

            if (hasPreviousValue)
                append(',');
            else
                hasPreviousValue = true;

            if (format.isAttributesFormatDml())
                appendAttributes();

            append(R"("payload":[)", sizeof(R"("payload":[)") - 1);
        }

        append(R"({"op":"ddl",)", sizeof(R"({"op":"ddl",)") - 1);
        appendSchema(table, obj);
        append(R"(,"sql":")", sizeof(R"(,"sql":")") - 1);
        appendEscape(sql, sqlSize);
        append(R"("})", sizeof(R"("})") - 1);

        if (!format.isMessageFormatFull()) {
            append("]}", sizeof("]}") - 1);
            builderCommit();
        }
        ++num;
    }

    void BuilderJson::processCheckpoint(typeScn scn, typeSeq sequence, time_t timestamp, uint64_t offset, bool redo) {
        if (lwnScn != scn) {
            lwnScn = scn;
            lwnIdx = 0;
        }

        builderBegin(scn, sequence, 0, BuilderMsg::OUTPUT_BUFFER::CHECKPOINT);
        append('{');
        hasPreviousValue = false;
        appendHeader(scn, timestamp, true, false, false);

        if (hasPreviousValue)
            append(',');
        else
            hasPreviousValue = true;

        append(R"("payload":[{"op":"chkpt","seq":)", sizeof(R"("payload":[{"op":"chkpt","seq":)") - 1);
        appendDec(sequence);
        append(R"(,"offset":)", sizeof(R"(,"offset":)") - 1);
        appendDec(offset);
        if (redo)
            append(R"(,"redo":true)", sizeof(R"(,"redo":true)") - 1);
        append("}]}", sizeof("}]}") - 1);
        builderCommit();
    }

    void BuilderJson::addTagData(LobCtx* lobCtx, const XmlCtx* xmlCtx, const DbTable* table, Format::VALUE_TYPE valueType, uint64_t offset) {
        if (table == nullptr || table->tagCols.size() == 0)
            return;

        uint64_t messagePositionOld = messagePosition;
        hasPreviousColumn = false;
        for (uint x = 0; x < table->tagCols.size(); ++x) {
            typeCol column = table->tagCols[x] - 1;
            if (values[column][static_cast<uint>(valueType)] == nullptr)
                continue;

            if (sizes[column][static_cast<uint>(valueType)] > 0)
                processValue(lobCtx, xmlCtx, table, column, values[column][static_cast<uint>(valueType)], sizes[column][static_cast<uint>(valueType)],
                             offset, true, compressedAfter);
            else
                columnNull(table, column, true);
        }

        if (messagePosition >= messagePositionOld)
            msg->tagSize = messagePosition - messagePositionOld;
        else
            msg->tagSize = messageSize + messagePosition;
    }
}
