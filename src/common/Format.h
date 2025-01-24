/* Header for Format class
   Copyright (C) 2018-2025 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef FORMAT_H_
#define FORMAT_H_

#include "../common/types/Types.h"

namespace OpenLogReplicator {

    class Format final {
    public:
        enum class ATTRIBUTES_FORMAT : unsigned char {
            DEFAULT = 0, BEGIN = 1UL << 0, DML = 1UL << 1, COMMIT = 1UL << 2
        };

        enum class DB_FORMAT : unsigned char {
            DEFAULT = 0, ADD_DML = 1, ADD_DDL = 2
        };

        enum class CHAR_FORMAT : unsigned char {
            UTF8 = 0, NOMAPPING = 1, HEX = 2
        };

        enum class COLUMN_FORMAT : unsigned char {
            CHANGED = 0,        // Default, only changed columns for update, or PK
            FULL_INS_DEC = 1,   // Show full nulls from insert and delete
            FULL_UPD = 2        // Show all from redo;
        };

        enum class INTERVAL_DTS_FORMAT : unsigned char {
            UNIX_NANO, UNIX_MICRO, UNIX_MILLI, UNIX, UNIX_NANO_STRING, UNIX_MICRO_STRING, UNIX_MILLI_STRING, UNIX_STRING, ISO8601_SPACE, ISO8601_COMMA,
            ISO8601_DASH
        };

        enum class INTERVAL_YTM_FORMAT : unsigned char {
            MONTHS, MONTHS_STRING, STRING_YM_SPACE, STRING_YM_COMMA, STRING_YM_DASH
        };

        enum class MESSAGE_FORMAT : unsigned char {
            DEFAULT = 0, FULL = 1 << 0, ADD_SEQUENCES = 1 << 1,
            // JSON only:
            SKIP_BEGIN = 1 << 2, SKIP_COMMIT = 1 << 3, ADD_OFFSET = 1 << 4
        };

        enum class RID_FORMAT : unsigned char {
            SKIP, TEXT
        };

        enum class SCN_FORMAT : unsigned char {
            NUMERIC, TEXT_HEX
        };

        enum class SCN_TYPE : unsigned char {
            NONE = 0, ALL_PAYLOADS = 1 << 0, COMMIT_VALUE = 1 << 1
        };

        enum class SCHEMA_FORMAT : unsigned char {
            DEFAULT = 0, FULL = 1 << 0, REPEATED = 1 << 1, OBJ = 1 << 2
        };

        enum class TIMESTAMP_ALL : unsigned char {
            JUST_BEGIN, ALL_PAYLOADS
        };

        enum class TIMESTAMP_FORMAT : unsigned char {
            UNIX_NANO, UNIX_MICRO, UNIX_MILLI, UNIX, UNIX_NANO_STRING, UNIX_MICRO_STRING, UNIX_MILLI_STRING, UNIX_STRING, ISO8601_NANO_TZ, ISO8601_MICRO_TZ,
            ISO8601_MILLI_TZ, ISO8601_TZ, ISO8601_NANO, ISO8601_MICRO, ISO8601_MILLI, ISO8601
        };

        enum class TIMESTAMP_TZ_FORMAT : unsigned char {
            UNIX_NANO_STRING, UNIX_MICRO_STRING, UNIX_MILLI_STRING, UNIX_STRING, ISO8601_NANO_TZ, ISO8601_MICRO_TZ, ISO8601_MILLI_TZ, ISO8601_TZ, ISO8601_NANO,
            ISO8601_MICRO, ISO8601_MILLI, ISO8601
        };

        enum class TRANSACTION_TYPE : unsigned char {
            T_NONE, INSERT, DELETE, UPDATE
        };

        enum class UNKNOWN_FORMAT : unsigned char {
            QUESTION_MARK, DUMP
        };

        enum class UNKNOWN_TYPE : unsigned char {
            HIDE, SHOW
        };

        enum class VALUE_TYPE : unsigned char {
            BEFORE, AFTER, BEFORE_SUPP, AFTER_SUPP, LENGTH
        };

        enum class XID_FORMAT : unsigned char {
            TEXT_HEX, TEXT_DEC, NUMERIC
        };

        DB_FORMAT dbFormat;
        ATTRIBUTES_FORMAT attributesFormat;
        INTERVAL_DTS_FORMAT intervalDtsFormat;
        INTERVAL_YTM_FORMAT intervalYtmFormat;
        MESSAGE_FORMAT messageFormat;
        RID_FORMAT ridFormat;
        XID_FORMAT xidFormat;
        TIMESTAMP_FORMAT timestampFormat;
        TIMESTAMP_TZ_FORMAT timestampTzFormat;
        TIMESTAMP_ALL timestampAll;
        CHAR_FORMAT charFormat;
        SCN_FORMAT scnFormat;
        SCN_TYPE scnType;
        UNKNOWN_FORMAT unknownFormat;
        SCHEMA_FORMAT schemaFormat;
        COLUMN_FORMAT columnFormat;
        UNKNOWN_TYPE unknownType;

        Format(DB_FORMAT newDbFormat, ATTRIBUTES_FORMAT newAttributesFormat, INTERVAL_DTS_FORMAT newIntervalDtsFormat,
               INTERVAL_YTM_FORMAT newIntervalYtmFormat, MESSAGE_FORMAT newMessageFormat, RID_FORMAT newRidFormat, XID_FORMAT newXidFormat,
               TIMESTAMP_FORMAT newTimestampFormat, TIMESTAMP_TZ_FORMAT newTimestampTzFormat, TIMESTAMP_ALL newTimestampAll, CHAR_FORMAT newCharFormat,
               SCN_FORMAT newScnFormat, SCN_TYPE newScnType, UNKNOWN_FORMAT newUnknownFormat, SCHEMA_FORMAT newSchemaFormat, COLUMN_FORMAT newColumnFormat,
               UNKNOWN_TYPE newUnknownType) :
                dbFormat(newDbFormat),
                attributesFormat(newAttributesFormat),
                intervalDtsFormat(newIntervalDtsFormat),
                intervalYtmFormat(newIntervalYtmFormat),
                messageFormat(newMessageFormat),
                ridFormat(newRidFormat),
                xidFormat(newXidFormat),
                timestampFormat(newTimestampFormat),
                timestampTzFormat(newTimestampTzFormat),
                timestampAll(newTimestampAll),
                charFormat(newCharFormat),
                scnFormat(newScnFormat),
                scnType(newScnType),
                unknownFormat(newUnknownFormat),
                schemaFormat(newSchemaFormat),
                columnFormat(newColumnFormat),
                unknownType(newUnknownType) {
        }

        [[nodiscard]] bool isAttributesFormatBegin() const {
            return (static_cast<uint>(attributesFormat) & static_cast<uint>(ATTRIBUTES_FORMAT::BEGIN)) != 0;
        };

        [[nodiscard]] bool isAttributesFormatDml() const {
            return (static_cast<uint>(attributesFormat) & static_cast<uint>(ATTRIBUTES_FORMAT::DML)) != 0;
        };

        [[nodiscard]] bool isAttributesFormatCommit() const {
            return (static_cast<uint>(attributesFormat) & static_cast<uint>(ATTRIBUTES_FORMAT::COMMIT)) != 0;
        };

        [[nodiscard]] bool isCharFormatNoMapping() const {
            return (static_cast<uint>(charFormat) & static_cast<uint>(CHAR_FORMAT::NOMAPPING)) != 0;
        };

        [[nodiscard]] bool isCharFormatHex() const {
            return (static_cast<uint>(charFormat) & static_cast<uint>(CHAR_FORMAT::HEX)) != 0;
        };

        [[nodiscard]] bool isScnTypeAllPayloads() const {
            return (static_cast<uint>(scnType) & static_cast<uint>(SCN_TYPE::ALL_PAYLOADS)) != 0;
        };

        [[nodiscard]] bool isScnTypeCommitValue() const {
            return (static_cast<uint>(scnType) & static_cast<uint>(SCN_TYPE::COMMIT_VALUE)) != 0;
        };

        [[nodiscard]] bool isSchemaFormatFull() const {
            return (static_cast<uint>(schemaFormat) & static_cast<uint>(SCHEMA_FORMAT::FULL)) != 0;
        };

        [[nodiscard]] bool isSchemaFormatRepeated() const {
            return (static_cast<uint>(schemaFormat) & static_cast<uint>(SCHEMA_FORMAT::REPEATED)) != 0;
        };

        [[nodiscard]] bool isSchemaFormatObj() const {
            return (static_cast<uint>(schemaFormat) & static_cast<uint>(SCHEMA_FORMAT::OBJ)) != 0;
        };

        [[nodiscard]] bool isMessageFormatFull() const {
            return (static_cast<uint>(messageFormat) & static_cast<uint>(MESSAGE_FORMAT::FULL)) != 0;
        }

        [[nodiscard]] bool isMessageFormatAddSequences() const {
            return (static_cast<uint>(messageFormat) & static_cast<uint>(MESSAGE_FORMAT::ADD_SEQUENCES)) != 0;
        }

        [[nodiscard]] bool isMessageFormatSkipBegin() const {
            return (static_cast<uint>(messageFormat) & static_cast<uint>(MESSAGE_FORMAT::SKIP_BEGIN)) != 0;
        }

        [[nodiscard]] bool isMessageFormatSkipCommit() const {
            return (static_cast<uint>(messageFormat) & static_cast<uint>(MESSAGE_FORMAT::SKIP_COMMIT)) != 0;
        }

        [[nodiscard]] bool isMessageFormatAddOffset() const {
            return (static_cast<uint>(messageFormat) & static_cast<uint>(MESSAGE_FORMAT::ADD_OFFSET)) != 0;
        }

        [[nodiscard]] bool isDbFormatAddDml() const {
            return (static_cast<uint>(dbFormat) & static_cast<uint>(DB_FORMAT::ADD_DML)) != 0;
        }

        [[nodiscard]] bool isDbFormatAddDdl() const {
            return (static_cast<uint>(dbFormat) & static_cast<uint>(DB_FORMAT::ADD_DDL)) != 0;
        }
    };
}
#endif
