/* Header for Format class
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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
            DEFAULT  = 0,
            BEGIN    = 1UL << 0,
            DML      = 1UL << 1,
            COMMIT   = 1UL << 2
        };

        enum class DB_FORMAT : unsigned char {
            DEFAULT = 0,
            ADD_DML = 1,
            ADD_DDL = 2,
            ALL     = 3
        };

        enum class CHAR_FORMAT : unsigned char {
            UTF8      = 0,
            NOMAPPING = 1,
            HEX       = 2
        };

        enum class COLUMN_FORMAT : unsigned char {
            CHANGED      = 0, // Default, only changed columns for update, or PK
            FULL_INS_DEC = 1, // Show full nulls from insert and delete
            FULL_UPD     = 2  // Show all from redo;
        };

        enum class INTERVAL_DTS_FORMAT : unsigned char {
            UNIX_NANO,
            UNIX_MICRO,
            UNIX_MILLI,
            UNIX,
            UNIX_NANO_STRING,
            UNIX_MICRO_STRING,
            UNIX_MILLI_STRING,
            UNIX_STRING,
            ISO8601_SPACE,
            ISO8601_COMMA,
            ISO8601_DASH
        };

        enum class INTERVAL_YTM_FORMAT : unsigned char {
            MONTHS,
            MONTHS_STRING,
            STRING_YM_SPACE,
            STRING_YM_COMMA,
            STRING_YM_DASH
        };

        enum class MESSAGE_FORMAT : unsigned char {
            DEFAULT       = 0,
            FULL          = 1 << 0,
            ADD_SEQUENCES = 1 << 1,
            // JSON only:
            SKIP_BEGIN  = 1 << 2,
            SKIP_COMMIT = 1 << 3,
            ADD_OFFSET  = 1 << 4
        };

        enum class RID_FORMAT : unsigned char {
            SKIP,
            TEXT
        };

        enum class REDO_THREAD_FORMAT : unsigned char {
            SKIP,
            TEXT
        };

        enum class SCN_FORMAT : unsigned char {
            NUMERIC,
            TEXT_HEX
        };

        enum class SCN_TYPE : unsigned char {
            DEFAULT      = 0,
            COMMIT_VALUE = 1 << 0,
            BEGIN        = 1 << 1,
            DML          = 1 << 2,
            COMMIT       = 1 << 3,
            DEBEZIUM     = BEGIN | DML | COMMIT
        };

        enum class SCHEMA_FORMAT : unsigned char {
            DEFAULT  = 0,
            FULL     = 1 << 0,
            REPEATED = 1 << 1,
            OBJ      = 1 << 2,
            ALL      = 7
        };

        enum class TIMESTAMP_TYPE : unsigned char {
            DEFAULT      = 0,
            COMMIT_VALUE = 1 << 0,
            BEGIN        = 1 << 1,
            DML          = 1 << 2,
            COMMIT       = 1 << 3,
            DEBEZIUM     = BEGIN | DML | COMMIT
        };

        enum class USER_TYPE : unsigned char {
            DEFAULT      = 0,
            BEGIN        = 1 << 0,
            DML          = 1 << 1,
            COMMIT       = 1 << 2,
            DDL          = 1 << 3,
            DEBEZIUM     = BEGIN | DML | COMMIT | DDL
        };

        enum class TIMESTAMP_FORMAT : unsigned char {
            UNIX_NANO,
            UNIX_MICRO,
            UNIX_MILLI,
            UNIX,
            UNIX_NANO_STRING,
            UNIX_MICRO_STRING,
            UNIX_MILLI_STRING,
            UNIX_STRING,
            ISO8601_NANO_TZ,
            ISO8601_MICRO_TZ,
            ISO8601_MILLI_TZ,
            ISO8601_TZ,
            ISO8601_NANO,
            ISO8601_MICRO,
            ISO8601_MILLI,
            ISO8601
        };

        enum class TIMESTAMP_TZ_FORMAT : unsigned char {
            UNIX_NANO_STRING,
            UNIX_MICRO_STRING,
            UNIX_MILLI_STRING,
            UNIX_STRING,
            ISO8601_NANO_TZ,
            ISO8601_MICRO_TZ,
            ISO8601_MILLI_TZ,
            ISO8601_TZ,
            ISO8601_NANO,
            ISO8601_MICRO,
            ISO8601_MILLI,
            ISO8601
        };

        enum class TRANSACTION_TYPE : unsigned char {
            T_NONE,
            INSERT,
            DELETE,
            UPDATE
        };

        enum class UNKNOWN_FORMAT : unsigned char {
            QUESTION_MARK,
            DUMP
        };

        enum class UNKNOWN_TYPE : unsigned char {
            HIDE,
            SHOW
        };

        enum class VALUE_TYPE : unsigned char {
            BEFORE,
            AFTER,
            BEFORE_SUPP,
            AFTER_SUPP,
            LENGTH
        };

        enum class XID_FORMAT : unsigned char {
            TEXT_HEX,
            TEXT_DEC,
            NUMERIC,
            TEXT_REVERSED
        };

        DB_FORMAT dbFormat;
        ATTRIBUTES_FORMAT attributesFormat;
        INTERVAL_DTS_FORMAT intervalDtsFormat;
        INTERVAL_YTM_FORMAT intervalYtmFormat;
        MESSAGE_FORMAT messageFormat;
        RID_FORMAT ridFormat;
        REDO_THREAD_FORMAT redoThreadFormat;
        XID_FORMAT xidFormat;
        TIMESTAMP_FORMAT timestampFormat;
        TIMESTAMP_FORMAT timestampMetadataFormat;
        TIMESTAMP_TZ_FORMAT timestampTzFormat;
        TIMESTAMP_TYPE timestampType;
        CHAR_FORMAT charFormat;
        SCN_FORMAT scnFormat;
        SCN_TYPE scnType;
        UNKNOWN_FORMAT unknownFormat;
        SCHEMA_FORMAT schemaFormat;
        COLUMN_FORMAT columnFormat;
        UNKNOWN_TYPE unknownType;
        USER_TYPE userType;

        Format(DB_FORMAT newDbFormat, ATTRIBUTES_FORMAT newAttributesFormat, INTERVAL_DTS_FORMAT newIntervalDtsFormat,
               INTERVAL_YTM_FORMAT newIntervalYtmFormat, MESSAGE_FORMAT newMessageFormat, RID_FORMAT newRidFormat, REDO_THREAD_FORMAT newRedoThreadFormat,
               XID_FORMAT newXidFormat, TIMESTAMP_FORMAT newTimestampFormat, TIMESTAMP_FORMAT newTimestampMetadataFormat,
               TIMESTAMP_TZ_FORMAT newTimestampTzFormat, TIMESTAMP_TYPE newTimestampType, CHAR_FORMAT newCharFormat, SCN_FORMAT newScnFormat,
               SCN_TYPE newScnType, UNKNOWN_FORMAT newUnknownFormat, SCHEMA_FORMAT newSchemaFormat, COLUMN_FORMAT newColumnFormat, UNKNOWN_TYPE newUnknownType,
               USER_TYPE newUserType):
                dbFormat(newDbFormat),
                attributesFormat(newAttributesFormat),
                intervalDtsFormat(newIntervalDtsFormat),
                intervalYtmFormat(newIntervalYtmFormat),
                messageFormat(newMessageFormat),
                ridFormat(newRidFormat),
                redoThreadFormat(newRedoThreadFormat),
                xidFormat(newXidFormat),
                timestampFormat(newTimestampFormat),
                timestampMetadataFormat(newTimestampMetadataFormat),
                timestampTzFormat(newTimestampTzFormat),
                timestampType(newTimestampType),
                charFormat(newCharFormat),
                scnFormat(newScnFormat),
                scnType(newScnType),
                unknownFormat(newUnknownFormat),
                schemaFormat(newSchemaFormat),
                columnFormat(newColumnFormat),
                unknownType(newUnknownType),
                userType(newUserType) {}

        [[nodiscard]] bool isAttributesFormatBegin() const {
            return (static_cast<unsigned char>(attributesFormat) & static_cast<unsigned char>(ATTRIBUTES_FORMAT::BEGIN)) != 0;
        }

        [[nodiscard]] bool isAttributesFormatDml() const {
            return (static_cast<unsigned char>(attributesFormat) & static_cast<unsigned char>(ATTRIBUTES_FORMAT::DML)) != 0;
        }

        [[nodiscard]] bool isAttributesFormatCommit() const {
            return (static_cast<unsigned char>(attributesFormat) & static_cast<unsigned char>(ATTRIBUTES_FORMAT::COMMIT)) != 0;
        }

        [[nodiscard]] bool isCharFormatNoMapping() const {
            return (static_cast<unsigned char>(charFormat) & static_cast<unsigned char>(CHAR_FORMAT::NOMAPPING)) != 0;
        }

        [[nodiscard]] bool isCharFormatHex() const {
            return (static_cast<unsigned char>(charFormat) & static_cast<unsigned char>(CHAR_FORMAT::HEX)) != 0;
        }

        [[nodiscard]] bool isScnTypeCommitValue() const {
            return (static_cast<unsigned char>(scnType) & static_cast<unsigned char>(SCN_TYPE::COMMIT_VALUE)) != 0;
        }

        [[nodiscard]] bool isScnTypeBegin() const {
            return (static_cast<unsigned char>(scnType) & static_cast<unsigned char>(SCN_TYPE::BEGIN)) != 0;
        }

        [[nodiscard]] bool isScnTypeDml() const {
            return (static_cast<unsigned char>(scnType) & static_cast<unsigned char>(SCN_TYPE::DML)) != 0;
        }

        [[nodiscard]] bool isScnTypeCommit() const {
            return (static_cast<unsigned char>(scnType) & static_cast<unsigned char>(SCN_TYPE::COMMIT)) != 0;
        }

        [[nodiscard]] bool isSchemaFormatFull() const {
            return (static_cast<unsigned char>(schemaFormat) & static_cast<unsigned char>(SCHEMA_FORMAT::FULL)) != 0;
        }

        [[nodiscard]] bool isSchemaFormatRepeated() const {
            return (static_cast<unsigned char>(schemaFormat) & static_cast<unsigned char>(SCHEMA_FORMAT::REPEATED)) != 0;
        }

        [[nodiscard]] bool isSchemaFormatObj() const {
            return (static_cast<unsigned char>(schemaFormat) & static_cast<unsigned char>(SCHEMA_FORMAT::OBJ)) != 0;
        }

        [[nodiscard]] bool isMessageFormatFull() const {
            return (static_cast<unsigned char>(messageFormat) & static_cast<unsigned char>(MESSAGE_FORMAT::FULL)) != 0;
        }

        [[nodiscard]] bool isMessageFormatAddSequences() const {
            return (static_cast<unsigned char>(messageFormat) & static_cast<unsigned char>(MESSAGE_FORMAT::ADD_SEQUENCES)) != 0;
        }

        [[nodiscard]] bool isMessageFormatSkipBegin() const {
            return (static_cast<unsigned char>(messageFormat) & static_cast<unsigned char>(MESSAGE_FORMAT::SKIP_BEGIN)) != 0;
        }

        [[nodiscard]] bool isMessageFormatSkipCommit() const {
            return (static_cast<unsigned char>(messageFormat) & static_cast<unsigned char>(MESSAGE_FORMAT::SKIP_COMMIT)) != 0;
        }

        [[nodiscard]] bool isMessageFormatAddOffset() const {
            return (static_cast<unsigned char>(messageFormat) & static_cast<unsigned char>(MESSAGE_FORMAT::ADD_OFFSET)) != 0;
        }

        [[nodiscard]] bool isTimestampTypeCommitValue() const {
            return (static_cast<unsigned char>(timestampType) & static_cast<unsigned char>(TIMESTAMP_TYPE::COMMIT_VALUE)) != 0;
        }

        [[nodiscard]] bool isTimestampTypeBegin() const {
            return (static_cast<unsigned char>(timestampType) & static_cast<unsigned char>(TIMESTAMP_TYPE::BEGIN)) != 0;
        }

        [[nodiscard]] bool isTimestampTypeDml() const {
            return (static_cast<unsigned char>(timestampType) & static_cast<unsigned char>(TIMESTAMP_TYPE::DML)) != 0;
        }

        [[nodiscard]] bool isTimestampTypeCommit() const {
            return (static_cast<unsigned char>(timestampType) & static_cast<unsigned char>(TIMESTAMP_TYPE::COMMIT)) != 0;
        }

        [[nodiscard]] bool isUserTypeBegin() const {
            return (static_cast<unsigned char>(userType) & static_cast<unsigned char>(USER_TYPE::BEGIN)) != 0;
        }

        [[nodiscard]] bool isUserTypeDml() const {
            return (static_cast<unsigned char>(userType) & static_cast<unsigned char>(USER_TYPE::DML)) != 0;
        }

        [[nodiscard]] bool isUserTypeCommit() const {
            return (static_cast<unsigned char>(userType) & static_cast<unsigned char>(USER_TYPE::COMMIT)) != 0;
        }

        [[nodiscard]] bool isUserTypeDdl() const {
            return (static_cast<unsigned char>(userType) & static_cast<unsigned char>(USER_TYPE::DDL)) != 0;
        }

        [[nodiscard]] bool isDbFormatAddDml() const {
            return (static_cast<unsigned char>(dbFormat) & static_cast<unsigned char>(DB_FORMAT::ADD_DML)) != 0;
        }

        [[nodiscard]] bool isDbFormatAddDdl() const {
            return (static_cast<unsigned char>(dbFormat) & static_cast<unsigned char>(DB_FORMAT::ADD_DDL)) != 0;
        }
    };

    constexpr auto operator+(Format::VALUE_TYPE e) noexcept {
        return static_cast<unsigned char>(e);
    }

    constexpr auto operator+(Format::MESSAGE_FORMAT e) noexcept {
        return static_cast<unsigned char>(e);
    }

}
#endif
