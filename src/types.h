/* Definition of types and macros
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

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <ostream>
#include <sstream>
#include <string.h>
#include <time.h>

#include "../config.h"
#include "global.h"

#ifndef TYPES_H_
#define TYPES_H_

typedef uint32_t typeRESETLOGS;
typedef uint32_t typeACTIVATION;
typedef uint16_t typeSUM;
typedef uint16_t typeOP1;
typedef uint32_t typeOP2;
typedef int16_t typeCONID;
typedef uint64_t typeUBA;
typedef uint32_t typeSEQ;
typedef uint64_t typeSCN;
typedef uint16_t typeSubSCN;
typedef uint8_t typeSLT;
typedef uint32_t typeSQN;
typedef uint8_t typeRCI;
typedef int16_t typeUSN;
typedef uint64_t typeXID;
typedef uint64_t typeXIDMAP;
typedef uint16_t typeAFN;
typedef uint32_t typeDBA;
typedef uint16_t typeSLOT;
typedef uint32_t typeBLK;
typedef uint32_t typeOBJ;
typedef uint32_t typeDATAOBJ;
typedef uint64_t typeOBJ2;
typedef int16_t typeCOL;
typedef uint16_t typeTYPE;
typedef uint32_t typeCON;
typedef uint32_t typeUSER;
typedef uint8_t  typeOPTIONS;
typedef uint16_t typeFIELD;

typedef uint16_t typeunicode16;
typedef uint32_t typeunicode32;
typedef uint64_t typeunicode;

#define CONFIG_SCHEMA_VERSION                   "0.9.34"
#define CHECKPOINT_FILE_MAX_SIZE                1024
#define CONFIG_FILE_MAX_SIZE                    1048576
#define SCHEMA_FILE_MAX_SIZE                    2147483648
#define ZERO_SEQ                                ((typeSEQ)0xFFFFFFFF)
#define ZERO_SCN                                ((typeSCN)0xFFFFFFFFFFFFFFFF)
#define ZERO_BLK                                ((typeBLK)0xFFFFFFFF)
#define MEMORY_ALIGNMENT                        512
#define MAX_PATH_LENGTH                         2048
#define MAX_FIELD_LENGTH                        1048576
#define MAX_NO_COLUMNS                          1000
#define MAX_TRANSACTIONS_LIMIT                  1048576
#define MAX_RECORDS_IN_LWN                      1048576
#define MEMORY_CHUNK_SIZE_MB                    1
#define MEMORY_CHUNK_SIZE_MB_CHR                "1"
#define MEMORY_CHUNK_SIZE                       (MEMORY_CHUNK_SIZE_MB*1024*1024)
#define MEMORY_CHUNK_MIN_MB                     16
#define MEMORY_CHUNK_MIN_MB_CHR                 "16"

#define ARCH_LOG_PATH                           0
#define ARCH_LOG_ONLINE                         1
#define ARCH_LOG_ONLINE_KEEP                    2
#define ARCH_LOG_LIST                           3

#define MESSAGE_FORMAT_DEFAULT                  0
#define MESSAGE_FORMAT_FULL                     1
#define MESSAGE_FORMAT_ADD_SEQUENCES            2
//JSON only:
#define MESSAGE_FORMAT_SKIP_BEGIN               4
#define MESSAGE_FORMAT_SKIP_COMMIT              8

#define TIMESTAMP_FORMAT_UNIX                   0
#define TIMESTAMP_FORMAT_ISO8601                1
#define TIMESTAMP_FORMAT_ALL_PAYLOADS           2

#define CHAR_FORMAT_UTF8                        0
#define CHAR_FORMAT_NOMAPPING                   1
#define CHAR_FORMAT_HEX                         2

#define SCN_FORMAT_NUMERIC                      0
#define SCN_FORMAT_HEX                          1
#define SCN_FORMAT_ALL_PAYLOADS                 2

#define RID_FORMAT_SKIP                         0
#define RID_FORMAT_DEFAULT                      1

#define XID_FORMAT_TEXT                         0
#define XID_FORMAT_NUMERIC                      1

#define UNKNOWN_FORMAT_QUESTION_MARK            0
#define UNKNOWN_FORMAT_DUMP                     1

#define SCHEMA_FORMAT_NAME                      0
#define SCHEMA_FORMAT_FULL                      1
#define SCHEMA_FORMAT_REPEATED                  2
#define SCHEMA_FORMAT_OBJ                       4

#define UNKNOWN_TYPE_HIDE                       0
#define UNKNOWN_TYPE_SHOW                       1

//default, only changed columns for update, or PK
#define COLUMN_FORMAT_CHANGED                   0
//show full nulls from insert & delete
#define COLUMN_FORMAT_FULL_INS_DEC              1
//show all from redo
#define COLUMN_FORMAT_FULL_UPD                  2

#define TRACE_SILENT                            0
#define TRACE_ERROR                             1
#define TRACE_WARNING                           2
#define TRACE_INFO                              3
#define TRACE_DEBUG                             4

#define TRACE2_DML                              0x00000001
#define TRACE2_DUMP                             0x00000002
#define TRACE2_LWN                              0x00000004
#define TRACE2_THREADS                          0x00000008
#define TRACE2_SQL                              0x00000010
#define TRACE2_FILE                             0x00000020
#define TRACE2_DISK                             0x00000040
#define TRACE2_MEMORY                           0x00000080
#define TRACE2_PERFORMANCE                      0x00000100
#define TRACE2_TRANSACTION                      0x00000200
#define TRACE2_REDO                             0x00000400
#define TRACE2_ARCHIVE_LIST                     0x00000800
#define TRACE2_SCHEMA_LIST                      0x00001000
#define TRACE2_WRITER                           0x00002000
#define TRACE2_CHECKPOINT                       0x00004000
#define TRACE2_SYSTEM                           0x00008000

#define REDO_FLAGS_ARCH_ONLY                    0x00000001
#define REDO_FLAGS_SCHEMALESS                   0x00000002
#define REDO_FLAGS_DIRECT                       0x00000004
#define REDO_FLAGS_NOATIME                      0x00000008
#define REDO_FLAGS_ON_ERROR_CONTINUE            0x00000010
#define REDO_FLAGS_TRACK_DDL                    0x00000020
#define REDO_FLAGS_SHOW_INVISIBLE_COLUMNS       0x00000040
#define REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS      0x00000080
#define REDO_FLAGS_SHOW_NESTED_COLUMNS          0x00000100
#define REDO_FLAGS_SHOW_UNUSED_COLUMNS          0x00000200
#define REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS 0x00000400
#define REDO_FLAGS_SHOW_SYSTEM_TRANSACTIONS     0x00000800
#define REDO_FLAGS_CHECKPOINT_KEEP              0x00001000
#define REDO_FLAGS_SCHEMA_KEEP                  0x00002000

#define DISABLE_CHECK_GRANTS                    0x00000001
#define DISABLE_CHECK_SUPPLEMENTAL_LOG          0x00000002
#define DISABLE_CHECK_BLOCK_SUM                 0x00000004

#define TRANSACTION_INSERT                      1
#define TRANSACTION_DELETE                      2
#define TRANSACTION_UPDATE                      3

#define OUTPUT_BUFFER_DATA_SIZE                 (MEMORY_CHUNK_SIZE - sizeof(struct OutputBufferQueue))
#define OUTPUT_BUFFER_ALLOCATED                 0x0001
#define OUTPUT_BUFFER_CONFIRMED                 0x0002

#define VALUE_BEFORE                            0
#define VALUE_AFTER                             1
#define VALUE_BEFORE_SUPP                       2
#define VALUE_AFTER_SUPP                        3

#define OPTIONS_DEBUG_TABLE                     1
#define OPTIONS_SYSTEM_TABLE                    2

#define TABLE_SYS_CCOL                          1
#define TABLE_SYS_CDEF                          2
#define TABLE_SYS_COL                           3
#define TABLE_SYS_DEFERRED_STG                  4
#define TABLE_SYS_ECOL                          5
#define TABLE_SYS_OBJ                           6
#define TABLE_SYS_TAB                           7
#define TABLE_SYS_TABPART                       8
#define TABLE_SYS_TABCOMPART                    9
#define TABLE_SYS_TABSUBPART                    10
#define TABLE_SYS_USER                          11

#define USN(__xid)                              ((typeUSN)(((uint64_t)(__xid))>>48))
#define SLT(__xid)                              ((typeSLT)(((((uint64_t)(__xid))>>32)&0xFFFF)))
#define SQN(__xid)                              ((typeSQN)(((__xid)&0xFFFFFFFF)))
#define XID(__usn,__slt,__sqn)                  ((((uint64_t)(__usn))<<48)|(((uint64_t)(__slt))<<32)|((uint64_t)(__sqn)))
#define PRINTXID(__xid)                         "0x"<<setfill('0')<<setw(4)<<hex<<USN(__xid)<<"."<<setw(3)<<(uint64_t)SLT(__xid)<<"."<<setw(8)<<SQN(__xid)

#define BLOCK(__uba)                            ((uint32_t)((__uba)&0xFFFFFFFF))
#define SEQUENCE(__uba)                         ((uint16_t)((((uint64_t)(__uba))>>32)&0xFFFF))
#define RECORD(__uba)                           ((uint8_t)((((uint64_t)(__uba))>>48)&0xFF))
#define PRINTUBA(__uba)                         "0x"<<setfill('0')<<setw(8)<<hex<<BLOCK(__uba)<<"."<<setfill('0')<<setw(4)<<hex<<SEQUENCE(__uba)<<"."<<setfill('0')<<setw(2)<<hex<<(uint32_t)RECORD(__uba)

#define SCN(__scn1,__scn2)                      ((((uint64_t)(__scn1))<<32)|(__scn2))
#define PRINTSCN48(__scn)                       "0x"<<setfill('0')<<setw(4)<<hex<<((uint32_t)((__scn)>>32)&0xFFFF)<<"."<<setw(8)<<((__scn)&0xFFFFFFFF)
#define PRINTSCN64(__scn)                       "0x"<<setfill('0')<<setw(16)<<hex<<(__scn)

#define ERROR(__x)                              {if (trace >= TRACE_ERROR) {stringstream __s; time_t now = time(nullptr); tm nowTm = *localtime(&now); char str[50]; strftime(str, sizeof(str), "%F %T", &nowTm); __s << str << " [ERROR] " << __x << endl; cerr << __s.str(); } }
#define WARNING(__x)                            {if (trace >= TRACE_WARNING) {stringstream __s; time_t now = time(nullptr); tm nowTm = *localtime(&now); char str[50]; strftime(str, sizeof(str), "%F %T", &nowTm); __s << str << " [WARNING] " << __x << endl; cerr << __s.str();} }
#define INFO(__x)                               {if (trace >= TRACE_INFO) {stringstream __s; time_t now = time(nullptr); tm nowTm = *localtime(&now); char str[50]; strftime(str, sizeof(str), "%F %T", &nowTm); __s << str << " [INFO] " << __x << endl; cerr << __s.str();} }
#define DEBUG(__x)                              {if (trace >= TRACE_DEBUG) {stringstream __s; time_t now = time(nullptr); tm nowTm = *localtime(&now); char str[50]; strftime(str, sizeof(str), "%F %T", &nowTm); __s << str << " [DEBUG] " << __x << endl; cerr << __s.str();} }
#define TRACE(__t,__x)                          {if ((trace2 & (__t)) != 0) {stringstream __s; time_t now = time(nullptr); tm nowTm = *localtime(&now); char str[50]; strftime(str, sizeof(str), "%F %T", &nowTm); __s << str << " [TRACE] " << __x << endl; cerr << __s.str();} }
#define CONFIG_FAIL(__x)                        {if (trace >= TRACE_ERROR) {stringstream __s; time_t now = time(nullptr); tm nowTm = *localtime(&now); char str[50]; strftime(str, sizeof(str), "%F %T", &nowTm); __s << str << " [ERROR] " << __x << endl; cerr << __s.str(); }; throw ConfigurationException("error");}
#define NETWORK_FAIL(__x)                       {if (trace >= TRACE_ERROR) {stringstream __s; time_t now = time(nullptr); tm nowTm = *localtime(&now); char str[50]; strftime(str, sizeof(str), "%F %T", &nowTm); __s << str << " [ERROR] " << __x << endl; cerr << __s.str(); }; throw NetworkException("error");}
#define REDOLOG_FAIL(__x)                       {if (trace >= TRACE_ERROR) {stringstream __s; time_t now = time(nullptr); tm nowTm = *localtime(&now); char str[50]; strftime(str, sizeof(str), "%F %T", &nowTm); __s << str << " [ERROR] " << __x << endl; cerr << __s.str(); }; throw RedoLogException("error");}
#define RUNTIME_FAIL(__x)                       {if (trace >= TRACE_ERROR) {stringstream __s; time_t now = time(nullptr); tm nowTm = *localtime(&now); char str[50]; strftime(str, sizeof(str), "%F %T", &nowTm); __s << str << " [ERROR] " << __x << endl; cerr << __s.str(); }; throw RuntimeException("error");}

#define TYPEINTXLEN                             2
#define TYPEINTXDIGITS                          77

#define FLAGS_XA                0x01
#define FLAGS_XR                0x02
#define FLAGS_CR                0x03
#define FLAGS_KDO_KDOM2         0x80

#define FLG_KTUCF_OP0504        0x0002
#define FLG_ROLLBACK_OP0504     0x0004

#define FLG_MULTIBLOCKUNDOHEAD  0x0001
#define FLG_MULTIBLOCKUNDOTAIL  0x0002
#define FLG_LASTBUFFERSPLIT     0x0004
#define FLG_KTUBL               0x0008
#define FLG_USERUNDODDONE       0x0010
#define FLG_ISTEMPOBJECT        0x0020
#define FLG_USERONLY            0x0040
#define FLG_TABLESPACEUNDO      0x0080
#define FLG_MULTIBLOCKUNDOMID   0x0100

#define FB_N                    0x01
#define FB_P                    0x02
#define FB_L                    0x04
#define FB_F                    0x08
#define FB_D                    0x10
#define FB_H                    0x20
#define FB_C                    0x40
#define FB_K                    0x80

#define OP_IUR                  0x01
#define OP_IRP                  0x02
#define OP_DRP                  0x03
#define OP_LKR                  0x04
#define OP_URP                  0x05
#define OP_ORP                  0x06
#define OP_MFC                  0x07
#define OP_CFA                  0x08
#define OP_CKI                  0x09
#define OP_SKL                  0x0A
#define OP_QMI                  0x0B
#define OP_QMD                  0x0C
#define OP_DSC                  0x0E
#define OP_LMN                  0x10
#define OP_LLB                  0x11
#define OP__19                  0x13
#define OP_SHK                  0x14
#define OP__21                  0x15
#define OP_CMP                  0x16
#define OP_DCU                  0x17
#define OP_MRK                  0x18
#define OP_ROWDEPENDENCIES      0x40

#define KTBOP_F                 0x01
#define KTBOP_C                 0x02
#define KTBOP_Z                 0x03
#define KTBOP_L                 0x04
#define KTBOP_N                 0x06
#define KTBOP_BLOCKCLEANOUT     0x10

#define SUPPLOG_UPDATE          0x01
#define SUPPLOG_INSERT          0x02
#define SUPPLOG_DELETE          0x04

#define OPFLAG_BEGIN_TRANS      0x01

#define JSON_PARAMETER_LENGTH   256
#define JSON_BROKERS_LENGTH     4096
#define JSON_TOPIC_LENGTH       256

#define JSON_USERNAME_LENGTH    128
#define JSON_PASSWORD_LENGTH    128
#define JSON_SERVER_LENGTH      4096
#define JSON_KEY_LENGTH         4096
#define JSON_XID_LIST_LENGTH    1048576

#define VCONTEXT_LENGTH         30
#define VPARAMETER_LENGTH       4000
#define VPROPERTY_LENGTH        4000

using namespace std;

namespace OpenLogReplicator {
    class OracleAnalyzer;

    class uintX_t {
    private:
        uint64_t data[TYPEINTXLEN];
        static uintX_t BASE10[TYPEINTXDIGITS][10];
    public:
        uintX_t(uint64_t val);
        uintX_t();
        ~uintX_t();

        static void initializeBASE10(void);

        bool operator!=(const uintX_t& other) const;
        bool operator==(const uintX_t& other) const;
        uintX_t& operator+=(const uintX_t& val);
        uintX_t& operator=(const uintX_t& val);
        uintX_t& operator=(uint64_t val);
        uintX_t& operator=(const string& val);
        uintX_t& operator=(const char* val);
        uintX_t& set(uint64_t val1, uint64_t val2);
        uintX_t& setStr(const char* val, uint64_t length);
        uint64_t get64(void);
        bool isSet64(uint64_t mask);
        bool isZero();

        friend ostream& operator<<(ostream& os, const uintX_t& val);
    };

    class typeTIME {
        uint32_t val;
    public:
        typeTIME(void) :
            val(0) {
        }

        typeTIME(uint32_t val) :
            val(val) {
        }

        uint32_t getVal(void) {
            return this->val;
        }

        typeTIME& operator= (uint32_t val) {
            this->val = val;
            return *this;
        }

        time_t toTime(void) {
            struct tm epochtime = {0};
            memset((void*) &epochtime, 0, sizeof(epochtime));
            uint64_t rest = val;
            epochtime.tm_sec = rest % 60; rest /= 60;
            epochtime.tm_min = rest % 60; rest /= 60;
            epochtime.tm_hour = rest % 24; rest /= 24;
            epochtime.tm_mday = (rest % 31) + 1; rest /= 31;
            epochtime.tm_mon = (rest % 12); rest /= 12;
            epochtime.tm_year = rest + 88;
            return mktime(&epochtime);
        }

        void toISO8601(char* buffer) {
            uint64_t rest = val;
            uint64_t ss = rest % 60; rest /= 60;
            uint64_t mi = rest % 60; rest /= 60;
            uint64_t hh = rest % 24; rest /= 24;
            uint64_t dd = (rest % 31) + 1; rest /= 31;
            uint64_t mm = (rest % 12) + 1; rest /= 12;
            uint64_t yy = rest + 1988;
            buffer[3] = '0' + (yy % 10); yy /= 10;
            buffer[2] = '0' + (yy % 10); yy /= 10;
            buffer[1] = '0' + (yy % 10); yy /= 10;
            buffer[0] = '0' + yy;
            buffer[4] = '-';
            buffer[6] = '0' + (mm % 10); mm /= 10;
            buffer[5] = '0' + mm;
            buffer[7] = '-';
            buffer[9] = '0' + (dd % 10); dd /= 10;
            buffer[8] = '0' + dd;
            buffer[10] = 'T';
            buffer[12] = '0' + (hh % 10); hh /= 10;
            buffer[11] = '0' + hh;
            buffer[13] = ':';
            buffer[15] = '0' + (mi % 10); mi /= 10;
            buffer[14] = '0' + mi;
            buffer[16] = ':';
            buffer[18] = '0' + (ss % 10); ss /= 10;
            buffer[17] = '0' + ss;
            buffer[19] = 'Z';
            buffer[20] = 0;
            //01234567890123456789
            //YYYY-MM-DDThh:mm:ssZ
        }

        friend ostream& operator<<(ostream& os, const typeTIME& time_) {
            uint64_t rest = time_.val;
            uint64_t ss = rest % 60; rest /= 60;
            uint64_t mi = rest % 60; rest /= 60;
            uint64_t hh = rest % 24; rest /= 24;
            uint64_t dd = (rest % 31) + 1; rest /= 31;
            uint64_t mm = (rest % 12) + 1; rest /= 12;
            uint64_t yy = rest + 1988;
            os << setfill('0') << setw(2) << dec << mm << "/" << setfill('0') << setw(2) << dec << dd << "/" << yy << " " <<
                    setfill('0') << setw(2) << dec << hh << ":" << setfill('0') << setw(2) << dec << mi << ":" << setfill('0') << setw(2) << dec << ss;
            return os;
            //0123456789012345678
            //DDDDDDDDDD HHHHHHHH
            //10/15/2018 22:25:36
        }
    };
}

#endif
