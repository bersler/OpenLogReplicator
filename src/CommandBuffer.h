/* Header for CommandBuffer class
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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
along with Open Log Replicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_map>
#include <stdint.h>

#include "types.h"

#ifndef COMMANDBUFFER_H_
#define COMMANDBUFFER_H_

#define ORA_CHARSET_CODE_UTF8               871
#define ORA_CHARSET_CODE_AL32UTF8           873
#define ORA_CHARSET_CODE_AL16UTF16          2000
#define ORA_CHARSET_CODE_COPY               100000

using namespace std;

namespace OpenLogReplicator {

    class Writer;
    class RedoLogRecord;
    class OracleAnalyser;
    class OracleObject;

    class CommandBuffer {
    protected:
        volatile bool shutdown;
        OracleAnalyser *oracleAnalyser;

        //conversion arrays for 7 bit character sets
        static typeunimap unicode_map_US7ASCII[128];
        static typeunimap unicode_map_SF7ASCII[128];
        static typeunimap unicode_map_D7DEC[128];
        static typeunimap unicode_map_S7DEC[128];
        static typeunimap unicode_map_E7DEC[128];
        static typeunimap unicode_map_I7DEC[128];
        static typeunimap unicode_map_NDK7DEC[128];
        static typeunimap unicode_map_SF7DEC[128];
        static typeunimap unicode_map_E7SIEMENS9780X[128];
        static typeunimap unicode_map_S7SIEMENS9780X[128];
        static typeunimap unicode_map_DK7SIEMENS9780X[128];
        static typeunimap unicode_map_I7SIEMENS9780X[128];
        static typeunimap unicode_map_N7SIEMENS9780X[128];
        static typeunimap unicode_map_D7SIEMENS9780X[128];

        //conversion arrays for 8 bit character sets
        //MS Windows character sets
        static typeunimap unicode_map_EE8MSWIN1250[256];
        static typeunimap unicode_map_CL8MSWIN1251[256];
        static typeunimap unicode_map_WE8MSWIN1252[256];
        static typeunimap unicode_map_EL8MSWIN1253[256];
        static typeunimap unicode_map_TR8MSWIN1254[256];
        static typeunimap unicode_map_AR8MSWIN1256[256];
        static typeunimap unicode_map_BLT8MSWIN1257[256];
        static typeunimap unicode_map_VN8MSWIN1258[256];
        static typeunimap unicode_map_IW8MSWIN1255[256];
        static typeunimap unicode_map_ET8MSWIN923[256];
        static typeunimap unicode_map_LT8MSWIN921[256];
        static typeunimap unicode_map_BG8MSWIN[256];

        //ISO 8859 character sets
        static typeunimap unicode_map_WE8ISO8859P1[256];
        static typeunimap unicode_map_EE8ISO8859P2[256];
        static typeunimap unicode_map_SE8ISO8859P3[256];
        static typeunimap unicode_map_NEE8ISO8859P4[256];
        static typeunimap unicode_map_CL8ISO8859P5[256];
        static typeunimap unicode_map_AR8ISO8859P6[256];
        static typeunimap unicode_map_EL8ISO8859P7[256];
        static typeunimap unicode_map_IW8ISO8859P8[256];
        static typeunimap unicode_map_WE8ISO8859P9[256];
        static typeunimap unicode_map_AZ8ISO8859P9E[256];
        static typeunimap unicode_map_NE8ISO8859P10[256];
        static typeunimap unicode_map_BLT8ISO8859P13[256];
        static typeunimap unicode_map_CEL8ISO8859P14[256];
        static typeunimap unicode_map_WE8ISO8859P15[256];

        //Mac character sets
        static typeunimap unicode_map_TR8MACTURKISHS[256];
        static typeunimap unicode_map_IS8MACICELANDICS[256];
        static typeunimap unicode_map_CL8MACCYRILLICS[256];
        static typeunimap unicode_map_AR8ARABICMACS[256];
        static typeunimap unicode_map_EL8MACGREEKS[256];
        static typeunimap unicode_map_EE8MACCROATIANS[256];
        static typeunimap unicode_map_WE8MACROMAN8S[256];
        static typeunimap unicode_map_TH8MACTHAIS[256];
        static typeunimap unicode_map_EE8MACCES[256];
        static typeunimap unicode_map_IW8MACHEBREWS[256];

        //IBM character sets
        static typeunimap unicode_map_LV8RST104090[256];
        static typeunimap unicode_map_BG8PC437S[256];
        static typeunimap unicode_map_EL8PC437S[256];
        static typeunimap unicode_map_US8PC437[256];
        static typeunimap unicode_map_EL8PC737[256];
        static typeunimap unicode_map_LT8PC772[256];
        static typeunimap unicode_map_LT8PC774[256];
        static typeunimap unicode_map_BLT8PC775[256];
        static typeunimap unicode_map_WE8PC850[256];
        static typeunimap unicode_map_EL8PC851[256];
        static typeunimap unicode_map_EE8PC852[256];
        static typeunimap unicode_map_RU8PC855[256];
        static typeunimap unicode_map_TR8PC857[256];
        static typeunimap unicode_map_WE8PC858[256];
        static typeunimap unicode_map_WE8PC860[256];
        static typeunimap unicode_map_IS8PC861[256];
        static typeunimap unicode_map_IW8PC1507[256];
        static typeunimap unicode_map_CDN8PC863[256];
        static typeunimap unicode_map_N8PC865[256];
        static typeunimap unicode_map_RU8PC866[256];
        static typeunimap unicode_map_EL8PC869[256];
        static typeunimap unicode_map_LV8PC1117[256];
        static typeunimap unicode_map_LV8PC8LR[256];

        //DOS character sets
        static typeunimap unicode_map_AR8ADOS710[256];
        static typeunimap unicode_map_AR8ADOS710T[256];
        static typeunimap unicode_map_AR8ADOS720[256];
        static typeunimap unicode_map_AR8ADOS720T[256];

        //DEC character sets
        static typeunimap unicode_map_TR8DEC[256];
        static typeunimap unicode_map_EL8DEC[256];
        static typeunimap unicode_map_WE8DEC[256];

        //other character sets
        static typeunimap unicode_map_TH8TISASCII[256];
        static typeunimap unicode_map_BN8BSCII[256];
        static typeunimap unicode_map_IN8ISCII[256];
        static typeunimap unicode_map_AR8APTEC715[256];
        static typeunimap unicode_map_AR8APTEC715T[256];
        static typeunimap unicode_map_AR8ASMO708PLUS[256];
        static typeunimap unicode_map_AR8ASMO8X[256];
        static typeunimap unicode_map_AR8HPARABIC8T[256];
        static typeunimap unicode_map_AR8MUSSAD768[256];
        static typeunimap unicode_map_AR8MUSSAD768T[256];
        static typeunimap unicode_map_AR8NAFITHA711[256];
        static typeunimap unicode_map_AR8NAFITHA711T[256];
        static typeunimap unicode_map_AR8NAFITHA721[256];
        static typeunimap unicode_map_AR8NAFITHA721T[256];
        static typeunimap unicode_map_AR8SAKHR706[256];
        static typeunimap unicode_map_AR8SAKHR707[256];
        static typeunimap unicode_map_AR8SAKHR707T[256];
        static typeunimap unicode_map_CL8KOI8R[256];
        static typeunimap unicode_map_CL8KOI8U[256];
        static typeunimap unicode_map_VN8VN3[256];
        static typeunimap unicode_map_LA8ISO6937[256];
        static typeunimap unicode_map_BLT8CP921[256];
        static typeunimap unicode_map_CL8ISOIR111[256];
        static typeunimap unicode_map_HU8ABMOD[256];
        static typeunimap unicode_map_HU8CWI2[256];
        static typeunimap unicode_map_LA8PASSPORT[256];
        static typeunimap unicode_map_RU8BESTA[256];
        static typeunimap unicode_map_WE8DG[256];
        static typeunimap unicode_map_WE8NCR4970[256];
        static typeunimap unicode_map_WE8NEXTSTEP[256];
        static typeunimap unicode_map_WE8ROMAN8[256];
        static typeunimap unicode_map_EEC8EUROASCI[256];
        static typeunimap unicode_map_EEC8EUROPA3[256];
        static typeunimap unicode_map_TIMESTEN8[256];
        static typeunimap unicode_map_WE8HP[256];

    public:
        uint64_t defaultCharacterMapId;
        uint64_t defaultCharacterNcharMapId;
        unordered_map<uint64_t, uint64_t> characterMapBits;
        unordered_map<uint64_t, const char*> characterMapName;
        unordered_map<uint64_t, typeunimap*> characterMap;
        static char translationMap[65];
        Writer *writer;
        uint8_t *intraThreadBuffer;
        mutex mtx;
        condition_variable analysersCond;
        condition_variable writerCond;
        volatile uint64_t posStart;
        volatile uint64_t posEnd;
        volatile uint64_t posEndTmp;
        volatile uint64_t posSize;
        uint64_t test;
        uint64_t timestampFormat;
        uint64_t outputBufferSize;

        void stop(void);
        void setOracleAnalyser(OracleAnalyser *oracleAnalyser);
        void setNlsCharset(string &nlsCharset, string &nlsNcharCharset);
        CommandBuffer* appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot);
        CommandBuffer* appendEscape(const uint8_t *str, uint64_t length);
        CommandBuffer* appendEscapeMap(const uint8_t *str, uint64_t length, uint64_t charsetId);
        CommandBuffer* appendChr(const char* str);
        CommandBuffer* appendStr(string &str);
        CommandBuffer* append(char chr);
        CommandBuffer* appendHex(uint64_t val, uint64_t length);
        CommandBuffer* appendDec(uint64_t val);
        CommandBuffer* appendScn(typescn scn);
        CommandBuffer* appendOperation(char *operation);
        CommandBuffer* appendTable(string &owner, string &table);
        CommandBuffer* appendTimestamp(const uint8_t *data, uint64_t length);
        CommandBuffer* appendValue(string &columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t charsetId, uint64_t fieldPos, uint64_t fieldLength);
        CommandBuffer* appendNull(string &columnName);
        CommandBuffer* appendMs(char *name, uint64_t time);
        CommandBuffer* appendXid(typexid xid);
        CommandBuffer* appendDbzCols(OracleObject *object);
        CommandBuffer* appendDbzHead(OracleObject *object);
        CommandBuffer* appendDbzTail(OracleObject *object, uint64_t time, typescn scn, char op, typexid xid);

        CommandBuffer* beginTran(void);
        CommandBuffer* commitTran(void);
        CommandBuffer* rewind(void);
        uint64_t currentTranSize(void);

        CommandBuffer(uint64_t outputBufferSize);
        virtual ~CommandBuffer();
    };
}

#endif
