/* Memory buffer for handling JSON data
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

#include <iostream>
#include <string.h>

#include "CommandBuffer.h"
#include "MemoryException.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"

namespace OpenLogReplicator {

    CommandBuffer::CommandBuffer(uint64_t outputBufferSize) :
            shutdown(false),
            oracleAnalyser(nullptr),
            defaultCharacterMapId(0),
            defaultCharacterNcharMapId(0),
            writer(nullptr),
            posStart(0),
            posEnd(0),
            posEndTmp(0),
            posSize(0),
            test(0),
            timestampFormat(0),
            outputBufferSize(outputBufferSize) {
        intraThreadBuffer = new uint8_t[outputBufferSize];

        //7-bit character sets
        characterMapBits[1] = 7; characterMapName[1] = "US7ASCII"; characterMap[1] = unicode_map_US7ASCII;
        characterMapBits[15] = 7; characterMapName[15] = "SF7ASCII"; characterMap[15] = unicode_map_SF7ASCII;
        characterMapBits[11] = 7; characterMapName[11] = "D7DEC"; characterMap[11] = unicode_map_D7DEC;
        characterMapBits[13] = 7; characterMapName[13] = "S7DEC"; characterMap[13] = unicode_map_S7DEC;
        characterMapBits[14] = 7; characterMapName[14] = "E7DEC"; characterMap[14] = unicode_map_E7DEC;
        characterMapBits[17] = 7; characterMapName[17] = "I7DEC"; characterMap[17] = unicode_map_I7DEC;
        characterMapBits[16] = 7; characterMapName[16] = "NDK7DEC"; characterMap[16] = unicode_map_NDK7DEC;
        characterMapBits[21] = 7; characterMapName[21] = "SF7DEC"; characterMap[21] = unicode_map_SF7DEC;
        characterMapBits[202] = 7; characterMapName[202] = "E7SIEMENS9780X"; characterMap[202] = unicode_map_E7SIEMENS9780X;
        characterMapBits[203] = 7; characterMapName[203] = "S7SIEMENS9780X"; characterMap[203] = unicode_map_S7SIEMENS9780X;
        characterMapBits[204] = 7; characterMapName[204] = "DK7SIEMENS9780X"; characterMap[204] = unicode_map_DK7SIEMENS9780X;
        characterMapBits[206] = 7; characterMapName[206] = "I7SIEMENS9780X"; characterMap[206] = unicode_map_I7SIEMENS9780X;
        characterMapBits[205] = 7; characterMapName[205] = "N7SIEMENS9780X"; characterMap[205] = unicode_map_N7SIEMENS9780X;
        characterMapBits[207] = 7; characterMapName[207] = "D7SIEMENS9780X"; characterMap[207] = unicode_map_D7SIEMENS9780X;

        //8-bit character sets
        //MS Windows character sets
        characterMapBits[176] = 8; characterMapName[176] = "LT8MSWIN921"; characterMap[176] = unicode_map_LT8MSWIN921;
        characterMapBits[172] = 8; characterMapName[172] = "ET8MSWIN923"; characterMap[172] = unicode_map_ET8MSWIN923;
        characterMapBits[170] = 8; characterMapName[170] = "EE8MSWIN1250"; characterMap[170] = unicode_map_EE8MSWIN1250;
        characterMapBits[171] = 8; characterMapName[171] = "CL8MSWIN1251"; characterMap[171] = unicode_map_CL8MSWIN1251;
        characterMapBits[178] = 8; characterMapName[178] = "WE8MSWIN1252"; characterMap[178] = unicode_map_WE8MSWIN1252;
        characterMapBits[174] = 8; characterMapName[174] = "EL8MSWIN1253"; characterMap[174] = unicode_map_EL8MSWIN1253;
        characterMapBits[177] = 8; characterMapName[177] = "TR8MSWIN1254"; characterMap[177] = unicode_map_TR8MSWIN1254;
        characterMapBits[175] = 8; characterMapName[175] = "IW8MSWIN1255"; characterMap[175] = unicode_map_IW8MSWIN1255;
        characterMapBits[560] = 8; characterMapName[560] = "AR8MSWIN1256"; characterMap[560] = unicode_map_AR8MSWIN1256;
        characterMapBits[179] = 8; characterMapName[179] = "BLT8MSWIN1257"; characterMap[179] = unicode_map_BLT8MSWIN1257;
        characterMapBits[45] = 8; characterMapName[45] = "VN8MSWIN1258"; characterMap[45] = unicode_map_VN8MSWIN1258;
        characterMapBits[173] = 8; characterMapName[173] = "BG8MSWIN"; characterMap[173] = unicode_map_BG8MSWIN;

        //ISO 8859 character sets
        characterMapBits[31] = 8; characterMapName[31] = "WE8ISO8859P1"; characterMap[31] = unicode_map_WE8ISO8859P1;
        characterMapBits[32] = 8; characterMapName[32] = "EE8ISO8859P2"; characterMap[32] = unicode_map_EE8ISO8859P2;
        characterMapBits[33] = 8; characterMapName[33] = "SE8ISO8859P3"; characterMap[33] = unicode_map_SE8ISO8859P3;
        characterMapBits[34] = 8; characterMapName[34] = "NEE8ISO8859P4"; characterMap[34] = unicode_map_NEE8ISO8859P4;
        characterMapBits[35] = 8; characterMapName[35] = "CL8ISO8859P5"; characterMap[35] = unicode_map_CL8ISO8859P5;
        characterMapBits[36] = 8; characterMapName[36] = "AR8ISO8859P6"; characterMap[36] = unicode_map_AR8ISO8859P6;
        characterMapBits[37] = 8; characterMapName[37] = "EL8ISO8859P7"; characterMap[37] = unicode_map_EL8ISO8859P7;
        characterMapBits[38] = 8; characterMapName[38] = "IW8ISO8859P8"; characterMap[38] = unicode_map_IW8ISO8859P8;
        characterMapBits[39] = 8; characterMapName[39] = "WE8ISO8859P9"; characterMap[39] = unicode_map_WE8ISO8859P9;
        characterMapBits[52] = 8; characterMapName[52] = "AZ8ISO8859P9E"; characterMap[52] = unicode_map_AZ8ISO8859P9E;
        characterMapBits[40] = 8; characterMapName[40] = "NE8ISO8859P10"; characterMap[40] = unicode_map_NE8ISO8859P10;
        characterMapBits[47] = 8; characterMapName[47] = "BLT8ISO8859P13"; characterMap[47] = unicode_map_BLT8ISO8859P13;
        characterMapBits[48] = 8; characterMapName[48] = "CEL8ISO8859P14"; characterMap[48] = unicode_map_CEL8ISO8859P14;
        characterMapBits[46] = 8; characterMapName[46] = "WE8ISO8859P15"; characterMap[46] = unicode_map_WE8ISO8859P15;

        //Mac character sets
        characterMapBits[159] = 8; characterMapName[159] = "CL8MACCYRILLICS"; characterMap[159] = unicode_map_CL8MACCYRILLICS;
        characterMapBits[162] = 8; characterMapName[162] = "EE8MACCES"; characterMap[162] = unicode_map_EE8MACCES;
        characterMapBits[163] = 8; characterMapName[163] = "EE8MACCROATIANS"; characterMap[163] = unicode_map_EE8MACCROATIANS;
        characterMapBits[164] = 8; characterMapName[164] = "TR8MACTURKISHS"; characterMap[164] = unicode_map_TR8MACTURKISHS;
        characterMapBits[165] = 8; characterMapName[165] = "IS8MACICELANDICS"; characterMap[165] = unicode_map_IS8MACICELANDICS;
        characterMapBits[166] = 8; characterMapName[166] = "EL8MACGREEKS"; characterMap[166] = unicode_map_EL8MACGREEKS;
        characterMapBits[167] = 8; characterMapName[167] = "IW8MACHEBREWS"; characterMap[167] = unicode_map_IW8MACHEBREWS;
        characterMapBits[352] = 8; characterMapName[352] = "WE8MACROMAN8S"; characterMap[352] = unicode_map_WE8MACROMAN8S;
        characterMapBits[354] = 8; characterMapName[354] = "TH8MACTHAIS"; characterMap[354] = unicode_map_TH8MACTHAIS;
        characterMapBits[566] = 8; characterMapName[566] = "AR8ARABICMACS"; characterMap[566] = unicode_map_AR8ARABICMACS;

        //IBM character sets
        characterMapBits[4] = 8; characterMapName[4] = "US8PC437"; characterMap[4] = unicode_map_US8PC437;
        characterMapBits[10] = 8; characterMapName[10] = "WE8PC850"; characterMap[10] = unicode_map_WE8PC850;
        characterMapBits[28] = 8; characterMapName[28] = "WE8PC858"; characterMap[28] = unicode_map_WE8PC858;
        characterMapBits[140] = 8; characterMapName[140] = "BG8PC437S"; characterMap[140] = unicode_map_BG8PC437S;
        characterMapBits[150] = 8; characterMapName[150] = "EE8PC852"; characterMap[150] = unicode_map_EE8PC852;
        characterMapBits[152] = 8; characterMapName[152] = "RU8PC866"; characterMap[152] = unicode_map_RU8PC866;
        characterMapBits[154] = 8; characterMapName[154] = "IW8PC1507"; characterMap[154] = unicode_map_IW8PC1507;
        characterMapBits[155] = 8; characterMapName[155] = "RU8PC855"; characterMap[155] = unicode_map_RU8PC855;
        characterMapBits[156] = 8; characterMapName[156] = "TR8PC857"; characterMap[156] = unicode_map_TR8PC857;
        characterMapBits[160] = 8; characterMapName[160] = "WE8PC860"; characterMap[160] = unicode_map_WE8PC860;
        characterMapBits[161] = 8; characterMapName[161] = "IS8PC861"; characterMap[161] = unicode_map_IS8PC861;
        characterMapBits[190] = 8; characterMapName[190] = "N8PC865"; characterMap[190] = unicode_map_N8PC865;
        characterMapBits[191] = 8; characterMapName[191] = "BLT8CP921"; characterMap[191] = unicode_map_BLT8CP921;
        characterMapBits[192] = 8; characterMapName[192] = "LV8PC1117"; characterMap[192] = unicode_map_LV8PC1117;
        characterMapBits[193] = 8; characterMapName[193] = "LV8PC8LR"; characterMap[193] = unicode_map_LV8PC8LR;
        characterMapBits[197] = 8; characterMapName[197] = "BLT8PC775"; characterMap[197] = unicode_map_BLT8PC775;
        characterMapBits[380] = 8; characterMapName[380] = "EL8PC437S"; characterMap[380] = unicode_map_EL8PC437S;
        characterMapBits[382] = 8; characterMapName[382] = "EL8PC737"; characterMap[382] = unicode_map_EL8PC737;
        characterMapBits[383] = 8; characterMapName[383] = "LT8PC772"; characterMap[383] = unicode_map_LT8PC772;
        characterMapBits[384] = 8; characterMapName[384] = "LT8PC774"; characterMap[384] = unicode_map_LT8PC774;
        characterMapBits[385] = 8; characterMapName[385] = "EL8PC869"; characterMap[385] = unicode_map_EL8PC869;
        characterMapBits[386] = 8; characterMapName[386] = "EL8PC851"; characterMap[386] = unicode_map_EL8PC851;
        characterMapBits[390] = 8; characterMapName[390] = "CDN8PC863"; characterMap[390] = unicode_map_CDN8PC863;

        //DOS character sets
        characterMapBits[507] = 8; characterMapName[507] = "AR8ADOS710T"; characterMap[507] = unicode_map_AR8ADOS710T;
        characterMapBits[508] = 8; characterMapName[508] = "AR8ADOS720T"; characterMap[508] = unicode_map_AR8ADOS720T;
        characterMapBits[557] = 8; characterMapName[557] = "AR8ADOS710"; characterMap[557] = unicode_map_AR8ADOS710;
        characterMapBits[558] = 8; characterMapName[558] = "AR8ADOS720"; characterMap[558] = unicode_map_AR8ADOS720;

        //DEC character sets
        characterMapBits[2] = 8; characterMapName[2] = "WE8DEC"; characterMap[2] = unicode_map_WE8DEC;
        characterMapBits[81] = 8; characterMapName[81] = "EL8DEC"; characterMap[81] = unicode_map_EL8DEC;
        characterMapBits[82] = 8; characterMapName[82] = "TR8DEC"; characterMap[82] = unicode_map_TR8DEC;

        //other character sets
        characterMapBits[3] = 8; characterMapName[3] = "WE8HP"; characterMap[3] = unicode_map_WE8HP;
        characterMapBits[25] = 8; characterMapName[25] = "IN8ISCII"; characterMap[25] = unicode_map_IN8ISCII;
        characterMapBits[41] = 8; characterMapName[41] = "TH8TISASCII"; characterMap[41] = unicode_map_TH8TISASCII;
        characterMapBits[43] = 8; characterMapName[43] = "BN8BSCII"; characterMap[43] = unicode_map_BN8BSCII;
        characterMapBits[44] = 8; characterMapName[44] = "VN8VN3"; characterMap[44] = unicode_map_VN8VN3;
        characterMapBits[49] = 8; characterMapName[49] = "CL8ISOIR111"; characterMap[49] = unicode_map_CL8ISOIR111;
        characterMapBits[50] = 8; characterMapName[50] = "WE8NEXTSTEP"; characterMap[50] = unicode_map_WE8NEXTSTEP;
        characterMapBits[51] = 8; characterMapName[51] = "CL8KOI8U"; characterMap[51] = unicode_map_CL8KOI8U;
        characterMapBits[61] = 8; characterMapName[61] = "AR8ASMO708PLUS"; characterMap[61] = unicode_map_AR8ASMO708PLUS;
        characterMapBits[110] = 8; characterMapName[110] = "EEC8EUROASCI"; characterMap[110] = unicode_map_EEC8EUROASCI;
        characterMapBits[113] = 8; characterMapName[113] = "EEC8EUROPA3"; characterMap[113] = unicode_map_EEC8EUROPA3;
        characterMapBits[114] = 8; characterMapName[114] = "LA8PASSPORT"; characterMap[114] = unicode_map_LA8PASSPORT;
        characterMapBits[153] = 8; characterMapName[153] = "RU8BESTA"; characterMap[153] = unicode_map_RU8BESTA;
        characterMapBits[195] = 8; characterMapName[195] = "LV8RST104090"; characterMap[195] = unicode_map_LV8RST104090;
        characterMapBits[196] = 8; characterMapName[196] = "CL8KOI8R"; characterMap[196] = unicode_map_CL8KOI8R;
        characterMapBits[241] = 8; characterMapName[241] = "WE8DG"; characterMap[241] = unicode_map_WE8DG;
        characterMapBits[251] = 8; characterMapName[251] = "WE8NCR4970"; characterMap[251] = unicode_map_WE8NCR4970;
        characterMapBits[261] = 8; characterMapName[261] = "WE8ROMAN8"; characterMap[261] = unicode_map_WE8ROMAN8;
        characterMapBits[368] = 8; characterMapName[368] = "HU8CWI2"; characterMap[368] = unicode_map_HU8CWI2;
        characterMapBits[401] = 8; characterMapName[401] = "HU8ABMOD"; characterMap[401] = unicode_map_HU8ABMOD;
        characterMapBits[500] = 8; characterMapName[500] = "AR8ASMO8X"; characterMap[500] = unicode_map_AR8ASMO8X;
        characterMapBits[504] = 8; characterMapName[504] = "AR8NAFITHA711T"; characterMap[504] = unicode_map_AR8NAFITHA711T;
        characterMapBits[505] = 8; characterMapName[505] = "AR8SAKHR707T"; characterMap[505] = unicode_map_AR8SAKHR707T;
        characterMapBits[506] = 8; characterMapName[506] = "AR8MUSSAD768T"; characterMap[506] = unicode_map_AR8MUSSAD768T;
        characterMapBits[509] = 8; characterMapName[509] = "AR8APTEC715T"; characterMap[509] = unicode_map_AR8APTEC715T;
        characterMapBits[511] = 8; characterMapName[511] = "AR8NAFITHA721T"; characterMap[511] = unicode_map_AR8NAFITHA721T;
        characterMapBits[514] = 8; characterMapName[514] = "AR8HPARABIC8T"; characterMap[514] = unicode_map_AR8HPARABIC8T;
        characterMapBits[554] = 8; characterMapName[554] = "AR8NAFITHA711"; characterMap[554] = unicode_map_AR8NAFITHA711;
        characterMapBits[555] = 8; characterMapName[555] = "AR8SAKHR707"; characterMap[555] = unicode_map_AR8SAKHR707;
        characterMapBits[556] = 8; characterMapName[556] = "AR8MUSSAD768"; characterMap[556] = unicode_map_AR8MUSSAD768;
        characterMapBits[559] = 8; characterMapName[559] = "AR8APTEC715"; characterMap[559] = unicode_map_AR8APTEC715;
        characterMapBits[561] = 8; characterMapName[561] = "AR8NAFITHA721"; characterMap[561] = unicode_map_AR8NAFITHA721;
        characterMapBits[563] = 8; characterMapName[563] = "AR8SAKHR706"; characterMap[563] = unicode_map_AR8SAKHR706;
        characterMapBits[590] = 8; characterMapName[590] = "LA8ISO6937"; characterMap[590] = unicode_map_LA8ISO6937;
        characterMapBits[1002] = 8; characterMapName[1002] = "TIMESTEN8"; characterMap[1002] = unicode_map_TIMESTEN8;

        //16-bit character sets - no translation
        characterMapBits[829] = 16; characterMapName[829] = "JA16VMS"; characterMap[829] = nullptr;
        characterMapBits[830] = 16; characterMapName[830] = "JA16EUC"; characterMap[830] = nullptr;
        characterMapBits[831] = 16; characterMapName[831] = "JA16EUCYEN"; characterMap[831] = nullptr;
        characterMapBits[832] = 16; characterMapName[832] = "JA16SJIS"; characterMap[832] = nullptr;
        characterMapBits[834] = 16; characterMapName[834] = "JA16SJISYEN"; characterMap[834] = nullptr;
        characterMapBits[837] = 16; characterMapName[837] = "JA16EUCTILDE"; characterMap[837] = nullptr;
        characterMapBits[838] = 16; characterMapName[838] = "JA16SJISTILDE"; characterMap[838] = nullptr;
        characterMapBits[840] = 16; characterMapName[840] = "KO16KSC5601"; characterMap[840] = nullptr;
        characterMapBits[845] = 16; characterMapName[845] = "KO16KSCCS"; characterMap[845] = nullptr;
        characterMapBits[846] = 16; characterMapName[846] = "KO16MSWIN949"; characterMap[846] = nullptr;
        characterMapBits[850] = 16; characterMapName[850] = "ZHS16CGB231280"; characterMap[850] = nullptr;
        characterMapBits[852] = 16; characterMapName[852] = "ZHS16GBK"; characterMap[852] = nullptr;
        characterMapBits[862] = 16; characterMapName[862] = "ZHT16DBT"; characterMap[862] = nullptr;
        characterMapBits[865] = 16; characterMapName[865] = "ZHT16BIG5"; characterMap[865] = nullptr;
        characterMapBits[867] = 16; characterMapName[867] = "ZHT16MSWIN950"; characterMap[867] = nullptr;
        characterMapBits[868] = 16; characterMapName[868] = "ZHT16HKSCS"; characterMap[868] = nullptr;
        characterMapBits[866] = 16; characterMapName[866] = "ZHT16CCDC"; characterMap[866] = nullptr;
        characterMapBits[992] = 16; characterMapName[992] = "ZHT16HKSCS31"; characterMap[992] = nullptr;
        characterMapBits[994] = 16; characterMapName[994] = "WE16DECTST2"; characterMap[994] = nullptr;
        characterMapBits[995] = 16; characterMapName[995] = "WE16DECTST"; characterMap[995] = nullptr;
        characterMapBits[996] = 16; characterMapName[996] = "KO16TSTSET"; characterMap[996] = nullptr;
        characterMapBits[997] = 16; characterMapName[997] = "JA16TSTSET2"; characterMap[997] = nullptr;

        //32-bit character sets - no translation
        characterMapBits[854] = 32; characterMapName[854] = "ZHS32GB18030"; characterMap[854] = nullptr;
        characterMapBits[860] = 32; characterMapName[860] = "ZHT32EUC"; characterMap[860] = nullptr;
        characterMapBits[863] = 32; characterMapName[863] = "ZHT32TRIS"; characterMap[863] = nullptr;

        if (intraThreadBuffer == nullptr)
            throw MemoryException("CommandBuffer::CommandBuffer", outputBufferSize);
    }

    CommandBuffer::~CommandBuffer() {
        if (intraThreadBuffer != nullptr) {
            delete[] intraThreadBuffer;
            intraThreadBuffer = nullptr;
        }
    }

    void CommandBuffer::stop(void) {
        shutdown = true;
    }

    void CommandBuffer::setOracleAnalyser(OracleAnalyser *oracleAnalyser) {
        this->oracleAnalyser = oracleAnalyser;
    }

    void CommandBuffer::setNlsCharset(string &nlsCharset, string &nlsNcharCharset) {
        cout << "- loading character mapping for " << nlsCharset << endl;

        if (strcmp(nlsCharset.c_str(), "AL32UTF8") == 0) defaultCharacterMapId = ORA_CHARSET_CODE_AL32UTF8;
        else if (strcmp(nlsCharset.c_str(), "UTF8") == 0) defaultCharacterMapId = ORA_CHARSET_CODE_UTF8;
        else
        for (auto elem: characterMapName) {
            if (strcmp(nlsCharset.c_str(), elem.second) == 0) {
                defaultCharacterMapId = elem.first;
                break;
            }
        }

        if (defaultCharacterMapId == 0)
            throw RuntimeException("unsupported NLS_CHARACTERSET value");

        cout << "- loading character mapping for " << nlsNcharCharset << endl;
        if (strcmp(nlsNcharCharset.c_str(), "AL16UTF16") == 0) defaultCharacterNcharMapId = ORA_CHARSET_CODE_AL16UTF16;
        else if (strcmp(nlsNcharCharset.c_str(), "UTF8") == 0) defaultCharacterNcharMapId = ORA_CHARSET_CODE_UTF8;
        else
            throw RuntimeException("unsupported NLS_NCHAR_CHARACTERSET value");
    }

    CommandBuffer* CommandBuffer::appendEscape(const uint8_t *str, uint64_t length) {
        if (shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length * 2 >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (1)" << endl;
                writerCond.wait(lck);
                if (shutdown)
                    return this;
            }
            if (shutdown)
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

    CommandBuffer* CommandBuffer::appendEscapeMap(const uint8_t *str, uint64_t length, uint64_t charsetId) {
        if (shutdown)
            return this;

        //reserve pessimistic 6 bytes per char
        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length * 6 >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (1)" << endl;
                writerCond.wait(lck);
                if (shutdown)
                    return this;
            }
            if (shutdown)
                return this;
        }

        if (posEndTmp + length * 6 >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (1)" << endl;
            return this;
        }

        while (length > 0) {
            bool noMapping = false;
            uint64_t unicodeCharacter;

            if (charsetId == ORA_CHARSET_CODE_UTF8 || charsetId == ORA_CHARSET_CODE_AL32UTF8) {
                unicodeCharacter = *str++;
                --length;
                noMapping = true;

            } else if (charsetId == ORA_CHARSET_CODE_AL16UTF16) {
                //U' = yyyy yyyy yyxx xxxx xxxx   // U - 0x10000
                //W1 = 1101 10yy yyyy yyyy      // 0xD800 + yyyyyyyyyy
                //W2 = 1101 11xx xxxx xxxx      // 0xDC00 + xxxxxxxxxx

                if (length <= 1)
                     throw RuntimeException("too short UTF-16 value");

                uint64_t character1 = (((uint64_t)(*str++)) << 8) | ((uint64_t)(*str++));
                length -= 2;

                if ((character1 & 0xFC00) == 0xDC00) {
                    cerr << "ERROR: found first lower UTF-16 character: " << dec << character1 << endl;
                    throw RuntimeException("unsupported UTF-16 value");
                } else if ((character1 & 0xFC00) == 0xD800) {
                    if (length <= 1)
                         throw RuntimeException("too short UTF-16 value");

                    uint64_t character2 = (((uint64_t)(*str++)) << 8) | ((uint64_t)(*str++));
                    length -= 2;

                    if ((character2 & 0xFC00) != 0xDC00) {
                        cerr << "ERROR: lower UTF-16 character in bad format: " << dec << character2 << endl;
                        throw RuntimeException("unsupported Unicode character map");
                    }

                    unicodeCharacter = 0x10000 + ((character1 & 0xFC00) >> 10) + (character2 & 0xFC00);
                } else {
                    unicodeCharacter = character1;
                }
            } else {
                uint64_t bits = characterMapBits[charsetId];

                if (bits == 7) {
                    typeunimap* mapCharset = characterMap[charsetId];
                    if (mapCharset == nullptr) {
                        cerr << "ERROR: can't find character set map for id = " << dec << charsetId << endl;
                        throw RuntimeException("unsupported Unicode character map");
                    }

                    uint64_t character1 = *str++;
                    --length;
                    unicodeCharacter = mapCharset[character1 & 0x7F];

                } else if (bits == 8) {
                    typeunimap* mapCharset = characterMap[charsetId];
                    if (mapCharset == nullptr) {
                        cerr << "ERROR: can't find character set map for id = " << dec << charsetId << endl;
                        throw RuntimeException("unsupported Unicode character map");
                    }

                    uint64_t character1 = *str++;
                    --length;
                    unicodeCharacter = mapCharset[character1];

                } else if (bits == 16 || bits == 32) {
                    uint64_t character1 = *str++;
                    --length;
                    unicodeCharacter = character1;
                    noMapping = true;

                } else {
                    cerr << "ERROR: character bit size not supported: " << dec << bits << endl;
                    throw RuntimeException("unsupported character bit width");
                }
            }

            if (unicodeCharacter == '\t') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 't';
            } else if (unicodeCharacter == '\r') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'r';
            } else if (unicodeCharacter == '\n') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'n';
            } else if (unicodeCharacter == '\f') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'f';
            } else if (unicodeCharacter == '\b') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'b';
            } else if (unicodeCharacter == '"') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = '"';
            } else if (unicodeCharacter == '\\') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = '\\';
            } else if (unicodeCharacter == '/') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = '/';
            } else {
                //0xxxxxxx
                if (unicodeCharacter <= 0x7F || noMapping) {
                    intraThreadBuffer[posEndTmp++] = (uint8_t)unicodeCharacter;

                //110xxxxx 10xxxxxx
                } else if (unicodeCharacter <= 0x7FF) {
                    intraThreadBuffer[posEndTmp++] = 0xC0 | (uint8_t)(unicodeCharacter >> 6);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)(unicodeCharacter & 0x3F);

                //1110xxxx 10xxxxxx 10xxxxxx
                } else if (unicodeCharacter <= 0xFFFF) {
                    intraThreadBuffer[posEndTmp++] = 0xE0 | (uint8_t)(unicodeCharacter >> 12);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)((unicodeCharacter >> 6) & 0x3F);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)(unicodeCharacter & 0x3F);

                //11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                } else if (unicodeCharacter <= 0x1FFFFF) {
                    intraThreadBuffer[posEndTmp++] = 0xF0 | (uint8_t)(unicodeCharacter >> 18);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)((unicodeCharacter >> 12) & 0x3F);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)((unicodeCharacter >> 6) & 0x3F);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)(unicodeCharacter & 0x3F);

                //111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
                } else if (unicodeCharacter <= 0x3FFFFFF) {
                    intraThreadBuffer[posEndTmp++] = 0xF8 | (uint8_t)(unicodeCharacter >> 24);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)((unicodeCharacter >> 18) & 0x3F);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)((unicodeCharacter >> 12) & 0x3F);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)((unicodeCharacter >> 6) & 0x3F);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)(unicodeCharacter & 0x3F);

                //1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
                } else if (unicodeCharacter <= 0x7FFFFFFF) {
                    intraThreadBuffer[posEndTmp++] = 0xFC | (uint8_t)(unicodeCharacter >> 32);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)((unicodeCharacter >> 24) & 0x3F);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)((unicodeCharacter >> 18) & 0x3F);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)((unicodeCharacter >> 12) & 0x3F);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)((unicodeCharacter >> 6) & 0x3F);
                    intraThreadBuffer[posEndTmp++] = 0x80 | (uint8_t)(unicodeCharacter & 0x3F);

                } else
                    throw RuntimeException("unsupported Unicode character");
            }
        }

        return this;
    }

    CommandBuffer* CommandBuffer::appendHex(uint64_t val, uint64_t length) {
        static const char* digits = "0123456789abcdef";
        if (shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (2)" << endl;
                writerCond.wait(lck);
                if (shutdown)
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
        if (shutdown)
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
                cerr << "WARNING, JSON buffer full, log reader suspended (3)" << endl;
                writerCond.wait(lck);
                if (shutdown)
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
            appendChr("\"scn\":\"0x");
            appendHex(scn, 16);
            append('"');
        } else {
            appendChr("\"scn\":");
            string scnStr = to_string(scn);
            appendStr(scnStr);
        }

        return this;
    }

    CommandBuffer* CommandBuffer::appendOperation(char *operation) {
        appendChr("\"operation\":\"");
        appendChr(operation);
        append('"');

        return this;
    }

    CommandBuffer* CommandBuffer::appendTable(string &owner, string &table) {
        appendChr("\"table\":\"");
        appendStr(owner);
        append('.');
        appendStr(table);
        append('"');

        return this;
    }

    CommandBuffer* CommandBuffer::appendNull(string &columnName) {
        append('"');
        appendStr(columnName);
        appendChr("\":null");

        return this;
    }

    CommandBuffer* CommandBuffer::appendMs(char *name, uint64_t time) {
        append('"');
        appendChr(name);
        appendChr("\":");
        appendDec(time);

        return this;
    }

    CommandBuffer* CommandBuffer::appendXid(typexid xid) {
        appendChr("\"xid\":\"");
        appendDec(USN(xid));
        append('.');
        appendDec(SLT(xid));
        append('.');
        appendDec(SQN(xid));
        append('"');

        return this;
    }

    CommandBuffer* CommandBuffer::appendTimestamp(const uint8_t *data, uint64_t length) {
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
                appendChr("BC");

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

        return this;
    }


    CommandBuffer* CommandBuffer::appendValue(string &columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t charsetId, uint64_t fieldPos, uint64_t fieldLength) {
        uint64_t j, jMax;
        uint8_t digits;

        if (redoLogRecord->length == 0) {
            cerr << "ERROR, trying to output null data for column: " << columnName << endl;
            return this;
        }

        append('"');
        appendStr(columnName);
        appendChr("\":");

        switch(typeNo) {
        case 1: //varchar(2)
        case 96: //char
            append('"');
            appendEscapeMap(redoLogRecord->data + fieldPos, fieldLength, charsetId);
            append('"');
            break;

        case 23: //raw
            append('"');
            for (uint64_t j = 0; j < fieldLength; ++j)
                appendHex(*(redoLogRecord->data + fieldPos + j), 2);
            append('"');
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
            } else {
                cerr << "ERROR: unknown value (table: " << redoLogRecord->object->owner << "." << redoLogRecord->object->objectName << " column: " << columnName << " type: " << dec << typeNo << "): " << dec << fieldLength << " - ";
                for (uint64_t j = 0; j < fieldLength; ++j)
                    cerr << " " << hex << setfill('0') << setw(2) << (uint64_t) redoLogRecord->data[fieldPos + j];
                cerr << endl;
            }
            break;

        case 12:  //date
        case 180: //timestamp
            if (fieldLength != 7 && fieldLength != 11) {
                cerr << "ERROR: unknown value (table: " << redoLogRecord->object->owner << "." << redoLogRecord->object->objectName << " column: " << columnName << " type: " << dec << typeNo << "): " << dec << fieldLength << " - ";
                for (uint64_t j = 0; j < fieldLength; ++j)
                    cerr << " " << hex << setfill('0') << setw(2) << (uint64_t)redoLogRecord->data[fieldPos + j];
                cerr << endl;
                appendChr("\"?\"");
            } else {
                append('"');
                appendTimestamp(redoLogRecord->data + fieldPos, fieldLength);
                append('"');
            }
            break;

        //case 231: //timestamp with local time zone
        case 181: //timestamp with time zone
            if (fieldLength != 13) {
                cerr << "ERROR: unknown value (table: " << redoLogRecord->object->owner << "." << redoLogRecord->object->objectName << " column: " << columnName << " type: " << dec << typeNo << "): " << dec << fieldLength << " - ";
                for (uint64_t j = 0; j < fieldLength; ++j)
                    cerr << " " << hex << setfill('0') << setw(2) << (uint64_t)redoLogRecord->data[fieldPos + j];
                cerr << endl;
                appendChr("\"?\"");
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
                        char *tz = oracleAnalyser->timeZoneMap[tzkey];
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
            if ((oracleAnalyser->trace2 & TRACE2_TYPES) != 0) {
                cerr << "TYPES: unknown value (table: " << redoLogRecord->object->owner << "." << redoLogRecord->object->objectName << " column: " << columnName << " type: " << dec << typeNo << "): " << dec << fieldLength << " - ";
                for (uint64_t j = 0; j < fieldLength; ++j)
                    cerr << " " << hex << setfill('0') << setw(2) << (uint64_t)redoLogRecord->data[fieldPos + j];
                cerr << endl;
            }
            appendChr("\"?\"");
        }

        return this;
    }

    CommandBuffer* CommandBuffer::appendStr(string &str) {
        if (shutdown)
            return this;

        uint64_t length = str.length();
        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (4)" << endl;
                writerCond.wait(lck);
                if (shutdown)
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

    CommandBuffer* CommandBuffer::appendChr(const char *str) {
        if (shutdown)
            return this;

        uint64_t length = strlen(str);
        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (5)" << endl;
                writerCond.wait(lck);
                if (shutdown)
                    return this;
            }
        }

        if (posEndTmp + length >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (2)" << endl;
            return this;
        }

        memcpy(intraThreadBuffer + posEndTmp, str, length);
        posEndTmp += length;

        return this;
    }

    char CommandBuffer::translationMap[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    CommandBuffer* CommandBuffer::appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot) {
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
        return this;
    }

    CommandBuffer* CommandBuffer::append(char chr) {
        if (shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + 1 >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (6)" << endl;
                writerCond.wait(lck);
                if (shutdown)
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

            if (object->columns[i] == nullptr)
                continue;

            if (i > 0)
                append(',');

            appendChr("{\"type\":\"");
            switch(object->columns[i]->typeNo) {
            case 1: //varchar(2)
            case 96: //char
                appendChr("string");
                break;

            case 2: //numeric
                if (object->columns[i]->scale > 0)
                    appendChr("Decimal");
                else {
                    uint64_t digits = object->columns[i]->precision - object->columns[i]->scale;
                    if (digits < 3)
                        appendChr("int8");
                    else if (digits < 5)
                        appendChr("int16");
                    else if (digits < 10)
                        appendChr("int32");
                    else if (digits < 19)
                        appendChr("int64");
                    else
                        appendChr("Decimal");
                }
                break;

            case 12:
            case 180:
                if (timestampFormat == 0 || timestampFormat == 1)
                    appendChr("datetime");
                else if (timestampFormat == 2) {
                    appendChr("int64");
                    microTimestamp = true;
                }
                break;
            }
            appendChr("\",\"optional\":");
            if (object->columns[i]->nullable)
                appendChr("true");
            else
                appendChr("false");

            if (microTimestamp)
                appendChr(",\"name\":\"io.debezium.time.MicroTimestamp\",\"version\":1");
            appendChr(",\"field\":\"");
            appendStr(object->columns[i]->columnName);
            appendChr("\"}");
        }
        return this;
    }

    CommandBuffer* CommandBuffer::appendDbzHead(OracleObject *object) {
        appendChr("{\"schema\":{\"type\":\"struct\",\"fields\":[");
        appendChr("{\"type\":\"struct\",\"fields\":[");
        appendDbzCols(object);
        appendChr("],\"optional\":true,\"name\":\"");
        appendStr(oracleAnalyser->alias);
        append('.');
        appendStr(object->owner);
        append('.');
        appendStr(object->objectName);
        appendChr(".Value\",\"field\":\"before\"},");
        appendChr("{\"type\":\"struct\",\"fields\":[");
        appendDbzCols(object);
        appendChr("],\"optional\":true,\"name\":\"");
        appendStr(oracleAnalyser->alias);
        append('.');
        appendStr(object->owner);
        append('.');
        appendStr(object->objectName);
        appendChr(".Value\",\"field\":\"after\"},"
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
        appendChr(",\"source\":{\"version\":\"" PROGRAM_VERSION "\",\"connector\":\"oracle\",\"name\":\"");
        appendStr(oracleAnalyser->alias);
        appendChr("\",");
        appendMs("ts_ms", time);
        appendChr(",\"snapshot\":\"false\",\"db\":\"");
        appendStr(oracleAnalyser->databaseContext);
        appendChr("\",\"schema\":\"");
        appendStr(object->owner);
        appendChr("\",\"table\":\"");
        appendStr(object->objectName);
        appendChr("\",\"txId\":\"");
        appendDec(USN(xid));
        append('.');
        appendDec(SLT(xid));
        append('.');
        appendDec(SQN(xid));
        appendChr("\",");
        appendScn(scn);
        appendChr(",\"lcr_position\":null},\"op\":\"");
        append(op);
        appendChr("\",");
        appendMs("ts_ms", time);
        appendChr(",\"transaction\":null,\"messagetopic\":\"");
        appendStr(oracleAnalyser->alias);
        append('.');
        appendStr(object->owner);
        append('.');
        appendStr(object->objectName);
        appendChr("\",\"messagesource\":\"OpenLogReplicator from Oracle on ");
        appendStr(oracleAnalyser->alias);
        appendChr("\"}}");
        return this;
    }

    CommandBuffer* CommandBuffer::beginTran(void) {
        if (shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + 8 >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (7)" << endl;
                writerCond.wait(lck);
                if (shutdown)
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

    CommandBuffer* CommandBuffer::commitTran(void) {
        if (posEndTmp == posEnd) {
            cerr << "WARNING: JSON buffer - commit of empty transaction" << endl;
            return this;
        }

        {
            unique_lock<mutex> lck(mtx);
            *((uint64_t*)(intraThreadBuffer + posEnd)) = posEndTmp - posEnd;
            posEndTmp = (posEndTmp + 7) & 0xFFFFFFFFFFFFFFF8;
            posEnd = posEndTmp;

            analysersCond.notify_all();
        }

        if (posEndTmp + 1 >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (8)" << endl;
            return this;
        }

        return this;
    }

    CommandBuffer* CommandBuffer::rewind(void) {
        if (shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 || posStart == 0) {
                cerr << "WARNING, JSON buffer full, log reader suspended (8)" << endl;
                writerCond.wait(lck);
                if (shutdown)
                    return this;
            }

            posSize = posEnd;
            posEnd = 0;
            posEndTmp = 0;
        }

        return this;
    }

    uint64_t CommandBuffer::currentTranSize(void) {
        return posEndTmp - posEnd;
    }
}
