/* Memory buffer for handling JSON data
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

#include <iostream>
#include <sstream>
#include <thread>
#include <string.h>

#include "CharacterSet16bit.h"
#include "CharacterSet7bit.h"
#include "CharacterSet8bit.h"
#include "CharacterSetAL16UTF16.h"
#include "CharacterSetAL32UTF8.h"
#include "CharacterSetJA16EUC.h"
#include "CharacterSetJA16EUCTILDE.h"
#include "CharacterSetJA16SJIS.h"
#include "CharacterSetJA16SJISTILDE.h"
#include "CharacterSetKO16KSCCS.h"
#include "CharacterSetUTF8.h"
#include "CharacterSetZHS16GBK.h"
#include "CharacterSetZHS32GB18030.h"
#include "CharacterSetZHT16HKSCS31.h"
#include "CharacterSetZHT32EUC.h"
#include "CharacterSetZHT32TRIS.h"
#include "CommandBuffer.h"
#include "MemoryException.h"
#include "OracleAnalyser.h"
#include "OracleColumn.h"
#include "OracleObject.h"
#include "RedoLogRecord.h"
#include "RuntimeException.h"

namespace OpenLogReplicator {

    CommandBuffer::CommandBuffer() :
            oracleAnalyser(nullptr),
            test(0),
            timestampFormat(0),
            charFormat(0),
            messageLength(0),
            defaultCharacterMapId(0),
            defaultCharacterNcharMapId(0),
            writer(nullptr),
            buffersAllocated(0),
            firstBufferPos(0),
            firstBuffer(nullptr),
            curBuffer(nullptr),
            curBufferPos(0),
            lastBuffer(nullptr),
            lastBufferPos(0) {

        characterMap[1] = new CharacterSet7bit("US7ASCII", CharacterSet7bit::unicode_map_US7ASCII);
        characterMap[2] = new CharacterSet8bit("WE8DEC", CharacterSet8bit::unicode_map_WE8DEC);
        characterMap[3] = new CharacterSet8bit("WE8HP", CharacterSet8bit::unicode_map_WE8HP, true);
        characterMap[4] = new CharacterSet8bit("US8PC437", CharacterSet8bit::unicode_map_US8PC437);
        characterMap[10] = new CharacterSet8bit("WE8PC850", CharacterSet8bit::unicode_map_WE8PC850);
        characterMap[11] = new CharacterSet7bit("D7DEC", CharacterSet7bit::unicode_map_D7DEC);
        characterMap[13] = new CharacterSet7bit("S7DEC", CharacterSet7bit::unicode_map_S7DEC);
        characterMap[14] = new CharacterSet7bit("E7DEC", CharacterSet7bit::unicode_map_E7DEC);
        characterMap[15] = new CharacterSet7bit("SF7ASCII", CharacterSet7bit::unicode_map_SF7ASCII);
        characterMap[16] = new CharacterSet7bit("NDK7DEC", CharacterSet7bit::unicode_map_NDK7DEC);
        characterMap[17] = new CharacterSet7bit("I7DEC", CharacterSet7bit::unicode_map_I7DEC);
        characterMap[21] = new CharacterSet7bit("SF7DEC", CharacterSet7bit::unicode_map_SF7DEC);
        characterMap[25] = new CharacterSet8bit("IN8ISCII", CharacterSet8bit::unicode_map_IN8ISCII);
        characterMap[28] = new CharacterSet8bit("WE8PC858", CharacterSet8bit::unicode_map_WE8PC858);
        characterMap[31] = new CharacterSet8bit("WE8ISO8859P1", CharacterSet8bit::unicode_map_WE8ISO8859P1);
        characterMap[32] = new CharacterSet8bit("EE8ISO8859P2", CharacterSet8bit::unicode_map_EE8ISO8859P2);
        characterMap[33] = new CharacterSet8bit("SE8ISO8859P3", CharacterSet8bit::unicode_map_SE8ISO8859P3);
        characterMap[34] = new CharacterSet8bit("NEE8ISO8859P4", CharacterSet8bit::unicode_map_NEE8ISO8859P4);
        characterMap[35] = new CharacterSet8bit("CL8ISO8859P5", CharacterSet8bit::unicode_map_CL8ISO8859P5);
        characterMap[36] = new CharacterSet8bit("AR8ISO8859P6", CharacterSet8bit::unicode_map_AR8ISO8859P6);
        characterMap[37] = new CharacterSet8bit("EL8ISO8859P7", CharacterSet8bit::unicode_map_EL8ISO8859P7);
        characterMap[38] = new CharacterSet8bit("IW8ISO8859P8", CharacterSet8bit::unicode_map_IW8ISO8859P8);
        characterMap[39] = new CharacterSet8bit("WE8ISO8859P9", CharacterSet8bit::unicode_map_WE8ISO8859P9);
        characterMap[40] = new CharacterSet8bit("NE8ISO8859P10", CharacterSet8bit::unicode_map_NE8ISO8859P10);
        characterMap[41] = new CharacterSet8bit("TH8TISASCII", CharacterSet8bit::unicode_map_TH8TISASCII);
        characterMap[43] = new CharacterSet8bit("BN8BSCII", CharacterSet8bit::unicode_map_BN8BSCII);
        characterMap[44] = new CharacterSet8bit("VN8VN3", CharacterSet8bit::unicode_map_VN8VN3);
        characterMap[45] = new CharacterSet8bit("VN8MSWIN1258", CharacterSet8bit::unicode_map_VN8MSWIN1258);
        characterMap[46] = new CharacterSet8bit("WE8ISO8859P15", CharacterSet8bit::unicode_map_WE8ISO8859P15);
        characterMap[47] = new CharacterSet8bit("BLT8ISO8859P13", CharacterSet8bit::unicode_map_BLT8ISO8859P13);
        characterMap[48] = new CharacterSet8bit("CEL8ISO8859P14", CharacterSet8bit::unicode_map_CEL8ISO8859P14);
        characterMap[49] = new CharacterSet8bit("CL8ISOIR111", CharacterSet8bit::unicode_map_CL8ISOIR111);
        characterMap[50] = new CharacterSet8bit("WE8NEXTSTEP", CharacterSet8bit::unicode_map_WE8NEXTSTEP);
        characterMap[51] = new CharacterSet8bit("CL8KOI8U", CharacterSet8bit::unicode_map_CL8KOI8U);
        characterMap[52] = new CharacterSet8bit("AZ8ISO8859P9E", CharacterSet8bit::unicode_map_AZ8ISO8859P9E);
        characterMap[61] = new CharacterSet8bit("AR8ASMO708PLUS", CharacterSet8bit::unicode_map_AR8ASMO708PLUS);
        characterMap[81] = new CharacterSet8bit("EL8DEC", CharacterSet8bit::unicode_map_EL8DEC);
        characterMap[82] = new CharacterSet8bit("TR8DEC", CharacterSet8bit::unicode_map_TR8DEC);
        characterMap[110] = new CharacterSet8bit("EEC8EUROASCI", CharacterSet8bit::unicode_map_EEC8EUROASCI, true);
        characterMap[113] = new CharacterSet8bit("EEC8EUROPA3", CharacterSet8bit::unicode_map_EEC8EUROPA3, true);
        characterMap[114] = new CharacterSet8bit("LA8PASSPORT", CharacterSet8bit::unicode_map_LA8PASSPORT);
        characterMap[140] = new CharacterSet8bit("BG8PC437S", CharacterSet8bit::unicode_map_BG8PC437S);
        characterMap[150] = new CharacterSet8bit("EE8PC852", CharacterSet8bit::unicode_map_EE8PC852);
        characterMap[152] = new CharacterSet8bit("RU8PC866", CharacterSet8bit::unicode_map_RU8PC866);
        characterMap[153] = new CharacterSet8bit("RU8BESTA", CharacterSet8bit::unicode_map_RU8BESTA);
        characterMap[154] = new CharacterSet8bit("IW8PC1507", CharacterSet8bit::unicode_map_IW8PC1507);
        characterMap[155] = new CharacterSet8bit("RU8PC855", CharacterSet8bit::unicode_map_RU8PC855);
        characterMap[156] = new CharacterSet8bit("TR8PC857", CharacterSet8bit::unicode_map_TR8PC857);
        characterMap[159] = new CharacterSet8bit("CL8MACCYRILLICS", CharacterSet8bit::unicode_map_CL8MACCYRILLICS);
        characterMap[160] = new CharacterSet8bit("WE8PC860", CharacterSet8bit::unicode_map_WE8PC860);
        characterMap[161] = new CharacterSet8bit("IS8PC861", CharacterSet8bit::unicode_map_IS8PC861);
        characterMap[162] = new CharacterSet8bit("EE8MACCES", CharacterSet8bit::unicode_map_EE8MACCES);
        characterMap[163] = new CharacterSet8bit("EE8MACCROATIANS", CharacterSet8bit::unicode_map_EE8MACCROATIANS);
        characterMap[164] = new CharacterSet8bit("TR8MACTURKISHS", CharacterSet8bit::unicode_map_TR8MACTURKISHS);
        characterMap[165] = new CharacterSet8bit("IS8MACICELANDICS", CharacterSet8bit::unicode_map_IS8MACICELANDICS, true);
        characterMap[166] = new CharacterSet8bit("EL8MACGREEKS", CharacterSet8bit::unicode_map_EL8MACGREEKS);
        characterMap[167] = new CharacterSet8bit("IW8MACHEBREWS", CharacterSet8bit::unicode_map_IW8MACHEBREWS);
        characterMap[170] = new CharacterSet8bit("EE8MSWIN1250", CharacterSet8bit::unicode_map_EE8MSWIN1250);
        characterMap[171] = new CharacterSet8bit("CL8MSWIN1251", CharacterSet8bit::unicode_map_CL8MSWIN1251);
        characterMap[172] = new CharacterSet8bit("ET8MSWIN923", CharacterSet8bit::unicode_map_ET8MSWIN923);
        characterMap[173] = new CharacterSet8bit("BG8MSWIN", CharacterSet8bit::unicode_map_BG8MSWIN);
        characterMap[174] = new CharacterSet8bit("EL8MSWIN1253", CharacterSet8bit::unicode_map_EL8MSWIN1253);
        characterMap[175] = new CharacterSet8bit("IW8MSWIN1255", CharacterSet8bit::unicode_map_IW8MSWIN1255);
        characterMap[176] = new CharacterSet8bit("LT8MSWIN921", CharacterSet8bit::unicode_map_LT8MSWIN921);
        characterMap[177] = new CharacterSet8bit("TR8MSWIN1254", CharacterSet8bit::unicode_map_TR8MSWIN1254);
        characterMap[178] = new CharacterSet8bit("WE8MSWIN1252", CharacterSet8bit::unicode_map_WE8MSWIN1252);
        characterMap[179] = new CharacterSet8bit("BLT8MSWIN1257", CharacterSet8bit::unicode_map_BLT8MSWIN1257);
        characterMap[190] = new CharacterSet8bit("N8PC865", CharacterSet8bit::unicode_map_N8PC865);
        characterMap[191] = new CharacterSet8bit("BLT8CP921", CharacterSet8bit::unicode_map_BLT8CP921);
        characterMap[192] = new CharacterSet8bit("LV8PC1117", CharacterSet8bit::unicode_map_LV8PC1117);
        characterMap[193] = new CharacterSet8bit("LV8PC8LR", CharacterSet8bit::unicode_map_LV8PC8LR);
        characterMap[195] = new CharacterSet8bit("LV8RST104090", CharacterSet8bit::unicode_map_LV8RST104090);
        characterMap[196] = new CharacterSet8bit("CL8KOI8R", CharacterSet8bit::unicode_map_CL8KOI8R);
        characterMap[197] = new CharacterSet8bit("BLT8PC775", CharacterSet8bit::unicode_map_BLT8PC775);
        characterMap[202] = new CharacterSet7bit("E7SIEMENS9780X", CharacterSet7bit::unicode_map_E7SIEMENS9780X);
        characterMap[203] = new CharacterSet7bit("S7SIEMENS9780X", CharacterSet7bit::unicode_map_S7SIEMENS9780X);
        characterMap[204] = new CharacterSet7bit("DK7SIEMENS9780X", CharacterSet7bit::unicode_map_DK7SIEMENS9780X);
        characterMap[206] = new CharacterSet7bit("I7SIEMENS9780X", CharacterSet7bit::unicode_map_I7SIEMENS9780X);
        characterMap[205] = new CharacterSet7bit("N7SIEMENS9780X", CharacterSet7bit::unicode_map_N7SIEMENS9780X);
        characterMap[207] = new CharacterSet7bit("D7SIEMENS9780X", CharacterSet7bit::unicode_map_D7SIEMENS9780X);
        characterMap[241] = new CharacterSet8bit("WE8DG", CharacterSet8bit::unicode_map_WE8DG);
        characterMap[251] = new CharacterSet8bit("WE8NCR4970", CharacterSet8bit::unicode_map_WE8NCR4970);
        characterMap[261] = new CharacterSet8bit("WE8ROMAN8", CharacterSet8bit::unicode_map_WE8ROMAN8);
        characterMap[352] = new CharacterSet8bit("WE8MACROMAN8S", CharacterSet8bit::unicode_map_WE8MACROMAN8S);
        characterMap[354] = new CharacterSet8bit("TH8MACTHAIS", CharacterSet8bit::unicode_map_TH8MACTHAIS);
        characterMap[368] = new CharacterSet8bit("HU8CWI2", CharacterSet8bit::unicode_map_HU8CWI2);
        characterMap[380] = new CharacterSet8bit("EL8PC437S", CharacterSet8bit::unicode_map_EL8PC437S);
        characterMap[382] = new CharacterSet8bit("EL8PC737", CharacterSet8bit::unicode_map_EL8PC737);
        characterMap[383] = new CharacterSet8bit("LT8PC772", CharacterSet8bit::unicode_map_LT8PC772);
        characterMap[384] = new CharacterSet8bit("LT8PC774", CharacterSet8bit::unicode_map_LT8PC774);
        characterMap[385] = new CharacterSet8bit("EL8PC869", CharacterSet8bit::unicode_map_EL8PC869);
        characterMap[386] = new CharacterSet8bit("EL8PC851", CharacterSet8bit::unicode_map_EL8PC851);
        characterMap[390] = new CharacterSet8bit("CDN8PC863", CharacterSet8bit::unicode_map_CDN8PC863);
        characterMap[401] = new CharacterSet8bit("HU8ABMOD", CharacterSet8bit::unicode_map_HU8ABMOD);
        characterMap[500] = new CharacterSet8bit("AR8ASMO8X", CharacterSet8bit::unicode_map_AR8ASMO8X);
        characterMap[504] = new CharacterSet8bit("AR8NAFITHA711T", CharacterSet8bit::unicode_map_AR8NAFITHA711T);
        characterMap[505] = new CharacterSet8bit("AR8SAKHR707T", CharacterSet8bit::unicode_map_AR8SAKHR707T);
        characterMap[506] = new CharacterSet8bit("AR8MUSSAD768T", CharacterSet8bit::unicode_map_AR8MUSSAD768T);
        characterMap[507] = new CharacterSet8bit("AR8ADOS710T", CharacterSet8bit::unicode_map_AR8ADOS710T);
        characterMap[508] = new CharacterSet8bit("AR8ADOS720T", CharacterSet8bit::unicode_map_AR8ADOS720T);
        characterMap[509] = new CharacterSet8bit("AR8APTEC715T", CharacterSet8bit::unicode_map_AR8APTEC715T);
        characterMap[511] = new CharacterSet8bit("AR8NAFITHA721T", CharacterSet8bit::unicode_map_AR8NAFITHA721T);
        characterMap[514] = new CharacterSet8bit("AR8HPARABIC8T", CharacterSet8bit::unicode_map_AR8HPARABIC8T);
        characterMap[554] = new CharacterSet8bit("AR8NAFITHA711", CharacterSet8bit::unicode_map_AR8NAFITHA711);
        characterMap[555] = new CharacterSet8bit("AR8SAKHR707", CharacterSet8bit::unicode_map_AR8SAKHR707);
        characterMap[556] = new CharacterSet8bit("AR8MUSSAD768", CharacterSet8bit::unicode_map_AR8MUSSAD768);
        characterMap[557] = new CharacterSet8bit("AR8ADOS710", CharacterSet8bit::unicode_map_AR8ADOS710);
        characterMap[558] = new CharacterSet8bit("AR8ADOS720", CharacterSet8bit::unicode_map_AR8ADOS720);
        characterMap[559] = new CharacterSet8bit("AR8APTEC715", CharacterSet8bit::unicode_map_AR8APTEC715);
        characterMap[560] = new CharacterSet8bit("AR8MSWIN1256", CharacterSet8bit::unicode_map_AR8MSWIN1256);
        characterMap[561] = new CharacterSet8bit("AR8NAFITHA721", CharacterSet8bit::unicode_map_AR8NAFITHA721);
        characterMap[563] = new CharacterSet8bit("AR8SAKHR706", CharacterSet8bit::unicode_map_AR8SAKHR706);
        characterMap[566] = new CharacterSet8bit("AR8ARABICMACS", CharacterSet8bit::unicode_map_AR8ARABICMACS);
        characterMap[590] = new CharacterSet8bit("LA8ISO6937", CharacterSet8bit::unicode_map_LA8ISO6937);
        characterMap[829] = new CharacterSet16bit("JA16VMS", CharacterSet16bit::unicode_map_JA16VMS, JA16VMS_b1_min, JA16VMS_b1_max, JA16VMS_b2_min, JA16VMS_b2_max);
        characterMap[830] = new CharacterSetJA16EUC();
        characterMap[831] = new CharacterSetJA16EUC("JA16EUCYEN");
        characterMap[832] = new CharacterSetJA16SJIS();
        characterMap[834] = new CharacterSetJA16SJIS("JA16SJISYEN");
        characterMap[837] = new CharacterSetJA16EUCTILDE();
        characterMap[838] = new CharacterSetJA16SJISTILDE();
        characterMap[840] = new CharacterSet16bit("KO16KSC5601", CharacterSet16bit::unicode_map_KO16KSC5601_2b, KO16KSC5601_b1_min, KO16KSC5601_b1_max, KO16KSC5601_b2_min, KO16KSC5601_b2_max);
        characterMap[845] = new CharacterSetKO16KSCCS();
        characterMap[846] = new CharacterSet16bit("KO16MSWIN949", CharacterSet16bit::unicode_map_KO16MSWIN949_2b, KO16MSWIN949_b1_min, KO16MSWIN949_b1_max, KO16MSWIN949_b2_min, KO16MSWIN949_b2_max);
        characterMap[850] = new CharacterSet16bit("ZHS16CGB231280", CharacterSet16bit::unicode_map_ZHS16CGB231280_2b, ZHS16CGB231280_b1_min, ZHS16CGB231280_b1_max, ZHS16CGB231280_b2_min, ZHS16CGB231280_b2_max);
        characterMap[852] = new CharacterSetZHS16GBK();
        characterMap[854] = new CharacterSetZHS32GB18030();
        characterMap[860] = new CharacterSetZHT32EUC();
        characterMap[863] = new CharacterSetZHT32TRIS();
        characterMap[865] = new CharacterSet16bit("ZHT16BIG5", CharacterSet16bit::unicode_map_ZHT16BIG5_2b, ZHT16BIG5_b1_min, ZHT16BIG5_b1_max, ZHT16BIG5_b2_min, ZHT16BIG5_b2_max);
        characterMap[866] = new CharacterSet16bit("ZHT16CCDC", CharacterSet16bit::unicode_map_ZHT16CCDC_2b, ZHT16CCDC_b1_min, ZHT16CCDC_b1_max, ZHT16CCDC_b2_min, ZHT16CCDC_b2_max);
        characterMap[867] = new CharacterSet16bit("ZHT16MSWIN950", CharacterSet16bit::unicode_map_ZHT16MSWIN950_2b, ZHT16MSWIN950_b1_min, ZHT16MSWIN950_b1_max, ZHT16MSWIN950_b2_min, ZHT16MSWIN950_b2_max);
        characterMap[868] = new CharacterSet16bit("ZHT16HKSCS", CharacterSet16bit::unicode_map_ZHT16HKSCS_2b, ZHT16HKSCS_b1_min, ZHT16HKSCS_b1_max, ZHT16HKSCS_b2_min, ZHT16HKSCS_b2_max);
        characterMap[871] = new CharacterSetUTF8();
        characterMap[873] = new CharacterSetAL32UTF8();
        characterMap[992] = new CharacterSetZHT16HKSCS31();
        characterMap[1002] = new CharacterSet8bit("TIMESTEN8", CharacterSet8bit::unicode_map_TIMESTEN8);
        characterMap[2000] = new CharacterSetAL16UTF16();
    }

    CommandBuffer::~CommandBuffer() {
        while (firstBuffer != nullptr) {
            uint8_t* nextBuffer = *((uint8_t**)(firstBuffer + KAFKA_BUFFER_NEXT));
            oracleAnalyser->freeMemoryChunk("KAFKA", firstBuffer, true);
            firstBuffer = nextBuffer;
            --buffersAllocated;
        }
    }

    void CommandBuffer::initialize(OracleAnalyser *oracleAnalyser) {
        this->oracleAnalyser = oracleAnalyser;

        buffersAllocated = 1;
        firstBuffer = oracleAnalyser->getMemoryChunk("KAFKA", false);
        *((uint8_t**)(firstBuffer + KAFKA_BUFFER_NEXT)) = nullptr;
        *((uint64_t*)(firstBuffer + KAFKA_BUFFER_END)) = KAFKA_BUFFER_DATA;
        firstBufferPos = KAFKA_BUFFER_DATA;
        lastBuffer = firstBuffer;
        lastBufferPos = KAFKA_BUFFER_DATA;
    }

    void CommandBuffer::bufferAppend(uint8_t character) {
        lastBuffer[lastBufferPos] = character;
        ++messageLength;
        bufferShift(1);
    }

    void CommandBuffer::bufferShift(uint64_t bytes) {
        lastBufferPos += bytes;

        if (lastBufferPos >= MEMORY_CHUNK_SIZE) {
            uint8_t *nextBuffer = oracleAnalyser->getMemoryChunk("KAFKA", true);
            *((uint8_t**)(nextBuffer + KAFKA_BUFFER_NEXT)) = nullptr;
            *((uint64_t*)(nextBuffer + KAFKA_BUFFER_END)) = KAFKA_BUFFER_DATA;
            {
                unique_lock<mutex> lck(mtx);
                *((uint8_t**)(lastBuffer + KAFKA_BUFFER_NEXT)) = nextBuffer;
                *((uint64_t*)(lastBuffer + KAFKA_BUFFER_END)) = MEMORY_CHUNK_SIZE;
                ++buffersAllocated;
                lastBuffer = nextBuffer;
                lastBufferPos = KAFKA_BUFFER_DATA;
            }
        }
    }

    CommandBuffer* CommandBuffer::beginMessage(void) {
        curBuffer = lastBuffer;
        curBufferPos = lastBufferPos;
        messageLength = 0;
        *((uint64_t*)(lastBuffer + lastBufferPos)) = 0;
        bufferShift(KAFKA_BUFFER_LENGTH_SIZE);

        return this;
    }

    CommandBuffer* CommandBuffer::commitMessage(void) {
        if (messageLength == 0)
            cerr << "WARNING: JSON buffer - commit of empty transaction" << endl;

        bufferShift((8 - (messageLength & 7)) & 7);
        {
            unique_lock<mutex> lck(mtx);
            *((uint64_t*)(curBuffer + curBufferPos)) = messageLength;
            if (curBuffer != lastBuffer)
                *((uint64_t*)(curBuffer + KAFKA_BUFFER_END)) = MEMORY_CHUNK_SIZE;
            *((uint64_t*)(lastBuffer + KAFKA_BUFFER_END)) = lastBufferPos;
            writersCond.notify_all();
        }
        return this;
    }

    uint64_t CommandBuffer::currentMessageSize(void) {
        return messageLength + KAFKA_BUFFER_LENGTH_SIZE;
    }

    void CommandBuffer::setParameters(uint64_t test, uint64_t timestampFormat, uint64_t charFormat, Writer *writer) {
        this->test = test;
        this->timestampFormat = timestampFormat;
        this->charFormat = charFormat;
        this->writer = writer;
    }

    void CommandBuffer::setNlsCharset(string &nlsCharset, string &nlsNcharCharset) {
        cerr << "- loading character mapping for " << nlsCharset << endl;

        for (auto elem: characterMap) {
            if (strcmp(nlsCharset.c_str(), elem.second->name) == 0) {
                defaultCharacterMapId = elem.first;
                break;
            }
        }

        if (defaultCharacterMapId == 0)
            throw RuntimeException("unsupported NLS_CHARACTERSET value");

        cerr << "- loading character mapping for " << nlsNcharCharset << endl;
        for (auto elem: characterMap) {
            if (strcmp(nlsNcharCharset.c_str(), elem.second->name) == 0) {
                defaultCharacterNcharMapId = elem.first;
                break;
            }
        }

        if (defaultCharacterNcharMapId == 0)
            throw RuntimeException("unsupported NLS_NCHAR_CHARACTERSET value");
    }

    CommandBuffer* CommandBuffer::appendEscape(const uint8_t *str, uint64_t length) {
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

        return this;
    }

CommandBuffer* CommandBuffer::appendEscapeMap(const uint8_t *str, uint64_t length, uint64_t charsetId) {
        bool isNext = false;

        CharacterSet *characterSet = characterMap[charsetId];
        if (characterSet == nullptr && (charFormat & 1 == 0)) {
            cerr << "ERROR: can't find character set map for id = " << dec << charsetId << endl;
            throw RuntimeException("unsupported character map");
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
                    cerr << "ERROR: got character code: U+" << dec << unicodeCharacter << endl;
                    throw RuntimeException("unsupported Unicode character");
                }
            }
        }

        return this;
    }

    CommandBuffer* CommandBuffer::appendHex(uint64_t val, uint64_t length) {
        static const char* digits = "0123456789abcdef";

        uint64_t j = (length - 1) * 4;
        for (uint64_t i = 0; i < length; ++i) {
            bufferAppend(digits[(val >> j) & 0xF]);
            j -= 4;
        };

        return this;
    }

    CommandBuffer* CommandBuffer::appendDec(uint64_t val) {
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

        for (uint64_t i = 0; i < length; ++i)
            bufferAppend(buffer[length - i - 1]);

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

    CommandBuffer* CommandBuffer::appendUnknown(string &columnName, RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t fieldPos, uint64_t fieldLength) {
        appendChr("\"?\"");
        cerr << "ERROR: unknown value (table: " << redoLogRecord->object->owner << "." << redoLogRecord->object->objectName << " column: " << columnName << " type: " << dec << typeNo << "): " << dec << fieldLength << " - ";
        for (uint64_t j = 0; j < fieldLength; ++j)
            cerr << " " << hex << setfill('0') << setw(2) << (uint64_t) redoLogRecord->data[fieldPos + j];
        cerr << endl;
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
            if ((oracleAnalyser->trace2 & TRACE2_TYPES) != 0)
                appendUnknown(columnName, redoLogRecord, typeNo, fieldPos, fieldLength);
            else
                appendChr("\"?\"");
        }

        return this;
    }

    CommandBuffer* CommandBuffer::appendStr(string &str) {
        const char *charstr = str.c_str();
        uint64_t length = str.length();
        for (uint i = 0; i < length; ++i)
            bufferAppend(*charstr++);
        return this;
    }

    CommandBuffer* CommandBuffer::appendChr(const char *str) {
        char character = *str++;
        while (character != 0) {
            bufferAppend(character);
            character = *str++;
        }
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
        bufferAppend(chr);
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
}
