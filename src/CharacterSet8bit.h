/* Header for CharacterSet8bit class
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

#include "CharacterSet7bit.h"

#ifndef CHARACTERSET8BIT_H_
#define CHARACTERSET8BIT_H_

using namespace std;

namespace OpenLogReplicator {

    class CharacterSet8bit : public CharacterSet7bit {
    protected:
        virtual typeunicode readMap(uint64_t character) const;
        bool customASCII;

    public:
        CharacterSet8bit(const char *name, const typeunicode16 *map);
        CharacterSet8bit(const char *name, const typeunicode16 *map, bool customASCII);
        virtual ~CharacterSet8bit();

        virtual typeunicode decode(const uint8_t* &str, uint64_t &length) const;

        static typeunicode16 unicode_map_AR8ADOS710[128];
        static typeunicode16 unicode_map_AR8ADOS710T[128];
        static typeunicode16 unicode_map_AR8ADOS720[128];
        static typeunicode16 unicode_map_AR8ADOS720T[128];
        static typeunicode16 unicode_map_AR8APTEC715[128];
        static typeunicode16 unicode_map_AR8APTEC715T[128];
        static typeunicode16 unicode_map_AR8ARABICMACS[128];
        static typeunicode16 unicode_map_AR8ASMO708PLUS[128];
        static typeunicode16 unicode_map_AR8ASMO8X[128];
        static typeunicode16 unicode_map_AR8HPARABIC8T[128];
        static typeunicode16 unicode_map_AR8ISO8859P6[128];
        static typeunicode16 unicode_map_AR8MSWIN1256[128];
        static typeunicode16 unicode_map_AR8MUSSAD768[128];
        static typeunicode16 unicode_map_AR8MUSSAD768T[128];
        static typeunicode16 unicode_map_AR8NAFITHA711[128];
        static typeunicode16 unicode_map_AR8NAFITHA711T[128];
        static typeunicode16 unicode_map_AR8NAFITHA721[128];
        static typeunicode16 unicode_map_AR8NAFITHA721T[128];
        static typeunicode16 unicode_map_AR8SAKHR706[128];
        static typeunicode16 unicode_map_AR8SAKHR707[128];
        static typeunicode16 unicode_map_AR8SAKHR707T[128];
        static typeunicode16 unicode_map_AZ8ISO8859P9E[128];
        static typeunicode16 unicode_map_BG8MSWIN[128];
        static typeunicode16 unicode_map_BG8PC437S[128];
        static typeunicode16 unicode_map_BLT8CP921[128];
        static typeunicode16 unicode_map_BLT8ISO8859P13[128];
        static typeunicode16 unicode_map_BLT8MSWIN1257[128];
        static typeunicode16 unicode_map_BLT8PC775[128];
        static typeunicode16 unicode_map_BN8BSCII[128];
        static typeunicode16 unicode_map_CDN8PC863[128];
        static typeunicode16 unicode_map_CEL8ISO8859P14[128];
        static typeunicode16 unicode_map_CL8ISO8859P5[128];
        static typeunicode16 unicode_map_CL8ISOIR111[128];
        static typeunicode16 unicode_map_CL8KOI8R[128];
        static typeunicode16 unicode_map_CL8KOI8U[128];
        static typeunicode16 unicode_map_CL8MACCYRILLICS[128];
        static typeunicode16 unicode_map_CL8MSWIN1251[128];
        static typeunicode16 unicode_map_EE8ISO8859P2[128];
        static typeunicode16 unicode_map_EE8MACCES[128];
        static typeunicode16 unicode_map_EE8MACCROATIANS[128];
        static typeunicode16 unicode_map_EE8MSWIN1250[128];
        static typeunicode16 unicode_map_EE8PC852[128];
        static typeunicode16 unicode_map_EEC8EUROASCI[256];
        static typeunicode16 unicode_map_EEC8EUROPA3[256];
        static typeunicode16 unicode_map_EL8DEC[128];
        static typeunicode16 unicode_map_EL8ISO8859P7[128];
        static typeunicode16 unicode_map_EL8MACGREEKS[128];
        static typeunicode16 unicode_map_EL8MSWIN1253[128];
        static typeunicode16 unicode_map_EL8PC437S[128];
        static typeunicode16 unicode_map_EL8PC737[128];
        static typeunicode16 unicode_map_EL8PC851[128];
        static typeunicode16 unicode_map_EL8PC869[128];
        static typeunicode16 unicode_map_ET8MSWIN923[128];
        static typeunicode16 unicode_map_HU8ABMOD[128];
        static typeunicode16 unicode_map_HU8CWI2[128];
        static typeunicode16 unicode_map_IN8ISCII[128];
        static typeunicode16 unicode_map_IS8MACICELANDICS[256];
        static typeunicode16 unicode_map_IS8PC861[128];
        static typeunicode16 unicode_map_IW8ISO8859P8[128];
        static typeunicode16 unicode_map_IW8MACHEBREWS[128];
        static typeunicode16 unicode_map_IW8MSWIN1255[128];
        static typeunicode16 unicode_map_IW8PC1507[128];
        static typeunicode16 unicode_map_LA8ISO6937[128];
        static typeunicode16 unicode_map_LA8PASSPORT[128];
        static typeunicode16 unicode_map_LT8MSWIN921[128];
        static typeunicode16 unicode_map_LT8PC772[128];
        static typeunicode16 unicode_map_LT8PC774[128];
        static typeunicode16 unicode_map_LV8PC1117[128];
        static typeunicode16 unicode_map_LV8PC8LR[128];
        static typeunicode16 unicode_map_LV8RST104090[128];
        static typeunicode16 unicode_map_N8PC865[128];
        static typeunicode16 unicode_map_NE8ISO8859P10[128];
        static typeunicode16 unicode_map_NEE8ISO8859P4[128];
        static typeunicode16 unicode_map_RU8BESTA[128];
        static typeunicode16 unicode_map_RU8PC855[128];
        static typeunicode16 unicode_map_RU8PC866[128];
        static typeunicode16 unicode_map_SE8ISO8859P3[128];
        static typeunicode16 unicode_map_TH8MACTHAIS[128];
        static typeunicode16 unicode_map_TH8TISASCII[128];
        static typeunicode16 unicode_map_TIMESTEN8[128];
        static typeunicode16 unicode_map_TR8DEC[128];
        static typeunicode16 unicode_map_TR8MACTURKISHS[128];
        static typeunicode16 unicode_map_TR8MSWIN1254[128];
        static typeunicode16 unicode_map_TR8PC857[128];
        static typeunicode16 unicode_map_US8PC437[128];
        static typeunicode16 unicode_map_VN8MSWIN1258[128];
        static typeunicode16 unicode_map_VN8VN3[128];
        static typeunicode16 unicode_map_WE8DEC[128];
        static typeunicode16 unicode_map_WE8DG[128];
        static typeunicode16 unicode_map_WE8HP[256];
        static typeunicode16 unicode_map_WE8ISO8859P1[128];
        static typeunicode16 unicode_map_WE8ISO8859P15[128];
        static typeunicode16 unicode_map_WE8ISO8859P9[128];
        static typeunicode16 unicode_map_WE8MACROMAN8S[128];
        static typeunicode16 unicode_map_WE8MSWIN1252[128];
        static typeunicode16 unicode_map_WE8NCR4970[128];
        static typeunicode16 unicode_map_WE8NEXTSTEP[128];
        static typeunicode16 unicode_map_WE8PC850[128];
        static typeunicode16 unicode_map_WE8PC858[128];
        static typeunicode16 unicode_map_WE8PC860[128];
        static typeunicode16 unicode_map_WE8ROMAN8[128];
    };
}

#endif
