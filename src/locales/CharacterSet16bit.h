/* Header for CharacterSet16bit class
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

#include "CharacterSet.h"

#ifndef CHARACTER_SET_16BIT_H_
#define CHARACTER_SET_16BIT_H_

namespace OpenLogReplicator {
    class CharacterSet16bit : public CharacterSet {
    public:
        static constexpr uint64_t JA16VMS_b1_min = 0xA1;
        static constexpr uint64_t JA16VMS_b1_max = 0xF4;
        static constexpr uint64_t JA16VMS_b2_min = 0xA1;
        static constexpr uint64_t JA16VMS_b2_max = 0xFE;

        static constexpr uint64_t KO16KSC5601_b1_min = 0xA1;
        static constexpr uint64_t KO16KSC5601_b1_max = 0xFD;
        static constexpr uint64_t KO16KSC5601_b2_min = 0xA1;
        static constexpr uint64_t KO16KSC5601_b2_max = 0xFE;

        static constexpr uint64_t KO16MSWIN949_b1_min = 0x81;
        static constexpr uint64_t KO16MSWIN949_b1_max = 0xFE;
        static constexpr uint64_t KO16MSWIN949_b2_min = 0x41;
        static constexpr uint64_t KO16MSWIN949_b2_max = 0xFE;

        static constexpr uint64_t ZHS16CGB231280_b1_min = 0xA1;
        static constexpr uint64_t ZHS16CGB231280_b1_max = 0xF7;
        static constexpr uint64_t ZHS16CGB231280_b2_min = 0xA1;
        static constexpr uint64_t ZHS16CGB231280_b2_max = 0xFE;

        static constexpr uint64_t ZHT16BIG5_b1_min = 0xA1;
        static constexpr uint64_t ZHT16BIG5_b1_max = 0xF9;
        static constexpr uint64_t ZHT16BIG5_b2_min = 0x40;
        static constexpr uint64_t ZHT16BIG5_b2_max = 0xFE;

        static constexpr uint64_t ZHT16CCDC_b1_min = 0xA1;
        static constexpr uint64_t ZHT16CCDC_b1_max = 0xFB;
        static constexpr uint64_t ZHT16CCDC_b2_min = 0x21;
        static constexpr uint64_t ZHT16CCDC_b2_max = 0xFE;

        static constexpr uint64_t ZHT16MSWIN950_b1_min = 0x81;
        static constexpr uint64_t ZHT16MSWIN950_b1_max = 0xFE;
        static constexpr uint64_t ZHT16MSWIN950_b2_min = 0x40;
        static constexpr uint64_t ZHT16MSWIN950_b2_max = 0xFE;

        static constexpr uint64_t ZHT16HKSCS_b1_min = 0x81;
        static constexpr uint64_t ZHT16HKSCS_b1_max = 0xFE;
        static constexpr uint64_t ZHT16HKSCS_b2_min = 0x40;
        static constexpr uint64_t ZHT16HKSCS_b2_max = 0xFE;

    protected:
        const typeUnicode16* map;
        uint64_t byte1min;
        uint64_t byte1max;
        uint64_t byte2min;
        uint64_t byte2max;
        [[nodiscard]] virtual typeUnicode readMap(uint64_t byte1, uint64_t byte2) const;

    public:
        CharacterSet16bit(const char* newName, const typeUnicode16* newMap, uint64_t newByte1min, uint64_t newByte1max, uint64_t newByte2min,
                          uint64_t newByte2max);
        virtual ~CharacterSet16bit() override;

        virtual typeUnicode decode(const Ctx* ctx, typeXid xid, const uint8_t*& str, uint64_t& length) const override;

        static typeUnicode16 unicode_map_JA16VMS[(JA16VMS_b1_max - JA16VMS_b1_min + 1) *
                                                 (JA16VMS_b2_max - JA16VMS_b2_min + 1)];
        static typeUnicode16 unicode_map_KO16KSC5601_2b[(KO16KSC5601_b1_max - KO16KSC5601_b1_min + 1) *
                                                        (KO16KSC5601_b2_max - KO16KSC5601_b2_min + 1)];
        static typeUnicode16 unicode_map_KO16MSWIN949_2b[(KO16MSWIN949_b1_max - KO16MSWIN949_b1_min + 1) *
                                                         (KO16MSWIN949_b2_max - KO16MSWIN949_b2_min + 1)];
        static typeUnicode16 unicode_map_ZHS16CGB231280_2b[(ZHS16CGB231280_b1_max - ZHS16CGB231280_b1_min + 1) *
                                                           (ZHS16CGB231280_b2_max - ZHS16CGB231280_b2_min + 1)];
        static typeUnicode16 unicode_map_ZHT16BIG5_2b[(ZHT16BIG5_b1_max - ZHT16BIG5_b1_min + 1) *
                                                      (ZHT16BIG5_b2_max - ZHT16BIG5_b2_min + 1)];
        static typeUnicode16 unicode_map_ZHT16CCDC_2b[(ZHT16CCDC_b1_max - ZHT16CCDC_b1_min + 1) *
                                                      (ZHT16CCDC_b2_max - ZHT16CCDC_b2_min + 1)];
        static typeUnicode16 unicode_map_ZHT16HKSCS_2b[(ZHT16HKSCS_b1_max - ZHT16HKSCS_b1_min + 1) *
                                                       (ZHT16HKSCS_b2_max - ZHT16HKSCS_b2_min + 1)];
        static typeUnicode16 unicode_map_ZHT16MSWIN950_2b[(ZHT16MSWIN950_b1_max - ZHT16MSWIN950_b1_min + 1) *
                                                          (ZHT16MSWIN950_b2_max - ZHT16MSWIN950_b2_min + 1)];
    };
}

#endif
