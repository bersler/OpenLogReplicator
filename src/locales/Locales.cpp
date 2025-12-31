/* Base class for locales
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
#include "Locales.h"

namespace OpenLogReplicator {
    Locales::Locales():
        timeZoneMap({
            {0x80a8, "Africa/Abidjan"},
            {0x80c8, "Africa/Accra"},
            {0x80a8, "Africa/Abidjan"},
            {0x80c8, "Africa/Accra"},
            {0x80bc, "Africa/Addis_Ababa"},
            {0x8078, "Africa/Algiers"},
            {0x80b8, "Africa/Asmara"},
            {0x88b8, "Africa/Asmera"},
            {0x80e8, "Africa/Bamako"},
            {0x8094, "Africa/Bangui"},
            {0x80c4, "Africa/Banjul"},
            {0x80d0, "Africa/Bissau"},
            {0x80e4, "Africa/Blantyre"},
            {0x80a4, "Africa/Brazzaville"},
            {0x808c, "Africa/Bujumbura"},
            {0x80b0, "Africa/Cairo"},
            {0x80f4, "Africa/Casablanca"},
            {0x8144, "Africa/Ceuta"},
            {0x80cc, "Africa/Conakry"},
            {0x8114, "Africa/Dakar"},
            {0x812c, "Africa/Dar_es_Salaam"},
            {0x80ac, "Africa/Djibouti"},
            {0x8090, "Africa/Douala"},
            {0x80f8, "Africa/El_Aaiun"},
            {0x8118, "Africa/Freetown"},
            {0x8084, "Africa/Gaborone"},
            {0x8140, "Africa/Harare"},
            {0x8120, "Africa/Johannesburg"},
            {0x8504, "Africa/Juba"},
            {0x8138, "Africa/Kampala"},
            {0x8124, "Africa/Khartoum"},
            {0x810c, "Africa/Kigali"},
            {0x809c, "Africa/Kinshasa"},
            {0x8108, "Africa/Lagos"},
            {0x80c0, "Africa/Libreville"},
            {0x8130, "Africa/Lome"},
            {0x807c, "Africa/Luanda"},
            {0x80a0, "Africa/Lubumbashi"},
            {0x813c, "Africa/Lusaka"},
            {0x80b4, "Africa/Malabo"},
            {0x80fc, "Africa/Maputo"},
            {0x80d8, "Africa/Maseru"},
            {0x8128, "Africa/Mbabane"},
            {0x811c, "Africa/Mogadishu"},
            {0x80dc, "Africa/Monrovia"},
            {0x80d4, "Africa/Nairobi"},
            {0x8098, "Africa/Ndjamena"},
            {0x8104, "Africa/Niamey"},
            {0x80f0, "Africa/Nouakchott"},
            {0x8088, "Africa/Ouagadougou"},
            {0x8080, "Africa/Porto-Novo"},
            {0x8110, "Africa/Sao_Tome"},
            {0x88e8, "Africa/Timbuktu"},
            {0x80e0, "Africa/Tripoli"},
            {0x8134, "Africa/Tunis"},
            {0x8100, "Africa/Windhoek"},
            {0x81b0, "America/Adak"},
            {0x81a8, "America/Anchorage"},
            {0x8248, "America/Anguilla"},
            {0x824c, "America/Antigua"},
            {0x82e8, "America/Araguaina"},
            {0x8abc, "America/Argentina/Buenos_Aires"},
            {0x8acc, "America/Argentina/Catamarca"},
            {0x92cc, "America/Argentina/ComodRivadavia"},
            {0x8ac4, "America/Argentina/Cordoba"},
            {0x8ac8, "America/Argentina/Jujuy"},
            {0x818c, "America/Argentina/La_Rioja"},
            {0x8ad0, "America/Argentina/Mendoza"},
            {0x8188, "America/Argentina/Rio_Gallegos"},
            {0x83b4, "America/Argentina/Salta"},
            {0x8394, "America/Argentina/San_Juan"},
            {0x8184, "America/Argentina/San_Luis"},
            {0x8390, "America/Argentina/Tucuman"},
            {0x82c0, "America/Argentina/Ushuaia"},
            {0x82d4, "America/Aruba"},
            {0x8320, "America/Asuncion"},
            {0x8374, "America/Atikokan"},
            {0x89b0, "America/Atka"},
            {0x8168, "America/Bahia"},
            {0x817c, "America/Bahia_Banderas"},
            {0x8254, "America/Barbados"},
            {0x82e0, "America/Belem"},
            {0x8258, "America/Belize"},
            {0x8380, "America/Blanc-Sablon"},
            {0x82fc, "America/Boa_Vista"},
            {0x830c, "America/Bogota"},
            {0x81b8, "America/Boise"},
            {0x82bc, "America/Buenos_Aires"},
            {0x821c, "America/Cambridge_Bay"},
            {0x8378, "America/Campo_Grande"},
            {0x8230, "America/Cancun"},
            {0x8334, "America/Caracas"},
            {0x82cc, "America/Catamarca"},
            {0x8318, "America/Cayenne"},
            {0x825c, "America/Cayman"},
            {0x8194, "America/Chicago"},
            {0x8238, "America/Chihuahua"},
            {0x8b74, "America/Coral_Harbour"},
            {0x82c4, "America/Cordoba"},
            {0x8260, "America/Costa_Rica"},
            {0x8514, "America/Creston"},
            {0x82f4, "America/Cuiaba"},
            {0x8310, "America/Curacao"},
            {0x837c, "America/Danmarkshavn"},
            {0x822c, "America/Dawson"},
            {0x820c, "America/Dawson_Creek"},
            {0x8198, "America/Denver"},
            {0x81d0, "America/Detroit"},
            {0x8268, "America/Dominica"},
            {0x8204, "America/Edmonton"},
            {0x8384, "America/Eirunepe"},
            {0x8270, "America/El_Salvador"},
            {0x8a44, "America/Ensenada"},
            {0x82e4, "America/Fortaleza"},
            {0x855c, "America/Fort_Nelson"},
            {0x89bc, "America/Fort_Wayne"},
            {0x81e4, "America/Glace_Bay"},
            {0x833c, "America/Godthab"},
            {0x81dc, "America/Goose_Bay"},
            {0x82b0, "America/Grand_Turk"},
            {0x8274, "America/Grenada"},
            {0x8278, "America/Guadeloupe"},
            {0x827c, "America/Guatemala"},
            {0x8314, "America/Guayaquil"},
            {0x831c, "America/Guyana"},
            {0x81e0, "America/Halifax"},
            {0x8264, "America/Havana"},
            {0x823c, "America/Hermosillo"},
            {0x99bc, "America/Indiana/Indianapolis"},
            {0x81c4, "America/Indiana/Knox"},
            {0x81c0, "America/Indiana/Marengo"},
            {0x8348, "America/Indiana/Petersburg"},
            {0x81bc, "America/Indianapolis"},
            {0x8178, "America/Indiana/Tell_City"},
            {0x81c8, "America/Indiana/Vevay"},
            {0x8344, "America/Indiana/Vincennes"},
            {0x8368, "America/Indiana/Winamac"},
            {0x8224, "America/Inuvik"},
            {0x8214, "America/Iqaluit"},
            {0x8288, "America/Jamaica"},
            {0x82c8, "America/Jujuy"},
            {0x81a0, "America/Juneau"},
            {0x89cc, "America/Kentucky/Louisville"},
            {0x816c, "America/Kentucky/Monticello"},
            {0x89c4, "America/Knox_IN"},
            {0x850c, "America/Kralendijk"},
            {0x82d8, "America/La_Paz"},
            {0x8324, "America/Lima"},
            {0x819c, "America/Los_Angeles"},
            {0x81cc, "America/Louisville"},
            {0x8508, "America/Lower_Princes"},
            {0x82ec, "America/Maceio"},
            {0x8294, "America/Managua"},
            {0x8300, "America/Manaus"},
            {0x8a78, "America/Marigot"},
            {0x828c, "America/Martinique"},
            {0x815c, "America/Matamoros"},
            {0x8240, "America/Mazatlan"},
            {0x82d0, "America/Mendoza"},
            {0x81d4, "America/Menominee"},
            {0x8388, "America/Merida"},
            {0x84fc, "America/Metlakatla"},
            {0x8234, "America/Mexico_City"},
            {0x82a8, "America/Miquelon"},
            {0x8170, "America/Moncton"},
            {0x838c, "America/Monterrey"},
            {0x8330, "America/Montevideo"},
            {0x81e8, "America/Montreal"},
            {0x8290, "America/Montserrat"},
            {0x8250, "America/Nassau"},
            {0x8190, "America/New_York"},
            {0x81f0, "America/Nipigon"},
            {0x81ac, "America/Nome"},
            {0x82dc, "America/Noronha"},
            {0x8500, "America/North_Dakota/Beulah"},
            {0x8160, "America/North_Dakota/Center"},
            {0x8164, "America/North_Dakota/New_Salem"},
            {0x8174, "America/Ojinaga"},
            {0x8298, "America/Panama"},
            {0x8210, "America/Pangnirtung"},
            {0x8328, "America/Paramaribo"},
            {0x81b4, "America/Phoenix"},
            {0x8280, "America/Port-au-Prince"},
            {0x8304, "America/Porto_Acre"},
            {0x832c, "America/Port_of_Spain"},
            {0x82f8, "America/Porto_Velho"},
            {0x829c, "America/Puerto_Rico"},
            {0x8628, "America/Punta_Arenas"},
            {0x81f4, "America/Rainy_River"},
            {0x8218, "America/Rankin_Inlet"},
            {0x8158, "America/Recife"},
            {0x81fc, "America/Regina"},
            {0x836c, "America/Resolute"},
            {0x9304, "America/Rio_Branco"},
            {0x92c4, "America/Rosario"},
            {0x8180, "America/Santa_Isabel"},
            {0x814c, "America/Santarem"},
            {0x8308, "America/Santiago"},
            {0x826c, "America/Santo_Domingo"},
            {0x82f0, "America/Sao_Paulo"},
            {0x8338, "America/Scoresbysund"},
            {0x9998, "America/Shiprock"},
            {0x84f8, "America/Sitka"},
            {0x9278, "America/St_Barthelemy"},
            {0x81d8, "America/St_Johns"},
            {0x82a0, "America/St_Kitts"},
            {0x82a4, "America/St_Lucia"},
            {0x82b8, "America/St_Thomas"},
            {0x82ac, "America/St_Vincent"},
            {0x8200, "America/Swift_Current"},
            {0x8284, "America/Tegucigalpa"},
            {0x8340, "America/Thule"},
            {0x81ec, "America/Thunder_Bay"},
            {0x8244, "America/Tijuana"},
            {0x8370, "America/Toronto"},
            {0x82b4, "America/Tortola"},
            {0x8208, "America/Vancouver"},
            {0x8ab8, "America/Virgin"},
            {0x8228, "America/Whitehorse"},
            {0x81f8, "America/Winnipeg"},
            {0x81a4, "America/Yakutat"},
            {0x8220, "America/Yellowknife"},
            {0x8398, "Antarctica/Casey"},
            {0x839c, "Antarctica/Davis"},
            {0x83a4, "Antarctica/DumontDUrville"},
            {0x8154, "Antarctica/Macquarie"},
            {0x83a0, "Antarctica/Mawson"},
            {0x83b0, "Antarctica/McMurdo"},
            {0x83ac, "Antarctica/Palmer"},
            {0x8148, "Antarctica/Rothera"},
            {0x8bb0, "Antarctica/South_Pole"},
            {0x83a8, "Antarctica/Syowa"},
            {0x8524, "Antarctica/Troll"},
            {0x80ec, "Antarctica/Vostok"},
            {0x8e34, "Arctic/Longyearbyen"},
            {0x84b8, "Asia/Aden"},
            {0x8434, "Asia/Almaty"},
            {0x8430, "Asia/Amman"},
            {0x84e0, "Asia/Anadyr"},
            {0x843c, "Asia/Aqtau"},
            {0x8438, "Asia/Aqtobe"},
            {0x84a4, "Asia/Ashgabat"},
            {0x8ca4, "Asia/Ashkhabad"},
            {0x85ac, "Asia/Atyrau"},
            {0x8424, "Asia/Baghdad"},
            {0x83cc, "Asia/Bahrain"},
            {0x83c8, "Asia/Baku"},
            {0x84a0, "Asia/Bangkok"},
            {0x859c, "Asia/Barnaul"},
            {0x8454, "Asia/Beirut"},
            {0x8440, "Asia/Bishkek"},
            {0x83d8, "Asia/Brunei"},
            {0x8410, "Asia/Calcutta"},
            {0x853c, "Asia/Chita"},
            {0x84f0, "Asia/Choibalsan"},
            {0x8bec, "Asia/Chongqing"},
            {0x83ec, "Asia/Chungking"},
            {0x8494, "Asia/Colombo"},
            {0x83d0, "Asia/Dacca"},
            {0x8498, "Asia/Damascus"},
            {0x8bd0, "Asia/Dhaka"},
            {0x840c, "Asia/Dili"},
            {0x84a8, "Asia/Dubai"},
            {0x849c, "Asia/Dushanbe"},
            {0x85a8, "Asia/Famagusta"},
            {0x8474, "Asia/Gaza"},
            {0x83e4, "Asia/Harbin"},
            {0x8510, "Asia/Hebron"},
            {0x8cb4, "Asia/Ho_Chi_Minh"},
            {0x83f8, "Asia/Hong_Kong"},
            {0x8460, "Asia/Hovd"},
            {0x84cc, "Asia/Irkutsk"},
            {0x965c, "Asia/Istanbul"},
            {0x8414, "Asia/Jakarta"},
            {0x841c, "Asia/Jayapura"},
            {0x8428, "Asia/Jerusalem"},
            {0x83c0, "Asia/Kabul"},
            {0x84dc, "Asia/Kamchatka"},
            {0x8470, "Asia/Karachi"},
            {0x83f4, "Asia/Kashgar"},
            {0x8c74, "Asia/Kathmandu"},
            {0x8468, "Asia/Katmandu"},
            {0x8518, "Asia/Khandyga"},
            {0x8c10, "Asia/Kolkata"},
            {0x84c8, "Asia/Krasnoyarsk"},
            {0x8458, "Asia/Kuala_Lumpur"},
            {0x845c, "Asia/Kuching"},
            {0x844c, "Asia/Kuwait"},
            {0x8400, "Asia/Macao"},
            {0x8c00, "Asia/Macau"},
            {0x84d8, "Asia/Magadan"},
            {0x8c18, "Asia/Makassar"},
            {0x8478, "Asia/Manila"},
            {0x846c, "Asia/Muscat"},
            {0x8404, "Asia/Nicosia"},
            {0x8150, "Asia/Novokuznetsk"},
            {0x84c4, "Asia/Novosibirsk"},
            {0x84c0, "Asia/Omsk"},
            {0x84ec, "Asia/Oral"},
            {0x83e0, "Asia/Phnom_Penh"},
            {0x84e4, "Asia/Pontianak"},
            {0x8448, "Asia/Pyongyang"},
            {0x847c, "Asia/Qatar"},
            {0x84e8, "Asia/Qyzylorda"},
            {0x83dc, "Asia/Rangoon"},
            {0x8480, "Asia/Riyadh"},
            {0x84b4, "Asia/Saigon"},
            {0x84f4, "Asia/Sakhalin"},
            {0x84ac, "Asia/Samarkand"},
            {0x8444, "Asia/Seoul"},
            {0x83e8, "Asia/Shanghai"},
            {0x8490, "Asia/Singapore"},
            {0x8554, "Asia/Srednekolymsk"},
            {0x83fc, "Asia/Taipei"},
            {0x84b0, "Asia/Tashkent"},
            {0x8408, "Asia/Tbilisi"},
            {0x8420, "Asia/Tehran"},
            {0x8c28, "Asia/Tel_Aviv"},
            {0x8bd4, "Asia/Thimbu"},
            {0x83d4, "Asia/Thimphu"},
            {0x842c, "Asia/Tokyo"},
            {0x85a0, "Asia/Tomsk"},
            {0x8418, "Asia/Ujung_Pandang"},
            {0x8464, "Asia/Ulaanbaatar"},
            {0x8c64, "Asia/Ulan_Bator"},
            {0x83f0, "Asia/Urumqi"},
            {0x851c, "Asia/Ust-Nera"},
            {0x8450, "Asia/Vientiane"},
            {0x84d4, "Asia/Vladivostok"},
            {0x84d0, "Asia/Yakutsk"},
            {0x85a4, "Asia/Yangon"},
            {0x84bc, "Asia/Yekaterinburg"},
            {0x83c4, "Asia/Yerevan"},
            {0x8540, "Atlantic/Azores"},
            {0x8528, "Atlantic/Bermuda"},
            {0x8548, "Atlantic/Canary"},
            {0x854c, "Atlantic/Cape_Verde"},
            {0x8d34, "Atlantic/Faeroe"},
            {0x8534, "Atlantic/Faroe"},
            {0x9634, "Atlantic/Jan_Mayen"},
            {0x8544, "Atlantic/Madeira"},
            {0x8538, "Atlantic/Reykjavik"},
            {0x8530, "Atlantic/South_Georgia"},
            {0x852c, "Atlantic/Stanley"},
            {0x8550, "Atlantic/St_Helena"},
            {0x8d80, "Australia/ACT"},
            {0x8574, "Australia/Adelaide"},
            {0x856c, "Australia/Brisbane"},
            {0x8584, "Australia/Broken_Hill"},
            {0x9580, "Australia/Canberra"},
            {0x858c, "Australia/Currie"},
            {0x8564, "Australia/Darwin"},
            {0x8590, "Australia/Eucla"},
            {0x8578, "Australia/Hobart"},
            {0x8d88, "Australia/LHI"},
            {0x8570, "Australia/Lindeman"},
            {0x8588, "Australia/Lord_Howe"},
            {0x857c, "Australia/Melbourne"},
            {0x8d64, "Australia/North"},
            {0x9d80, "Australia/NSW"},
            {0x8568, "Australia/Perth"},
            {0x8d6c, "Australia/Queensland"},
            {0x8d74, "Australia/South"},
            {0x8580, "Australia/Sydney"},
            {0x8d78, "Australia/Tasmania"},
            {0x8d7c, "Australia/Victoria"},
            {0x8d68, "Australia/West"},
            {0x8d84, "Australia/Yancowinna"},
            {0x8b04, "Brazil/Acre"},
            {0x8adc, "Brazil/DeNoronha"},
            {0x8af0, "Brazil/East"},
            {0x8b00, "Brazil/West"},
            {0x89e0, "Canada/Atlantic"},
            {0x89f8, "Canada/Central"},
            {0x89e8, "Canada/Eastern"},
            {0x89fc, "Canada/East-Saskatchewan"},
            {0x8a04, "Canada/Mountain"},
            {0x89d8, "Canada/Newfoundland"},
            {0x8a08, "Canada/Pacific"},
            {0x91fc, "Canada/Saskatchewan"},
            {0x8a28, "Canada/Yukon"},
            {0x85b8, "CET"},
            {0x8b08, "Chile/Continental"},
            {0x8f0c, "Chile/EasterIsland"},
            {0x9994, "CST"},
            {0x835c, "CST6CDT"},
            {0x8a64, "Cuba"},
            {0x85c0, "EET"},
            {0x88b0, "Egypt"},
            {0x8dcc, "Eire"},
            {0x834c, "EST"},
            {0x8358, "EST5EDT"},
            {0x9004, "Etc/GMT+0"},
            {0xa004, "Etc/GMT-0"},
            {0xb004, "Etc/GMT0"},
            {0x8004, "Etc/GMT"},
            {0x8018, "Etc/GMT-10"},
            {0x8064, "Etc/GMT+10"},
            {0x803c, "Etc/GMT-1"},
            {0x8040, "Etc/GMT+1"},
            {0x8014, "Etc/GMT-11"},
            {0x8068, "Etc/GMT+11"},
            {0x8010, "Etc/GMT-12"},
            {0x806c, "Etc/GMT+12"},
            {0x800c, "Etc/GMT-13"},
            {0x8008, "Etc/GMT-14"},
            {0x8038, "Etc/GMT-2"},
            {0x8044, "Etc/GMT+2"},
            {0x8034, "Etc/GMT-3"},
            {0x8048, "Etc/GMT+3"},
            {0x8030, "Etc/GMT-4"},
            {0x804c, "Etc/GMT+4"},
            {0x802c, "Etc/GMT-5"},
            {0x8050, "Etc/GMT+5"},
            {0x8028, "Etc/GMT-6"},
            {0x8054, "Etc/GMT+6"},
            {0x8024, "Etc/GMT-7"},
            {0x8058, "Etc/GMT+7"},
            {0x8020, "Etc/GMT-8"},
            {0x805c, "Etc/GMT+8"},
            {0x801c, "Etc/GMT-9"},
            {0x8060, "Etc/GMT+9"},
            {0xc004, "Etc/Greenwich"},
            {0x8074, "Etc/UCT"},
            {0x8870, "Etc/Universal"},
            {0x8070, "Etc/UTC"},
            {0x9870, "Etc/Zulu"},
            {0x8630, "Europe/Amsterdam"},
            {0x85d4, "Europe/Andorra"},
            {0x8560, "Europe/Astrakhan"},
            {0x8604, "Europe/Athens"},
            {0x85c8, "Europe/Belfast"},
            {0x8670, "Europe/Belgrade"},
            {0x85fc, "Europe/Berlin"},
            {0x8de8, "Europe/Bratislava"},
            {0x85e0, "Europe/Brussels"},
            {0x8640, "Europe/Bucharest"},
            {0x8608, "Europe/Budapest"},
            {0x8520, "Europe/Busingen"},
            {0x8624, "Europe/Chisinau"},
            {0x85ec, "Europe/Copenhagen"},
            {0x85cc, "Europe/Dublin"},
            {0x8600, "Europe/Gibraltar"},
            {0xa5c4, "Europe/Guernsey"},
            {0x85f4, "Europe/Helsinki"},
            {0xadc4, "Europe/Isle_of_Man"},
            {0x865c, "Europe/Istanbul"},
            {0x9dc4, "Europe/Jersey"},
            {0x8644, "Europe/Kaliningrad"},
            {0x8660, "Europe/Kiev"},
            {0x8594, "Europe/Kirov"},
            {0x863c, "Europe/Lisbon"},
            {0x8e70, "Europe/Ljubljana"},
            {0x85c4, "Europe/London"},
            {0x861c, "Europe/Luxembourg"},
            {0x8650, "Europe/Madrid"},
            {0x8620, "Europe/Malta"},
            {0x8df4, "Europe/Mariehamn"},
            {0x85dc, "Europe/Minsk"},
            {0x862c, "Europe/Monaco"},
            {0x8648, "Europe/Moscow"},
            {0x8c04, "Europe/Nicosia"},
            {0x8634, "Europe/Oslo"},
            {0x85f8, "Europe/Paris"},
            {0xae70, "Europe/Podgorica"},
            {0x85e8, "Europe/Prague"},
            {0x8610, "Europe/Riga"},
            {0x860c, "Europe/Rome"},
            {0x864c, "Europe/Samara"},
            {0x960c, "Europe/San_Marino"},
            {0x9670, "Europe/Sarajevo"},
            {0x85b0, "Europe/Saratov"},
            {0x866c, "Europe/Simferopol"},
            {0x9e70, "Europe/Skopje"},
            {0x85e4, "Europe/Sofia"},
            {0x8654, "Europe/Stockholm"},
            {0x85f0, "Europe/Tallinn"},
            {0x85d0, "Europe/Tirane"},
            {0x8e24, "Europe/Tiraspol"},
            {0x8598, "Europe/Ulyanovsk"},
            {0x8664, "Europe/Uzhgorod"},
            {0x8614, "Europe/Vaduz"},
            {0x8e0c, "Europe/Vatican"},
            {0x85d8, "Europe/Vienna"},
            {0x8618, "Europe/Vilnius"},
            {0x8674, "Europe/Volgograd"},
            {0x8638, "Europe/Warsaw"},
            {0xa670, "Europe/Zagreb"},
            {0x8668, "Europe/Zaporozhye"},
            {0x8658, "Europe/Zurich"},
            {0x8dc4, "GB"},
            {0x95c4, "GB-Eire"},
            {0x9804, "GMT+0"},
            {0xa804, "GMT-0"},
            {0xb804, "GMT0"},
            {0x8804, "GMT"},
            {0xc804, "Greenwich"},
            {0x8bf8, "Hongkong"},
            {0x8354, "HST"},
            {0x8d38, "Iceland"},
            {0x86d8, "Indian/Antananarivo"},
            {0x86d0, "Indian/Chagos"},
            {0x86dc, "Indian/Christmas"},
            {0x86e0, "Indian/Cocos"},
            {0x86e4, "Indian/Comoro"},
            {0x86cc, "Indian/Kerguelen"},
            {0x86e8, "Indian/Mahe"},
            {0x86d4, "Indian/Maldives"},
            {0x86ec, "Indian/Mauritius"},
            {0x86f0, "Indian/Mayotte"},
            {0x86f4, "Indian/Reunion"},
            {0x8c20, "Iran"},
            {0x9428, "Israel"},
            {0x8a88, "Jamaica"},
            {0x8c2c, "Japan"},
            {0x8f40, "Kwajalein"},
            {0x88e0, "Libya"},
            {0x85bc, "MET"},
            {0x9244, "Mexico/BajaNorte"},
            {0x8a40, "Mexico/BajaSur"},
            {0x8a34, "Mexico/General"},
            {0x8350, "MST"},
            {0x8360, "MST7MDT"},
            {0x8998, "Navajo"},
            {0x8f5c, "NZ"},
            {0x8f60, "NZ-CHAT"},
            {0x877c, "Pacific/Apia"},
            {0x875c, "Pacific/Auckland"},
            {0x8558, "Pacific/Bougainville"},
            {0x8760, "Pacific/Chatham"},
            {0x83b8, "Pacific/Chuuk"},
            {0x870c, "Pacific/Easter"},
            {0x87a0, "Pacific/Efate"},
            {0x8730, "Pacific/Enderbury"},
            {0x8788, "Pacific/Fakaofo"},
            {0x8718, "Pacific/Fiji"},
            {0x8790, "Pacific/Funafuti"},
            {0x8710, "Pacific/Galapagos"},
            {0x871c, "Pacific/Gambier"},
            {0x8784, "Pacific/Guadalcanal"},
            {0x8728, "Pacific/Guam"},
            {0x8708, "Pacific/Honolulu"},
            {0x8794, "Pacific/Johnston"},
            {0x8734, "Pacific/Kiritimati"},
            {0x8750, "Pacific/Kosrae"},
            {0x8740, "Pacific/Kwajalein"},
            {0x873c, "Pacific/Majuro"},
            {0x8720, "Pacific/Marquesas"},
            {0x8798, "Pacific/Midway"},
            {0x8754, "Pacific/Nauru"},
            {0x8764, "Pacific/Niue"},
            {0x8768, "Pacific/Norfolk"},
            {0x8758, "Pacific/Noumea"},
            {0x8778, "Pacific/Pago_Pago"},
            {0x876c, "Pacific/Palau"},
            {0x8774, "Pacific/Pitcairn"},
            {0x83bc, "Pacific/Pohnpei"},
            {0x874c, "Pacific/Ponape"},
            {0x8770, "Pacific/Port_Moresby"},
            {0x8714, "Pacific/Rarotonga"},
            {0x8738, "Pacific/Saipan"},
            {0x9778, "Pacific/Samoa"},
            {0x8724, "Pacific/Tahiti"},
            {0x872c, "Pacific/Tarawa"},
            {0x878c, "Pacific/Tongatapu"},
            {0x8748, "Pacific/Truk"},
            {0x879c, "Pacific/Wake"},
            {0x87a4, "Pacific/Wallis"},
            {0x8f48, "Pacific/Yap"},
            {0x8e38, "Poland"},
            {0x8e3c, "Portugal"},
            {0x8be8, "PRC"},
            {0xa19c, "PST"},
            {0x8364, "PST8PDT"},
            {0x8bfc, "ROC"},
            {0x8c44, "ROK"},
            {0x8c90, "Singapore"},
            {0x8e5c, "Turkey"},
            {0x8874, "UCT"},
            {0x9070, "Universal"},
            {0x89a8, "US/Alaska"},
            {0x91b0, "US/Aleutian"},
            {0x89b4, "US/Arizona"},
            {0x8994, "US/Central"},
            {0x8990, "US/Eastern"},
            {0x91bc, "US/East-Indiana"},
            {0x8f08, "US/Hawaii"},
            {0x91c4, "US/Indiana-Starke"},
            {0x89d0, "US/Michigan"},
            {0x9198, "US/Mountain"},
            {0x899c, "US/Pacific"},
            {0x999c, "US/Pacific-New"},
            {0x8f78, "US/Samoa"},
            {0xd004, "UTC"},
            {0x85b4, "WET"},
            {0x8e48, "W-SU"},
            {0xa070, "Zulu"}
        }) {}

    Locales::~Locales() {
        for (const auto& [_, cs]: characterMap)
            delete cs;
        timeZoneMap.clear();
        characterMap.clear();
    }

    void Locales::initialize() {
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
        characterMap[829] = new CharacterSet16bit("JA16VMS", CharacterSet16bit::unicode_map_JA16VMS, CharacterSet16bit::JA16VMS_b1_min,
                                                  CharacterSet16bit::JA16VMS_b1_max, CharacterSet16bit::JA16VMS_b2_min, CharacterSet16bit::JA16VMS_b2_max);
        characterMap[830] = new CharacterSetJA16EUC();
        characterMap[831] = new CharacterSetJA16EUC("JA16EUCYEN");
        characterMap[832] = new CharacterSetJA16SJIS();
        characterMap[834] = new CharacterSetJA16SJIS("JA16SJISYEN");
        characterMap[837] = new CharacterSetJA16EUCTILDE();
        characterMap[838] = new CharacterSetJA16SJISTILDE();
        characterMap[840] = new CharacterSet16bit("KO16KSC5601", CharacterSet16bit::unicode_map_KO16KSC5601_2b, CharacterSet16bit::KO16KSC5601_b1_min,
                                                  CharacterSet16bit::KO16KSC5601_b1_max, CharacterSet16bit::KO16KSC5601_b2_min,
                                                  CharacterSet16bit::KO16KSC5601_b2_max);
        characterMap[845] = new CharacterSetKO16KSCCS();
        characterMap[846] = new CharacterSet16bit("KO16MSWIN949", CharacterSet16bit::unicode_map_KO16MSWIN949_2b, CharacterSet16bit::KO16MSWIN949_b1_min,
                                                  CharacterSet16bit::KO16MSWIN949_b1_max, CharacterSet16bit::KO16MSWIN949_b2_min,
                                                  CharacterSet16bit::KO16MSWIN949_b2_max);
        characterMap[850] = new CharacterSet16bit("ZHS16CGB231280", CharacterSet16bit::unicode_map_ZHS16CGB231280_2b, CharacterSet16bit::ZHS16CGB231280_b1_min,
                                                  CharacterSet16bit::ZHS16CGB231280_b1_max, CharacterSet16bit::ZHS16CGB231280_b2_min,
                                                  CharacterSet16bit::ZHS16CGB231280_b2_max);
        characterMap[852] = new CharacterSetZHS16GBK();
        characterMap[854] = new CharacterSetZHS32GB18030();
        characterMap[860] = new CharacterSetZHT32EUC();
        characterMap[863] = new CharacterSetZHT32TRIS();
        characterMap[865] = new CharacterSet16bit("ZHT16BIG5", CharacterSet16bit::unicode_map_ZHT16BIG5_2b, CharacterSet16bit::ZHT16BIG5_b1_min,
                                                  CharacterSet16bit::ZHT16BIG5_b1_max, CharacterSet16bit::ZHT16BIG5_b2_min,
                                                  CharacterSet16bit::ZHT16BIG5_b2_max);
        characterMap[866] = new CharacterSet16bit("ZHT16CCDC", CharacterSet16bit::unicode_map_ZHT16CCDC_2b, CharacterSet16bit::ZHT16CCDC_b1_min,
                                                  CharacterSet16bit::ZHT16CCDC_b1_max, CharacterSet16bit::ZHT16CCDC_b2_min,
                                                  CharacterSet16bit::ZHT16CCDC_b2_max);
        characterMap[867] = new CharacterSet16bit("ZHT16MSWIN950", CharacterSet16bit::unicode_map_ZHT16MSWIN950_2b, CharacterSet16bit::ZHT16MSWIN950_b1_min,
                                                  CharacterSet16bit::ZHT16MSWIN950_b1_max, CharacterSet16bit::ZHT16MSWIN950_b2_min,
                                                  CharacterSet16bit::ZHT16MSWIN950_b2_max);
        characterMap[868] = new CharacterSet16bit("ZHT16HKSCS", CharacterSet16bit::unicode_map_ZHT16HKSCS_2b, CharacterSet16bit::ZHT16HKSCS_b1_min,
                                                  CharacterSet16bit::ZHT16HKSCS_b1_max, CharacterSet16bit::ZHT16HKSCS_b2_min,
                                                  CharacterSet16bit::ZHT16HKSCS_b2_max);
        characterMap[871] = new CharacterSetUTF8();
        characterMap[873] = new CharacterSetAL32UTF8();
        characterMap[992] = new CharacterSetZHT16HKSCS31();
        characterMap[1002] = new CharacterSet8bit("TIMESTEN8", CharacterSet8bit::unicode_map_TIMESTEN8);
        characterMap[2000] = new CharacterSetAL16UTF16();
    }
}
