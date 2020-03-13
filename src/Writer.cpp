/* Base class for target writer
   Copyright (C) 2018-2020 Adam Leszczynski.

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
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <iostream>
#include "Writer.h"

#include "CommandBuffer.h"
#include "OracleObject.h"
#include "OracleColumn.h"
#include "RedoLogRecord.h"
#include "RedoLogException.h"

using namespace std;

namespace OpenLogReplicator {

    Writer::Writer(const string alias, CommandBuffer *commandBuffer) :
        Thread(alias, commandBuffer) {
    }

    Writer::~Writer() {
    }

    void Writer::appendValue(RedoLogRecord *redoLogRecord, uint64_t typeNo, uint64_t fieldPos, uint64_t fieldLength) {
        uint64_t j, jMax;
        uint8_t digits;

        switch(typeNo) {
        case 1: //varchar(2)
        case 96: //char
            commandBuffer->append('\"');
            commandBuffer->appendEscape(redoLogRecord->data + fieldPos, fieldLength);
            commandBuffer->append('\"');
            break;

        case 2: //numeric
            digits = redoLogRecord->data[fieldPos + 0];
            //just zero
            if (digits == 0x80) {
                commandBuffer->append('0');
                break;
            }

            j = 1;
            jMax = fieldLength - 1;

            //positive number
            if (digits >= 0xC0 && jMax >= 1) {
                uint64_t val;
                //part of the total
                if (digits == 0xC0)
                    commandBuffer->append('0');
                else {
                    digits -= 0xC0;
                    //part of the total - omitting first zero for first digit
                    val = redoLogRecord->data[fieldPos + j] - 1;
                    if (val < 10)
                        commandBuffer->append('0' + val);
                    else
                        commandBuffer
                                ->append('0' + (val / 10))
                                ->append('0' + (val % 10));

                    ++j;
                    --digits;

                    while (digits > 0) {
                        val = redoLogRecord->data[fieldPos + j] - 1;
                        if (j <= jMax) {
                            commandBuffer
                                    ->append('0' + (val / 10))
                                    ->append('0' + (val % 10));
                            ++j;
                        } else {
                            commandBuffer->append("00");
                        }
                        --digits;
                    }
                }

                //fraction part
                if (j <= jMax) {
                    commandBuffer->append('.');

                    while (j <= jMax - 1) {
                        val = redoLogRecord->data[fieldPos + j] - 1;
                        commandBuffer
                                ->append('0' + (val / 10))
                                ->append('0' + (val % 10));
                        ++j;
                    }

                    //last digit - omitting 0 at the end
                    val = redoLogRecord->data[fieldPos + j] - 1;
                    commandBuffer->append('0' + (val / 10));
                    if ((val % 10) != 0)
                        commandBuffer->append('0' + (val % 10));
                }
            //negative number
            } else if (digits <= 0x3F && fieldLength >= 2) {
                uint64_t val;
                commandBuffer->append('-');

                if (redoLogRecord->data[fieldPos + jMax] == 0x66)
                    --jMax;

                //part of the total
                if (digits == 0x3F)
                    commandBuffer->append('0');
                else {
                    digits = 0x3F - digits;

                    val = 101 - redoLogRecord->data[fieldPos + j];
                    if (val < 10)
                        commandBuffer->append('0' + val);
                    else
                        commandBuffer
                                ->append('0' + (val / 10))
                                ->append('0' + (val % 10));
                    ++j;
                    --digits;

                    while (digits > 0) {
                        if (j <= jMax) {
                            val = 101 - redoLogRecord->data[fieldPos + j];
                            commandBuffer
                                    ->append('0' + (val / 10))
                                    ->append('0' + (val % 10));
                            ++j;
                        } else {
                            commandBuffer->append("00");
                        }
                        --digits;
                    }
                }

                if (j <= jMax) {
                    commandBuffer->append('.');

                    while (j <= jMax - 1) {
                        val = 101 - redoLogRecord->data[fieldPos + j];
                        commandBuffer
                                ->append('0' + (val / 10))
                                ->append('0' + (val % 10));
                        ++j;
                    }

                    val = 101 - redoLogRecord->data[fieldPos + j];
                    commandBuffer->append('0' + (val / 10));
                    if ((val % 10) != 0)
                        commandBuffer->append('0' + (val % 10));
                }
            } else {
                cerr << "ERROR: unknown value (type: " << typeNo << "): " << dec << fieldLength << " - ";
                for (uint64_t j = 0; j < fieldLength; ++j)
                    cout << " " << hex << setw(2) << (uint64_t) redoLogRecord->data[fieldPos + j];
                cout << endl;
            }
            break;

        case 12:
        case 180:
            commandBuffer->append('\"');
    //2012-04-23T18:25:43.511Z - ISO 8601 format
            jMax = fieldLength;

            if (jMax != 7) {
                cerr << "ERROR: unknown value (type: " << typeNo << "): ";
                for (uint64_t j = 0; j < fieldLength; ++j)
                    cout << " " << hex << setw(2) << (uint64_t) redoLogRecord->data[fieldPos + j];
                cout << endl;
            } else {
                uint64_t val1 = redoLogRecord->data[fieldPos + 0],
                         val2 = redoLogRecord->data[fieldPos + 1];
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
                    if (val1 > 10)
                        commandBuffer
                                ->append('0' + (val1 / 10))
                                ->append('0' + (val1 % 10))
                                ->append('0' + (val2 / 10))
                                ->append('0' + (val2 % 10));
                    else
                        commandBuffer
                                ->append('0' + val1)
                                ->append('0' + (val2 / 10))
                                ->append('0' + (val2 % 10));
                } else {
                    if (val2 > 10)
                        commandBuffer
                                ->append('0' + (val2 / 10))
                                ->append('0' + (val2 % 10));
                    else
                        commandBuffer->append('0' + val2);
                }

                if (bc)
                    commandBuffer->append("BC");

                commandBuffer
                        ->append('-')
                        ->append('0' + (redoLogRecord->data[fieldPos + 2] / 10))
                        ->append('0' + (redoLogRecord->data[fieldPos + 2] % 10))
                        ->append('-')
                        ->append('0' + (redoLogRecord->data[fieldPos + 3] / 10))
                        ->append('0' + (redoLogRecord->data[fieldPos + 3] % 10))
                        ->append('T')
                        ->append('0' + ((redoLogRecord->data[fieldPos + 4] - 1) / 10))
                        ->append('0' + ((redoLogRecord->data[fieldPos + 4] - 1) % 10))
                        ->append(':')
                        ->append('0' + ((redoLogRecord->data[fieldPos + 5] - 1) / 10))
                        ->append('0' + ((redoLogRecord->data[fieldPos + 5] - 1) % 10))
                        ->append(':')
                        ->append('0' + ((redoLogRecord->data[fieldPos + 6] - 1) / 10))
                        ->append('0' + ((redoLogRecord->data[fieldPos + 6] - 1) % 10));
            }
            commandBuffer->append('\"');
            break;

        default:
            commandBuffer->append("\"?\"");
        }
    }
}
