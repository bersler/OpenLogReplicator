/* Header for TransactionMap class
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

#include "types.h"

#ifndef TRANSACTIONMAP_H_
#define TRANSACTIONMAP_H_

#define HASHINGFUNCTION(uba,slt,rci) ((uba^((uint64_t)slt<<9)^((uint64_t)rci<<37))%((maps*MEMORY_CHUNK_SIZE_MB*1024*1024/sizeof(Transaction*))-1))
#define MAPS_MAX (MAX_TRANSACTIONS_LIMIT*2*sizeof(Transaction*)/(MEMORY_CHUNK_SIZE_MB*1024*1024))
#define MAPS_IN_CHUNK (1024*1024/sizeof(Transaction*))
#define MAP_AT(a) hashMapList[(a)/MAPS_IN_CHUNK][(a)%MAPS_IN_CHUNK]

namespace OpenLogReplicator {
    class OracleAnalyser;
    class RedoLogRecord;
    class Transaction;

    class TransactionMap {
    protected:
        OracleAnalyser *oracleAnalyser;
        uint64_t maps;
        uint64_t elements;
        Transaction** hashMapList[MAPS_MAX];

    public:
        TransactionMap(OracleAnalyser *oracleAnalyser, uint64_t maps);
        virtual ~TransactionMap();

        void erase(Transaction * transaction);
        void set(Transaction * transaction);
        Transaction* getMatchForRollback(RedoLogRecord *rollbackRedoLogRecord1, RedoLogRecord *rollbackRedoLogRecord2);
    };
}

#endif
