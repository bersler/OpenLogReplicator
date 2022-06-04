/* Header for ReplicatorBatch class
   Copyright (C) 2018-2022 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "Replicator.h"

#ifndef REPLICATORBATCH_H_
#define REPLICATORBATCH_H_

namespace OpenLogReplicator {
    class ReplicatorBatch : public Replicator {
    protected:
        const char* getModeName() const override;
        bool continueWithOnline() override;
        void positionReader() override;
        void createSchema() override;

    public:
        ReplicatorBatch(Ctx* ctx, void (*archGetLog)(Replicator* replicator), Builder* builder, Metadata* metadata, TransactionBuffer* transactionBuffer,
                        std::string alias, const char* database);
        ~ReplicatorBatch() override;
    };
}

#endif
