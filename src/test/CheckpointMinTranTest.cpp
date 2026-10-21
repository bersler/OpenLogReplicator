/* Test for the restart position (min-tran) stored with a checkpoint
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

This program is free software: you can redistribute it and/or
modify it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public
License along with this program; see the file LICENSE;
If not, see <http://www.gnu.org/licenses/>. */

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>

#include "../common/Ctx.h"
#include "../common/types/FileOffset.h"
#include "../common/types/Scn.h"
#include "../common/types/Seq.h"
#include "../common/types/Xid.h"
#include "../parser/TransactionBuffer.h"

// Transaction T began in sequence 20 and committed with SCN 462 in an LWN with SCN 455 (the records of an LWN can
// carry an SCN above the SCN of the LWN). The parser takes a checkpoint after that LWN, at 455. A client which has
// part of T restarts from scn 462: Metadata::readCheckpoints takes the checkpoint at 455 and Parser sends T since it
// committed at the start SCN, but T is no longer open, so without more the checkpoint points behind its commit and T
// is never read again. The checkpoint at 455 must point at the begin of T; later checkpoints above 462 need not.
namespace {
    int failures = 0;

    void check(const std::string& name, bool ok) {
        if (!ok) {
            std::cerr << "FAIL " << name << "\n";
            ++failures;
        } else
            std::cout << "ok   " << name << "\n";
    }

    struct MinTran {
        OpenLogReplicator::Seq sequence{OpenLogReplicator::Seq::none()};
        OpenLogReplicator::FileOffset fileOffset;
        OpenLogReplicator::Xid xid;
    };

    MinTran checkpointAt(OpenLogReplicator::TransactionBuffer& buffer, uint64_t scn) {
        MinTran m;
        buffer.checkpoint(OpenLogReplicator::Scn(scn), m.sequence, m.fileOffset, m.xid);
        return m;
    }
}

int main() {
    using namespace OpenLogReplicator;

    Ctx ctx;
    const Xid t(5, 19, 596);
    const Xid u(6, 2, 100);
    const Scn firstData(400);

    {
        auto buffer = std::make_unique<TransactionBuffer>(&ctx);
        check("nothing open or committed: no min-tran", checkpointAt(*buffer, 400).sequence == Seq::none());

        // T committed at 462 in the LWN 455
        buffer->addCommitted(Scn(462), firstData, Seq(20), FileOffset(533504ULL), t);
        const MinTran m = checkpointAt(*buffer, 455);
        check("checkpoint below the commit scn points at the begin of T", m.sequence == Seq(20) && m.fileOffset == FileOffset(533504ULL));
        check("checkpoint below the commit scn names T", m.xid == t);

        check("checkpoint at the commit scn still points at the begin of T", checkpointAt(*buffer, 462).sequence == Seq(20));
        check("checkpoint above the commit scn drops T", checkpointAt(*buffer, 463).sequence == Seq::none());
        check("T stays dropped", checkpointAt(*buffer, 470).sequence == Seq::none());
    }

    {
        auto buffer = std::make_unique<TransactionBuffer>(&ctx);
        // Two transactions committed in the same LWN: the older begin wins
        buffer->addCommitted(Scn(480), firstData, Seq(21), FileOffset(1024ULL), u);
        buffer->addCommitted(Scn(470), firstData, Seq(20), FileOffset(4096ULL), t);
        MinTran m = checkpointAt(*buffer, 465);
        check("two commits: the older begin is used", m.sequence == Seq(20) && m.fileOffset == FileOffset(4096ULL) && m.xid == t);
        m = checkpointAt(*buffer, 475);
        check("two commits: after the first commit scn the second begin is used",
              m.sequence == Seq(21) && m.fileOffset == FileOffset(1024ULL) && m.xid == u);
        check("two commits: after both commit scns nothing is left", checkpointAt(*buffer, 481).sequence == Seq::none());
    }

    {
        auto buffer = std::make_unique<TransactionBuffer>(&ctx);
        buffer->addCommitted(Scn(462), firstData, Seq(20), FileOffset(533504ULL), t);
        buffer->purge();
        check("purge drops committed begins", checkpointAt(*buffer, 455).sequence == Seq::none());
    }

    {
        // Catch-up: OLR reads from an old min-tran (or the cold-start low watermark) towards firstDataScn 100000.
        // Parser takes no checkpoint while lwnScn <= firstDataScn, so nothing prunes; commits below firstDataScn are
        // never sent and must not be remembered, otherwise the map grows by one entry per commit.
        auto buffer = std::make_unique<TransactionBuffer>(&ctx);
        const Scn catchUpFirstData(100000);
        uint64_t kept = 0;
        for (uint64_t scn = 1000; scn < 100000; ++scn)
            if (buffer->addCommitted(Scn(scn), catchUpFirstData, Seq(static_cast<uint32_t>(10 + (scn / 10000))), FileOffset(scn * 512), t))
                ++kept;
        check("catch-up: commits below firstDataScn are not kept", kept == 0 && buffer->committedSize() == 0);

        // The LWN that holds firstDataScn: its SCN is below, the commits in it at or above firstDataScn are kept
        for (uint64_t scn = 100000; scn < 100005; ++scn)
            buffer->addCommitted(Scn(scn), catchUpFirstData, Seq(20), FileOffset(scn * 512), u);
        check("catch-up: commits at or above firstDataScn in the boundary LWN are kept", buffer->committedSize() == 5);
        const MinTran m = checkpointAt(*buffer, 100002);
        check("catch-up: first checkpoint prunes below its scn", buffer->committedSize() == 3 && m.sequence == Seq(20));

        // Steady state: one checkpoint per LWN at the LWN SCN, each LWN with three commits at or above its SCN. Before
        // the checkpoint the map holds this LWN's commits and the previous one's, never more.
        size_t maxSize = 0;
        for (uint64_t lwn = 100010; lwn < 200000; lwn += 10) {
            for (uint64_t c = 0; c < 3; ++c)
                buffer->addCommitted(Scn(lwn + c), catchUpFirstData, Seq(21), FileOffset(lwn * 512), u);
            maxSize = std::max(maxSize, buffer->committedSize());
            checkpointAt(*buffer, lwn);
        }
        check("steady state: map stays bounded by the commits of two LWNs", maxSize <= 6 && buffer->committedSize() == 3);
    }

    if (failures > 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
