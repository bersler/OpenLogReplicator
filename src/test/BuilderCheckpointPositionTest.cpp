/* Test that a checkpoint never moves the message position (c_scn, c_idx) back
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

#include <iostream>
#include <string>

#include "../builder/BuilderJson.h"
#include "../common/Attribute.h"
#include "../common/Ctx.h"
#include "../common/Format.h"
#include "../common/Thread.h"
#include "../common/types/FileOffset.h"
#include "../locales/Locales.h"
#include "../metadata/Metadata.h"

// T1 commits at SCN 462 inside the LWN with SCN 455, then the checkpoint for LWN 455 is built, then T2 commits at 462
// too. The checkpoint must not move the position back to 455, or T2 restarts the index at 462 and reuses positions
// T1 already had: a client that continues from T1's last position would then skip T2's first messages.
namespace {
    int failures = 0;

    void check(const std::string& name, bool ok) {
        if (!ok) {
            std::cerr << "FAIL " << name << "\n";
            ++failures;
        } else
            std::cout << "ok   " << name << "\n";
    }

    class TestThread final : public OpenLogReplicator::Thread {
    public:
        explicit TestThread(OpenLogReplicator::Ctx* newCtx): Thread(newCtx, "test") {}
        void run() override {}
        std::string getName() const override { return "test"; }
    };
}

int main() {
    using namespace OpenLogReplicator;

    Ctx ctx;
    TestThread thread(&ctx);
    ctx.parserThread = &thread;
    ctx.initialize(32, 64, 16, 16, 0, 0, 16, 16);
    Locales locales;
    Metadata metadata(&ctx, &locales, "TEST", Scn::zero(), Seq::zero(), "", 0);
    Format format(Format::DB_FORMAT::DEFAULT, Format::ATTRIBUTES_FORMAT::DEFAULT, Format::INTERVAL_DTS_FORMAT::UNIX_NANO,
                  Format::INTERVAL_YTM_FORMAT::MONTHS, Format::MESSAGE_FORMAT::DEFAULT, Format::RID_FORMAT::SKIP,
                  Format::REDO_THREAD_FORMAT::SKIP, Format::XID_FORMAT::TEXT_HEX, Format::TIMESTAMP_FORMAT::UNIX_NANO,
                  Format::TIMESTAMP_FORMAT::UNIX_NANO, Format::TIMESTAMP_TZ_FORMAT::UNIX_NANO_STRING, Format::TIMESTAMP_TYPE::DEFAULT,
                  Format::CHAR_FORMAT::UTF8, Format::SCN_FORMAT::NUMERIC, Format::SCN_TYPE::DEFAULT, Format::UNKNOWN_FORMAT::QUESTION_MARK,
                  Format::SCHEMA_FORMAT::DEFAULT, Format::COLUMN_FORMAT::CHANGED, Format::UNKNOWN_TYPE::HIDE, Format::USER_TYPE::DEFAULT);
    BuilderJson builder(&ctx, &locales, &metadata, format, 0);
    builder.initialize();

    AttributeMap attributes;
    attributes[Attribute::KEY::LOGIN_USER_NAME] = "TEST";

    // T1 commits at 462 and has used three positions at that scn
    builder.processBegin(Xid(1, 1, 1), 1, Seq(1), Scn(450), Time(0), Seq(1), Scn(462), Time(0), &attributes);
    builder.lwnIdx = 3;
    const Scn t1Scn = builder.lwnScn;
    const typeIdx t1LastIdx = builder.lwnIdx;

    // Checkpoint for the LWN T1 committed in
    builder.processCheckpoint(Seq(1), Scn(455), Time(0), FileOffset(0), false);
    const auto* chkpt = reinterpret_cast<const BuilderMsg*>(builder.firstBuilderQueue->data);
    check("checkpoint keeps the position at T1's commit scn", builder.lwnScn == t1Scn);
    check("checkpoint message is not positioned before T1", chkpt->lwnScn == t1Scn && chkpt->lwnIdx >= t1LastIdx);

    // T2 commits at the same scn in the next LWN: its positions follow T1's
    builder.processBegin(Xid(2, 2, 2), 1, Seq(1), Scn(458), Time(0), Seq(1), Scn(462), Time(0), &attributes);
    check("T2 keeps the position scn", builder.lwnScn == t1Scn);
    check("T2 continues the index after T1", builder.lwnIdx > t1LastIdx);

    // A client that received all of T1 gets T2
    metadata.clientScn = t1Scn;
    metadata.clientIdx = t1LastIdx;
    check("T2 is new data for a client at T1's last position", metadata.isNewData(builder.lwnScn, builder.lwnIdx));

    // A checkpoint at a higher scn still moves the position forward
    builder.processCheckpoint(Seq(1), Scn(470), Time(0), FileOffset(0), false);
    check("checkpoint at a higher scn moves the position forward", builder.lwnScn == Scn(470));

    if (failures > 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
