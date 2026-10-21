/* Test for the message index c_idx sent in the header and used to continue or confirm a stream
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
#include "../common/Ctx.h"
#include "../common/Format.h"
#include "../common/Thread.h"
#include "../common/types/FileOffset.h"
#include "../locales/Locales.h"
#include "../metadata/Metadata.h"

// A client stores the (c_scn, c_idx) of the last message it received and sends it back in CONTINUE and CONFIRM. The
// writer compares it with the message's own index (Metadata::isNewData, WriterStream::processConfirm), so the header
// must carry that index and not the index of the next message, or CONTINUE skips one message and CONFIRM releases one
// the client has not received. A client which sends c_idx 0 must get the whole scn again.
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

    uint64_t headerIdx(const OpenLogReplicator::BuilderMsg* msg) {
        const std::string json(reinterpret_cast<const char*>(msg->data), msg->size);
        const std::string key(R"("c_idx":)");
        const std::string::size_type pos = json.find(key);
        if (pos == std::string::npos)
            return 0xFFFFFFFFFFFFFFFF;
        return std::stoull(json.substr(pos + key.length()));
    }

    const OpenLogReplicator::BuilderMsg* nextMsg(const OpenLogReplicator::BuilderMsg* msg) {
        return reinterpret_cast<const OpenLogReplicator::BuilderMsg*>(msg->data + ((msg->size + 7) & 0xFFFFFFFFFFFFFFF8));
    }
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

    // Three messages: two at scn 500, one at scn 600
    builder.processCheckpoint(Seq(1), Scn(500), Time(0), FileOffset(0), false);
    builder.processCheckpoint(Seq(1), Scn(500), Time(0), FileOffset(0), false);
    builder.processCheckpoint(Seq(1), Scn(600), Time(0), FileOffset(0), false);

    const BuilderMsg* m1 = reinterpret_cast<const BuilderMsg*>(builder.firstBuilderQueue->data);
    const BuilderMsg* m2 = nextMsg(m1);
    const BuilderMsg* m3 = nextMsg(m2);

    check("first message of an scn has index 1", m1->lwnScn == Scn(500) && m1->lwnIdx == 1);
    check("second message of the scn has index 2", m2->lwnScn == Scn(500) && m2->lwnIdx == 2);
    check("index restarts at a new scn", m3->lwnScn == Scn(600) && m3->lwnIdx == 1);
    check("header c_idx is the index of the first message", headerIdx(m1) == m1->lwnIdx);
    check("header c_idx is the index of the second message", headerIdx(m2) == m2->lwnIdx);
    check("header c_idx is the index of the message at the new scn", headerIdx(m3) == m3->lwnIdx);

    // The client received the first message and continues with its (c_scn, c_idx)
    metadata.clientScn = m1->lwnScn;
    metadata.clientIdx = headerIdx(m1);
    check("the message the client received is not sent again", !metadata.isNewData(m1->lwnScn, m1->lwnIdx));
    check("the message after the one the client received is sent", metadata.isNewData(m2->lwnScn, m2->lwnIdx));

    // The client received the second message
    metadata.clientIdx = headerIdx(m2);
    check("the last message of the scn is not sent again", !metadata.isNewData(m2->lwnScn, m2->lwnIdx));
    check("the first message of the next scn is sent", metadata.isNewData(m3->lwnScn, m3->lwnIdx));

    // The client continues at (c_scn, 0): everything at that scn is sent again
    metadata.clientIdx = 0;
    check("c_idx 0 sends the whole scn again", metadata.isNewData(m1->lwnScn, m1->lwnIdx));

    if (failures > 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
