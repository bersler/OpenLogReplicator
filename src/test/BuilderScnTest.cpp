/* Test for the message position (c_scn, c_idx) given by the builder
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
#include "../locales/Locales.h"
#include "../metadata/Metadata.h"

// Two transactions: T1 begins at SCN 100 and commits at 300, T2 begins at 200 and commits at 210. T2 is emitted first
// and the client confirms its position, then T1 is emitted. The writer sends only messages which are new for the
// client (Metadata::isNewData), so the position of T1 must be above the position of T2 or T1 is lost.
namespace {
    int failures = 0;

    void check(const std::string& name, bool ok) {
        if (!ok) {
            std::cerr << "FAIL " << name << "\n";
            ++failures;
        } else
            std::cout << "ok   " << name << "\n";
    }
}

int main() {
    using namespace OpenLogReplicator;

    Ctx ctx;
    Locales locales;
    Metadata metadata(&ctx, &locales, "TEST", Scn::zero(), Seq::zero(), "", 0);
    Format format(Format::DB_FORMAT::DEFAULT, Format::ATTRIBUTES_FORMAT::DEFAULT, Format::INTERVAL_DTS_FORMAT::UNIX_NANO,
                  Format::INTERVAL_YTM_FORMAT::MONTHS, Format::MESSAGE_FORMAT::DEFAULT, Format::RID_FORMAT::SKIP,
                  Format::REDO_THREAD_FORMAT::SKIP, Format::XID_FORMAT::TEXT_HEX, Format::TIMESTAMP_FORMAT::UNIX_NANO,
                  Format::TIMESTAMP_FORMAT::UNIX_NANO, Format::TIMESTAMP_TZ_FORMAT::UNIX_NANO_STRING, Format::TIMESTAMP_TYPE::DEFAULT,
                  Format::CHAR_FORMAT::UTF8, Format::SCN_FORMAT::NUMERIC, Format::SCN_TYPE::DEFAULT, Format::UNKNOWN_FORMAT::QUESTION_MARK,
                  Format::SCHEMA_FORMAT::DEFAULT, Format::COLUMN_FORMAT::CHANGED, Format::UNKNOWN_TYPE::HIDE, Format::USER_TYPE::DEFAULT);
    BuilderJson builder(&ctx, &locales, &metadata, format, 0);

    AttributeMap attributes;
    attributes[Attribute::KEY::LOGIN_USER_NAME] = "TEST";
    const Xid t1(1, 1, 1);
    const Xid t2(2, 2, 2);

    // T2 commits first and is emitted
    builder.processBegin(t2, 1, Seq(5), Scn(200), Time(0), Seq(5), Scn(210), Time(0), &attributes);
    check("T2 position is its commit scn", builder.lwnScn == Scn(210));
    check("T2 position index starts at 0", builder.lwnIdx == 0);
    const Scn t2Position = builder.lwnScn;
    builder.lwnIdx = 3;

    // Client confirmed everything up to T2 and continues from there
    metadata.clientScn = t2Position;
    metadata.clientIdx = 3;

    // T1 commits later, its begin precedes the client's position
    builder.processBegin(t1, 1, Seq(4), Scn(100), Time(0), Seq(6), Scn(300), Time(0), &attributes);
    check("T1 position is its commit scn", builder.lwnScn == Scn(300));
    check("T1 position index restarts", builder.lwnIdx == 0);
    check("T1 position grows after T2", t2Position < builder.lwnScn);
    check("T1 is new data for the client", metadata.isNewData(builder.lwnScn, builder.lwnIdx));

    // A transaction committed at the same scn continues the index instead of restarting it
    builder.lwnIdx = 7;
    builder.processBegin(Xid(3, 3, 3), 1, Seq(6), Scn(290), Time(0), Seq(6), Scn(300), Time(0), &attributes);
    check("same commit scn keeps the position", builder.lwnScn == Scn(300));
    check("same commit scn continues the index", builder.lwnIdx == 7);
    check("same commit scn is new data for a client at (300, 6)", (metadata.clientScn = Scn(300), metadata.clientIdx = 6,
                                                                   metadata.isNewData(builder.lwnScn, builder.lwnIdx)));
    check("same commit scn is not new data for a client at (300, 7)", (metadata.clientIdx = 7, !metadata.isNewData(builder.lwnScn, builder.lwnIdx)));

    if (failures > 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
