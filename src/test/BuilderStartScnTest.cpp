/* Test for the SCN a message is sent with when the transaction began before the start SCN
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

// The client started the stream at SCN 150. Transaction T began at 100 with a change at 120, made a second change
// at 200 and committed at 300; it is sent in full since it committed after the start SCN. The begin and the first
// change must not be sent with an scn below 150: a client which started there drops messages with a lower scn
// (Debezium's skipToStartScn), the second change and the commit keep their own SCN.
namespace {
    int failures = 0;

    void check(const std::string& name, bool ok) {
        if (!ok) {
            std::cerr << "FAIL " << name << "\n";
            ++failures;
        } else
            std::cout << "ok   " << name << "\n";
    }

    OpenLogReplicator::Format makeFormat(OpenLogReplicator::Format::SCN_TYPE scnType) {
        using namespace OpenLogReplicator;
        return {Format::DB_FORMAT::DEFAULT, Format::ATTRIBUTES_FORMAT::DEFAULT, Format::INTERVAL_DTS_FORMAT::UNIX_NANO,
                Format::INTERVAL_YTM_FORMAT::MONTHS, Format::MESSAGE_FORMAT::DEFAULT, Format::RID_FORMAT::SKIP,
                Format::REDO_THREAD_FORMAT::SKIP, Format::XID_FORMAT::TEXT_HEX, Format::TIMESTAMP_FORMAT::UNIX_NANO,
                Format::TIMESTAMP_FORMAT::UNIX_NANO, Format::TIMESTAMP_TZ_FORMAT::UNIX_NANO_STRING, Format::TIMESTAMP_TYPE::DEFAULT,
                Format::CHAR_FORMAT::UTF8, Format::SCN_FORMAT::NUMERIC, scnType, Format::UNKNOWN_FORMAT::QUESTION_MARK,
                Format::SCHEMA_FORMAT::DEFAULT, Format::COLUMN_FORMAT::CHANGED, Format::UNKNOWN_TYPE::HIDE, Format::USER_TYPE::DEFAULT};
    }
}

int main() {
    using namespace OpenLogReplicator;

    Ctx ctx;
    Locales locales;
    Metadata metadata(&ctx, &locales, "TEST", Scn::zero(), Seq::zero(), "", 0);
    AttributeMap attributes;
    attributes[Attribute::KEY::LOGIN_USER_NAME] = "TEST";
    const Xid t(1, 1, 1);

    {
        Format format = makeFormat(Format::SCN_TYPE::DML);
        BuilderJson builder(&ctx, &locales, &metadata, format, 0);
        builder.processBegin(t, 1, Seq(4), Scn(100), Time(0), Seq(6), Scn(300), Time(0), &attributes);

        // No start SCN known yet (e.g. a batch run): the scn of the redo record is sent as is
        metadata.firstDataScn = Scn::none();
        check("no start scn: begin keeps its scn", builder.outputScn(Scn(100)) == Scn(100));
        check("no start scn: change keeps its scn", builder.outputScn(Scn(120)) == Scn(120));

        // Stream started at 150, the transaction straddles it
        metadata.firstDataScn = Scn(150);
        check("begin before start is sent with the start scn", builder.outputScn(Scn(100)) == Scn(150));
        check("change before start is sent with the start scn", builder.outputScn(Scn(120)) == Scn(150));
        check("change at start keeps its scn", builder.outputScn(Scn(150)) == Scn(150));
        check("change after start keeps its scn", builder.outputScn(Scn(200)) == Scn(200));
        check("commit keeps its scn", builder.outputScn(Scn(300)) == Scn(300));

        // Stream started at 0 (no filter): nothing to raise
        metadata.firstDataScn = Scn::zero();
        check("start scn 0: change keeps its scn", builder.outputScn(Scn(120)) == Scn(120));
    }

    {
        // scn-type COMMIT_VALUE: every message carries the commit SCN, which is above any start SCN the transaction
        // passed through
        Format format = makeFormat(Format::SCN_TYPE::COMMIT_VALUE);
        BuilderJson builder(&ctx, &locales, &metadata, format, 0);
        builder.processBegin(t, 1, Seq(4), Scn(100), Time(0), Seq(6), Scn(300), Time(0), &attributes);
        metadata.firstDataScn = Scn(150);
        check("commit value: begin is sent with the commit scn", builder.outputScn(Scn(100)) == Scn(300));
        check("commit value: change is sent with the commit scn", builder.outputScn(Scn(200)) == Scn(300));
    }

    if (failures > 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
