/* Test for the scn of the messages a transaction straddling the start SCN is sent with
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

#include <deque>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "../builder/BuilderJson.h"
#include "../common/Attribute.h"
#include "../common/Ctx.h"
#include "../common/DbColumn.h"
#include "../common/DbTable.h"
#include "../common/Format.h"
#include "../common/RedoLogRecord.h"
#include "../common/Thread.h"
#include "../common/table/SysCol.h"
#include "../locales/Locales.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"

// The stream was started at SCN 150. Transaction T began at 100, inserted a row at 120 and one at 200, and committed
// at 300; it is sent in full because it committed after the start SCN. A client which started at 150 without an index
// drops messages with a lower scn (Debezium's skipToStartScn), so no message may carry an scn below 150: the begin and
// the first insert are sent with 150, the second insert and the commit keep their SCN. Only the builders' public
// interface is used.
using namespace OpenLogReplicator;

namespace {
    int failures = 0;

    void check(const std::string& name, const std::string& actual, const std::string& expected) {
        if (actual != expected) {
            std::cerr << "FAIL " << name << ": got " << actual << ", expected " << expected << "\n";
            ++failures;
        } else
            std::cout << "ok   " << name << "\n";
    }

    class TestThread final : public Thread {
    public:
        explicit TestThread(Ctx* newCtx): Thread(newCtx, "test") {}
        void run() override {}
        std::string getName() const override { return "test"; }
    };

    Format makeFormat(Format::XID_FORMAT xidFormat, Format::SCN_TYPE scnType) {
        return {Format::DB_FORMAT::DEFAULT, Format::ATTRIBUTES_FORMAT::DEFAULT, Format::INTERVAL_DTS_FORMAT::UNIX_NANO,
                Format::INTERVAL_YTM_FORMAT::MONTHS, Format::MESSAGE_FORMAT::DEFAULT, Format::RID_FORMAT::SKIP,
                Format::REDO_THREAD_FORMAT::SKIP, xidFormat, Format::TIMESTAMP_FORMAT::UNIX_NANO,
                Format::TIMESTAMP_FORMAT::UNIX_NANO, Format::TIMESTAMP_TZ_FORMAT::UNIX_NANO_STRING, Format::TIMESTAMP_TYPE::DEFAULT,
                Format::CHAR_FORMAT::UTF8, Format::SCN_FORMAT::NUMERIC, scnType, Format::UNKNOWN_FORMAT::QUESTION_MARK,
                Format::SCHEMA_FORMAT::DEFAULT, Format::COLUMN_FORMAT::CHANGED, Format::UNKNOWN_TYPE::HIDE, Format::USER_TYPE::DEFAULT};
    }

    // Table TEST.T (obj 1001) with one VARCHAR2 primary key column
    DbTable* makeTable() {
        auto* table = new DbTable(1001, 1001, 100, 0, DbTable::OPTIONS::DEFAULT, "TEST", "T");
        table->addColumn(new DbColumn(1, -1, 1, "ID", SysCol::COLTYPE::VARCHAR, 10, -1, -1, 0, 1, false, false, false, false, false, false,
                                      false, false, false));
        return table;
    }

    // Undo/redo pair of an INSERT into TEST.T without column data
    struct Insert {
        RedoLogRecord undo{};
        RedoLogRecord redo{};
        std::deque<const RedoLogRecord*> redo1;
        std::deque<const RedoLogRecord*> redo2;

        Insert() {
            undo.obj = 1001;
            undo.dataObj = 1001;
            redo.obj = 1001;
            redo.dataObj = 1001;
            redo.fb = RedoLogRecord::FB_F | RedoLogRecord::FB_L;
            redo1.push_back(&undo);
            redo2.push_back(&redo);
        }
    };

    struct Message {
        Scn scn;
        std::string json;
    };

    // Every message the builder queued, in order
    std::vector<Message> messages(const Builder& builder) {
        std::vector<Message> result;
        for (const BuilderQueue* queue = builder.firstBuilderQueue; queue != nullptr; queue = queue->next) {
            uint64_t pos = 0;
            while (pos < queue->confirmedSize) {
                const auto* msg = reinterpret_cast<const BuilderMsg*>(queue->data + pos);
                result.push_back({msg->scn, std::string(reinterpret_cast<const char*>(msg->data), msg->size)});
                pos = static_cast<uint64_t>(msg->data - queue->data) + ((msg->size + 7) & 0xFFFFFFFFFFFFFFF8);
            }
        }
        return result;
    }

    // Value of a top-level field: "key":"text" or "key":number
    std::string field(const std::string& json, const std::string& key) {
        const std::string k = "\"" + key + "\":";
        const std::string::size_type pos = json.find(k);
        if (pos == std::string::npos)
            return "<missing>";
        std::string::size_type start = pos + k.length();
        if (json[start] == '"')
            return json.substr(start + 1, json.find('"', start + 1) - start - 1);
        std::string::size_type end = start;
        while (end < json.size() && json[end] != ',' && json[end] != '}')
            ++end;
        return json.substr(start, end - start);
    }

    // BuilderMsg::scn and the "scn" field of every message of T
    std::vector<Message> transaction(Scn startScn, Format::SCN_TYPE scnType) {
        Ctx ctx;
        TestThread thread(&ctx);
        ctx.parserThread = &thread;
        ctx.initialize(32, 64, 16, 16, 0, 0, 16, 16);
        Locales locales;
        Metadata metadata(&ctx, &locales, "TEST", Scn::zero(), Seq::zero(), "", 0);
        metadata.firstDataScn = startScn;
        const std::unique_ptr<DbTable> table(makeTable());
        metadata.schema->tablePartitionMap.insert_or_assign(table->obj, table.get());
        Format format = makeFormat(Format::XID_FORMAT::TEXT_HEX, scnType);
        BuilderJson builder(&ctx, &locales, &metadata, format, 0);
        builder.initialize();

        AttributeMap attributes;
        attributes[Attribute::KEY::LOGIN_USER_NAME] = "TEST";
        const Insert insert;
        builder.processBegin(Xid(1, 1, 1), 1, Seq(4), Scn(100), Time(0), Seq(6), Scn(300), Time(0), &attributes);
        builder.processDml(Seq(4), Scn(120), Time(0), nullptr, nullptr, insert.redo1, insert.redo2, Format::TRANSACTION_TYPE::INSERT, false,
                           false, false);
        builder.processDml(Seq(5), Scn(200), Time(0), nullptr, nullptr, insert.redo1, insert.redo2, Format::TRANSACTION_TYPE::INSERT, false,
                           false, false);
        builder.processCommit();
        metadata.schema->tablePartitionMap.clear();
        return messages(builder);
    }

    void checkScns(const std::string& name, const std::vector<Message>& sent, const std::vector<std::string>& expected) {
        static const char* const what[]{"begin", "insert at 120", "insert at 200", "commit"};
        check(name + ": messages sent", std::to_string(sent.size()), std::to_string(expected.size()));
        for (uint64_t i = 0; i < sent.size() && i < expected.size(); ++i) {
            check(name + ": " + what[i] + " scn field", field(sent[i].json, "scn"), expected[i]);
            check(name + ": " + what[i] + " message scn", sent[i].scn.toString(), expected[i]);
        }
    }
}

int main() {
    // scn-type 4 (DML): every message has an "scn" field, not only the first one of a transaction
    checkScns("started at 150", transaction(Scn(150), Format::SCN_TYPE::DML), {"150", "150", "200", "300"});
    // Started at 0 or without a start SCN (batch): every message keeps the scn of its redo record
    checkScns("started at 0", transaction(Scn::zero(), Format::SCN_TYPE::DML), {"100", "120", "200", "300"});
    checkScns("no start scn", transaction(Scn::none(), Format::SCN_TYPE::DML), {"100", "120", "200", "300"});
    // scn-type COMMIT_VALUE: every message carries the commit SCN
    checkScns("commit value", transaction(Scn(150), static_cast<Format::SCN_TYPE>(static_cast<uint>(Format::SCN_TYPE::COMMIT_VALUE) | static_cast<uint>(Format::SCN_TYPE::DML))), {"300", "300", "300", "300"});

    if (failures > 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
