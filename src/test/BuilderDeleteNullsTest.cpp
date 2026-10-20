/* Test for the column bits a DELETE leaves behind in the builder
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

#include "../builder/Builder.h"
#include "../common/Ctx.h"
#include "../common/DbColumn.h"
#include "../common/DbTable.h"
#include "../common/Format.h"
#include "../common/RedoLogRecord.h"
#include "../common/Thread.h"
#include "../common/exception/RedoLogException.h"
#include "../common/table/SysCol.h"
#include "../locales/Locales.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"

// Redo does not store trailing NULL columns. For a DELETE the builder fills NULL for every column the record does not
// carry (all columns with column format 1, the primary key otherwise) and marks them in valuesSet. releaseValues()
// clears valuesSet only up to valuesMax, so a filled column must raise valuesMax. Otherwise a DELETE of a row whose
// stored columns end below a 64-column boundary leaves the bits of the filled columns above it set, and the next DML
// iterates them: an UPDATE on a table with fewer columns stops with ERROR 50073 "missmatch in column details", and on
// a wider table the leaked columns are sent as NULL before-images.
using namespace OpenLogReplicator;

namespace {
    int failures = 0;

    void check(const std::string& name, bool ok) {
        if (!ok) {
            std::cerr << "FAIL " << name << "\n";
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

    // Records the columns handed to processDelete/processUpdate, writes no output
    class TestBuilder final : public Builder {
    public:
        uint64_t deletes{0};
        uint64_t updates{0};

        TestBuilder(Ctx* newCtx, Locales* newLocales, Metadata* newMetadata,
                    const Format& newFormat):
                Builder(newCtx, newLocales, newMetadata, newFormat, 0) {}

        // Number of columns still marked as set
        [[nodiscard]] uint64_t columnsSet() const {
            uint64_t count = 0;
            for (const auto set: valuesSet)
                count += __builtin_popcountll(set);
            return count;
        }

        // A value the redo of an UPDATE carries for a column, as Builder::processDml stores it
        void setValue(Format::VALUE_TYPE type, uint16_t column) {
            static constexpr uint8_t data[1]{0x41};
            valueSet(type, column, data, 1, 0, false);
        }

    protected:
        void columnFloat(const std::string&, double) override {}
        void columnDouble(const std::string&, long double) override {}
        void columnString(const std::string&) override {}
        void columnNumber(const std::string&, int, int) override {}
        void columnRaw(const std::string&, const uint8_t*, uint64_t) override {}
        void columnRowId(const std::string&, RowId) override {}
        void columnTimestamp(const std::string&, time_t, uint64_t) override {}
        void columnTimestampTz(const std::string&, time_t, uint64_t, const std::string_view&) override {}
        void processInsert(Seq, Scn, Time, LobCtx*, const XmlCtx*, const DbTable*, typeObj, typeDataObj, typeDba, typeSlot, FileOffset) override {}
        void processUpdate(Seq, Scn, Time, LobCtx*, const XmlCtx*, const DbTable*, typeObj, typeDataObj, typeDba, typeSlot, FileOffset) override {
            ++updates;
        }
        void processDelete(Seq, Scn, Time, LobCtx*, const XmlCtx*, const DbTable*, typeObj, typeDataObj, typeDba, typeSlot, FileOffset) override {
            ++deletes;
        }
        void processDdl(Seq, Scn, Time, const DbTable*, typeObj) override {}
        void processBeginMessage(Seq, Time) override {}

    public:
        void processCommit() override {}
        void processCheckpoint(Seq, Scn, Time, FileOffset, bool) override {}
    };

    // A table with `columns` VARCHAR2 columns, the primary key is column `pkColumn` (0-based)
    DbTable* makeTable(typeObj obj, const std::string& name, typeCol columns, typeCol pkColumn) {
        auto* table = new DbTable(obj, obj, 100, 0, DbTable::OPTIONS::DEFAULT, "TEST", name);
        for (typeCol i = 0; i < columns; ++i)
            table->addColumn(new DbColumn(i + 1, -1, i + 1, "C" + std::to_string(i + 1), SysCol::COLTYPE::VARCHAR, 10, -1, -1,
                                          0, i == pkColumn ? 1 : 0, i != pkColumn, false, false, false, false, false, false, false, false));
        return table;
    }

    // An undo/redo pair of a row change without column data: the row ends before the first column (all NULL)
    struct RowChange {
        RedoLogRecord undo{};
        RedoLogRecord redo{};
        std::deque<const RedoLogRecord*> redo1;
        std::deque<const RedoLogRecord*> redo2;

        explicit RowChange(typeObj obj) {
            undo.obj = obj;
            undo.dataObj = obj;
            redo.obj = obj;
            redo.dataObj = obj;
            redo.fb = RedoLogRecord::FB_F | RedoLogRecord::FB_L;
            redo1.push_back(&undo);
            redo2.push_back(&redo);
        }
    };

    Format makeFormat(Format::COLUMN_FORMAT columnFormat) {
        return {Format::DB_FORMAT::DEFAULT, Format::ATTRIBUTES_FORMAT::DEFAULT, Format::INTERVAL_DTS_FORMAT::UNIX_NANO,
                Format::INTERVAL_YTM_FORMAT::MONTHS, Format::MESSAGE_FORMAT::DEFAULT, Format::RID_FORMAT::SKIP,
                Format::REDO_THREAD_FORMAT::SKIP, Format::XID_FORMAT::TEXT_HEX, Format::TIMESTAMP_FORMAT::UNIX_NANO,
                Format::TIMESTAMP_FORMAT::UNIX_NANO, Format::TIMESTAMP_TZ_FORMAT::UNIX_NANO_STRING, Format::TIMESTAMP_TYPE::DEFAULT,
                Format::CHAR_FORMAT::UTF8, Format::SCN_FORMAT::NUMERIC, Format::SCN_TYPE::DEFAULT, Format::UNKNOWN_FORMAT::QUESTION_MARK,
                Format::SCHEMA_FORMAT::DEFAULT, columnFormat, Format::UNKNOWN_TYPE::HIDE, Format::USER_TYPE::DEFAULT};
    }

    // DELETE on `wide` (65 columns), then an UPDATE of column 64 (0-based 63) on `narrow` (64 columns)
    void run(const std::string& label, Format::COLUMN_FORMAT columnFormat, typeCol widePk) {
        Ctx ctx;
        TestThread thread(&ctx);
        ctx.parserThread = &thread;
        ctx.initialize(32, 64, 16, 16, 0, 0, 16, 16);
        Locales locales;
        Metadata metadata(&ctx, &locales, "TEST", Scn::zero(), Seq::zero(), "", 0);
        // What Schema::buildMaps does for a table in the filter: Builder::processDml looks it up by obj
        const std::unique_ptr<DbTable> wide(makeTable(1001, "WIDE", 65, widePk));
        const std::unique_ptr<DbTable> narrow(makeTable(1002, "NARROW", 64, 0));
        metadata.schema->tablePartitionMap.insert_or_assign(wide->obj, wide.get());
        metadata.schema->tablePartitionMap.insert_or_assign(narrow->obj, narrow.get());
        TestBuilder builder(&ctx, &locales, &metadata, makeFormat(columnFormat));
        builder.initialize();

        const RowChange del(1001);
        builder.processDml(Seq(1), Scn(100), Time(0), nullptr, nullptr, del.redo1, del.redo2, Format::TRANSACTION_TYPE::DELETE, false, false,
                           false);
        check(label + ": the DELETE is sent", builder.deletes == 1);
        check(label + ": no column is left set after the DELETE", builder.columnsSet() == 0);

        const RowChange upd(1002);
        builder.setValue(Format::VALUE_TYPE::BEFORE, 63);
        builder.setValue(Format::VALUE_TYPE::AFTER, 63);
        try {
            builder.processDml(Seq(1), Scn(100), Time(0), nullptr, nullptr, upd.redo1, upd.redo2, Format::TRANSACTION_TYPE::UPDATE, false, false,
                               false);
            check(label + ": an UPDATE of column 64 on a 64-column table follows", builder.updates == 1);
        } catch (RedoLogException& ex) {
            std::cerr << "     " << ex.code << " " << ex.msg << "\n";
            check(label + ": an UPDATE of column 64 on a 64-column table follows", false);
        }
        check(label + ": no column is left set after the UPDATE", builder.columnsSet() == 0);
        metadata.schema->tablePartitionMap.clear();
    }
}

int main() {
    // column format 1: NULL is filled for every column the DELETE does not carry, column 65 included
    run("all columns", Format::COLUMN_FORMAT::FULL_INS_DEC, 0);
    // column format 0: NULL is filled for primary key columns only, here column 65
    run("primary key", Format::COLUMN_FORMAT::CHANGED, 64);

    if (failures > 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
