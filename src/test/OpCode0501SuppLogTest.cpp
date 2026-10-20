/* Test for the fields of a KDO undo record (5.1, opc 11.1) before the supplemental log
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

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "../common/Ctx.h"
#include "../common/RedoLogRecord.h"
#include "../common/exception/RedoLogException.h"
#include "../parser/OpCode0501.h"

// Two undo records with a field between the KDO header and the supplemental log; reading the supplemental log from
// that field fails with ERROR 50061 "too short field supplemental log":
//
// 1. SELECT ... FOR UPDATE on a table created with ROWDEPENDENCIES: KDO op LKR with row dependencies enabled. Like the
//    IRP, DRP and URP undo of such a table it carries the row's dependent SCN in an own field (8 bytes) before the
//    supplemental log fields.
// 2. A row piece without columns: the head piece of a migrated row (fb H, cc 0) only points at the next piece. Its
//    KDO op IRP/ORP undo still has the row data field (flags, lock, column count, next rowid: 9 bytes, the size in
//    the KDO header) before the supplemental log fields. Bulk inserts into a ROW STORE COMPRESS ADVANCED table produce it.
namespace {
    int failures = 0;

    void check(const std::string& name, bool ok) {
        if (!ok) {
            std::cerr << "FAIL " << name << "\n";
            ++failures;
        } else
            std::cout << "ok   " << name << "\n";
    }

    // Change vector built from its fields: the field size array first (one unused slot, then one 16-bit size per
    // field), then the fields, each padded to 4 bytes, the way RedoLogRecord::nextField walks them.
    class Vector {
    public:
        std::vector<uint8_t> bytes;
        OpenLogReplicator::RedoLogRecord record{};

        explicit Vector(const std::vector<std::vector<uint8_t>>& fields) {
            const size_t sizesBytes = (fields.size() + 1) * 2;
            const size_t fieldPos = (sizesBytes + 3) & ~static_cast<size_t>(3);
            bytes.assign(fieldPos, 0);
            for (size_t i = 0; i < fields.size(); ++i) {
                const uint16_t size = static_cast<uint16_t>(fields[i].size());
                bytes[(i + 1) * 2] = static_cast<uint8_t>(size & 0xFF);
                bytes[(i + 1) * 2 + 1] = static_cast<uint8_t>(size >> 8);
                bytes.insert(bytes.end(), fields[i].begin(), fields[i].end());
                bytes.resize((bytes.size() + 3) & ~static_cast<size_t>(3), 0);
            }
            record.dataExt = bytes.data();
            record.size = static_cast<uint32_t>(bytes.size());
            record.fieldCnt = static_cast<uint16_t>(fields.size());
            record.fieldSizesDelta = 0;
            record.fieldPos = static_cast<uint16_t>(fieldPos);
            record.opCode = 0x0501;
        }
    };

    std::vector<uint8_t> le16(std::vector<uint8_t> field, size_t at, uint16_t value) {
        field[at] = static_cast<uint8_t>(value & 0xFF);
        field[at + 1] = static_cast<uint8_t>(value >> 8);
        return field;
    }

    // ktudb, ktub (opc 11.1), ktb redo: the fields every KDO undo record starts with
    std::vector<std::vector<uint8_t>> undoHeader() {
        std::vector<std::vector<uint8_t>> fields;
        fields.emplace_back(20, 0);                                          // ktudb
        std::vector<uint8_t> ktub(24, 0);
        ktub[0] = 1;                                                         // obj
        ktub[4] = 1;                                                         // data obj
        ktub[16] = 0x0B;                                                     // opc 11.1
        ktub[17] = 0x01;
        fields.push_back(ktub);
        fields.emplace_back(4, 0);                                           // ktb redo (nothing to parse)
        return fields;
    }

    // Supplemental log header with two columns, their numbers, sizes and values
    void suppLogFields(std::vector<std::vector<uint8_t>>& fields) {
        std::vector<uint8_t> supp(20, 0);
        supp[1] = 0x2C;                                                      // fb
        supp = le16(supp, 2, 2);                                             // cc: 2 columns
        supp = le16(supp, 6, 1);                                             // before
        supp = le16(supp, 8, 1);                                             // after
        fields.push_back(supp);
        fields.push_back(le16(le16(std::vector<uint8_t>(4, 0), 0, 1), 2, 2)); // column numbers 1, 2
        fields.push_back(le16(le16(std::vector<uint8_t>(4, 0), 0, 2), 2, 3)); // column sizes 2, 3
        fields.push_back({0xC1, 0x02});                                      // column 1
        fields.push_back({0x61, 0x62, 0x63});                                // column 2
    }

    // Undo of a row lock: header, KDO LKR, [row dependencies], supplemental log
    Vector lockRowUndo(bool rowDependencies) {
        std::vector<std::vector<uint8_t>> fields = undoHeader();
        std::vector<uint8_t> kdo(20, 0);
        kdo[10] = OpenLogReplicator::RedoLogRecord::OP_LKR | (rowDependencies ? OpenLogReplicator::RedoLogRecord::OP_ROWDEPENDENCIES : 0);
        fields.push_back(le16(kdo, 16, 7));                                  // slot 7
        if (rowDependencies)
            fields.emplace_back(8, 0);                                       // dependent scn of the row
        suppLogFields(fields);
        return Vector(fields);
    }

    // Undo of a head piece without columns: header, KDO IRP (fb H, cc 0, size 9), the 9-byte row piece, supplemental log
    Vector headPieceUndo(bool withRowPiece) {
        std::vector<std::vector<uint8_t>> fields = undoHeader();
        std::vector<uint8_t> kdo(48, 0);
        kdo[10] = OpenLogReplicator::RedoLogRecord::OP_IRP;
        kdo[16] = OpenLogReplicator::RedoLogRecord::FB_H;                  // fb
        kdo[18] = 0;                                                         // cc
        kdo = le16(kdo, 40, withRowPiece ? 9 : 0);                           // size/delt
        kdo = le16(kdo, 42, 591);                                            // slot
        fields.push_back(kdo);
        if (withRowPiece)
            fields.push_back({0x20, 0x02, 0x00, 0x06, 0x00, 0x00, 0x3F, 0x00, 0x8E}); // fb, lb, cc, next rowid
        suppLogFields(fields);
        return Vector(fields);
    }

    bool process(OpenLogReplicator::Ctx& ctx, Vector& v, std::string& error) {
        try {
            OpenLogReplicator::OpCode0501::process0501(&ctx, &v.record);
            return true;
        } catch (OpenLogReplicator::RedoLogException& ex) {
            error = std::to_string(ex.code) + " " + ex.msg;
            return false;
        }
    }

    void checkSuppLog(const Vector& v, uint16_t rowDataField) {
        check("  supplemental log columns", v.record.suppLogCC == 2 && v.record.suppLogBefore == 1 && v.record.suppLogAfter == 1);
        check("  supplemental row data starts at field " + std::to_string(rowDataField), v.record.suppLogRowData == rowDataField);
    }
}

int main() {
    using namespace OpenLogReplicator;

    Ctx ctx;
    ctx.version = RedoLogRecord::REDO_VERSION_19_0;
    std::string error;

    {
        Vector v = lockRowUndo(false);
        check("lock row without row dependencies parses", process(ctx, v, error) || (std::cerr << error << "\n", false));
        check("  slot", v.record.slot == 7);
        checkSuppLog(v, 8);
    }

    {
        Vector v = lockRowUndo(true);
        check("lock row with row dependencies parses", process(ctx, v, error) || (std::cerr << error << "\n", false));
        check("  slot", v.record.slot == 7);
        checkSuppLog(v, 9);
    }

    {
        Vector v = headPieceUndo(false);
        check("insert of an empty row piece without row data parses", process(ctx, v, error) || (std::cerr << error << "\n", false));
        check("  slot", v.record.slot == 591);
        check("  no row data", v.record.rowData == 0 && !v.record.compressed);
        checkSuppLog(v, 8);
    }

    {
        Vector v = headPieceUndo(true);
        check("insert of a head piece pointing at the next piece parses", process(ctx, v, error) || (std::cerr << error << "\n", false));
        check("  slot", v.record.slot == 591);
        check("  no columns to read", v.record.rowData == 0 && !v.record.compressed);
        checkSuppLog(v, 9);
    }

    if (failures > 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all checks passed\n";
    return 0;
}
