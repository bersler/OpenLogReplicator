/* Definition of pack template class
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

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

#ifndef TABLE_PACK_H_
#define TABLE_PACK_H_

#include <map>
#include <memory>
#include <set>
#include <unordered_map>

#include "../exception/RuntimeException.h"
#include "../types/FileOffset.h"
#include "../types/RowId.h"
#include "../types/Types.h"

namespace OpenLogReplicator {
    template<class Data, class KeyMap = TabRowIdKeyDefault, class KeyUnorderedMap = TabRowIdUnorderedKeyDefault>
    class TablePack final {
    public:
        std::map<RowId, Data*> mapRowId;
        std::map<KeyMap, Data*> mapKey;
        std::unordered_map<KeyUnorderedMap, Data*> unorderedMapKey;
        std::set<Data*> setTouched;

        [[nodiscard]] Data* forUpdate(const Ctx* ctx, RowId rowId, FileOffset fileOffset) {
            auto mapRowIdIt = mapRowId.find(rowId);
            if (likely(mapRowIdIt != mapRowId.end())) {
                dropKeys(mapRowIdIt->second);
                return mapRowIdIt->second;
            }

            if (likely(!ctx->isFlagSet(Ctx::REDO_FLAGS::ADAPTIVE_SCHEMA))) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
                    ctx->logTrace(Ctx::TRACE::SYSTEM, "forUpdate: missing " + Data::tableName() + " (ROWID: " + rowId.toString() + ") for update at offset: " +
                                  fileOffset.toString());
                return nullptr;
            }
            auto data = new Data(rowId);
            mapRowId.insert_or_assign(rowId, data);
            return data;
        }

        void clear(const Ctx* ctx) {
            for (auto& [RowId, data]: mapRowId) {
                if constexpr (!std::is_same_v<KeyMap, TabRowIdKeyDefault>) {
                    KeyMap key(data);
                    const auto& it = mapKey.find(key);
                    if (unlikely(it == mapKey.end()))
                        ctx->warning(50030, "missing index for " + Data::tableName() + " (" + data->toString() + ")");
                    else
                        mapKey.erase(it);
                }

                if constexpr (!std::is_same_v<KeyUnorderedMap, TabRowIdUnorderedKeyDefault>) {
                    KeyUnorderedMap key(data);
                    const auto& it = unorderedMapKey.find(key);
                    if (unlikely(it == unorderedMapKey.end()))
                        ctx->warning(50030, "missing index for " + Data::tableName() + " (" + data->toString() + ")u");
                    else
                        unorderedMapKey.erase(it);
                }

                delete data;
            }
            mapRowId.clear();

            if constexpr (!std::is_same_v<KeyMap, TabRowIdKeyDefault>) {
                if (!mapKey.empty())
                    ctx->error(50029, "key map " + Data::tableName() + " not empty, left: " + std::to_string(mapKey.size()) + " at exit");
                mapKey.clear();
            }

            if constexpr (!std::is_same_v<KeyUnorderedMap, TabRowIdUnorderedKeyDefault>) {
                if (!unorderedMapKey.empty())
                    ctx->error(50029, "key map2 " + Data::tableName() + " not empty, left: " + std::to_string(unorderedMapKey.size()) + " at exit");
                unorderedMapKey.clear();
            }
        }

        bool compareTo(const TablePack& other, std::string& msgs) const {
            for (const auto& [_, data]: mapRowId) {
                const auto& it = other.mapRowId.find(data->rowId);
                if (it == other.mapRowId.end()) {
                    msgs.assign("schema mismatch: " + Data::tableName() + " lost ROWID: " + data->rowId.toString());
                    return false;
                }
                if (*data != *(it->second)) {
                    msgs.assign("schema mismatch: " + Data::tableName() + " differs ROWID: " + data->rowId.toString());
                    return false;
                }
            }

            for (const auto& [_, data]: other.mapRowId) {
                if (mapRowId.find(data->rowId) == mapRowId.end()) {
                    msgs.assign("schema mismatch: " + Data::tableName() + " lost ROWID: " + data->rowId.toString());
                    return false;
                }
            }
            return true;
        }

        Data* forInsert(const Ctx* ctx, RowId rowId, FileOffset fileOffset) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
                ctx->logTrace(Ctx::TRACE::SYSTEM, "forInsert " + Data::tableName() + " ('" + rowId.toString() + "')");

            const auto& it = mapRowId.find(rowId);
            Data* data;
            if (unlikely(it != mapRowId.end())) {
                // Duplicate
                data = it->second;
                if (unlikely(!ctx->isFlagSet(Ctx::REDO_FLAGS::ADAPTIVE_SCHEMA)))
                    throw RuntimeException(50022, "duplicate " + Data::tableName() + " (" + data->toString() + ") for insert at offset: " +
                                           fileOffset.toString());
                dropKeys(data);
            } else {
                data = new Data(rowId);
                mapRowId.insert_or_assign(rowId, data);
            }
            setTouched.insert(data);
            return data;
        }

        void add(const Ctx* ctx, Data* data) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
                ctx->logTrace(Ctx::TRACE::SYSTEM, "add: " + Data::tableName() + " (" + data->toString() + ")");

            const auto& it = mapRowId.find(data->rowId);
            if (unlikely(it != mapRowId.end()))
                throw RuntimeException(50022, "duplicate " + Data::tableName() + " (" + data->toString() + ") for insert");
            mapRowId.insert_or_assign(data->rowId, data);
            setTouched.insert(data);
        }

        void addWithKeys(const Ctx* ctx, Data* data) {
            add(ctx, data);
            addKeys(data);
        }

        void drop(const Ctx* ctx, RowId rowId, FileOffset fileOffset = FileOffset::zero(), bool deleteTouched = false) {
            const auto& it = mapRowId.find(rowId);
            if (unlikely(it == mapRowId.end())) {
                // Missing
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SYSTEM)))
                    ctx->logTrace(Ctx::TRACE::SYSTEM, "drop: missing " + Data::tableName() + " (ROWID: " + rowId.toString() + ") for delete at offset: " +
                                  fileOffset.toString());
                return;
            }

            if (deleteTouched)
                setTouched.erase(it->second);
            dropKeys(it->second);
            delete it->second;
            mapRowId.erase(it);
        }

        void addKeys(Data* data) {
            if constexpr (!std::is_same_v<KeyMap, TabRowIdKeyDefault>) {
                KeyMap key(data);
                const auto& it = mapKey.find(key);
                if (unlikely(it != mapKey.end()))
                    throw DataException(50024, "duplicate " + Data::tableName() + " value for unique (" + data->toString() + ")");
                mapKey.insert_or_assign(key, data);
            }

            if constexpr (!std::is_same_v<KeyUnorderedMap, TabRowIdUnorderedKeyDefault>) {
                KeyUnorderedMap key(data);
                const auto& it = unorderedMapKey.find(key);
                if (unlikely(it != unorderedMapKey.end()))
                    throw DataException(50024, "duplicate " + Data::tableName() + " value for unique (" + data->toString() + ")2");
                unorderedMapKey.insert_or_assign(key, data);
            }
        }

        void dropKeys(Data* data) {
            if constexpr (!std::is_same_v<KeyMap, TabRowIdKeyDefault>) {
                KeyMap key(data);
                const auto& it = mapKey.find(key);
                if (unlikely(it == mapKey.end()))
                    throw DataException(50030, "missing index for " + Data::tableName() + " value for unique (" + data->toString() + ")");
                mapKey.erase(it);
            }

            if constexpr (!std::is_same_v<KeyUnorderedMap, TabRowIdUnorderedKeyDefault>) {
                KeyUnorderedMap key(data);
                const auto& it = unorderedMapKey.find(key);
                if (unlikely(it == unorderedMapKey.end()))
                    throw DataException(50024, "missing index for " + Data::tableName() + " value for unique (" + data->toString() + ")2");
                unorderedMapKey.erase(it);
            }
        }
    };
}

#endif
