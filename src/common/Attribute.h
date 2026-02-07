/* Header for Transaction attributes
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

#ifndef ATTRIBUTE_H_
#define ATTRIBUTE_H_

#include <map>
#include <string_view>
#include <unordered_map>
#include <string>
#include <vector>

namespace OpenLogReplicator {
    class Attribute final {
    public:
        enum class KEY : unsigned char {
            VERSION,
            AUDIT_SESSION_ID,
            SESSION_NUMBER,
            SERIAL_NUMBER,
            CURRENT_USER_NAME,
            LOGIN_USER_NAME,
            CLIENT_INFO,
            OS_USER_NAME,
            MACHINE_NAME,
            OS_TERMINAL,
            OS_PROCESS_ID,
            OS_PROGRAM_NAME,
            TRANSACTION_NAME,
            CLIENT_ID,
            DDL_TRANSACTION,
            SPACE_MANAGEMENT_TRANSACTION,
            RECURSIVE_TRANSACTION,
            LOGMINER_INTERNAL_TRANSACTION,
            DB_OPEN_IN_MIGRATE_MODE,
            LSBY_IGNORE,
            LOGMINER_NO_TX_CHUNKING,
            LOGMINER_STEALTH_TRANSACTION,
            LSBY_PRESERVE,
            LOGMINER_MARKER_TRANSACTION,
            TRANSACTION_IN_PRAGMAED_PLSQL,
            DISABLED_LOGICAL_REPLICATION_TRANSACTION,
            DATAPUMP_IMPORT_TRANSACTION,
            TRANSACTION_AUDIT_CV_FLAGS_UNDEFINED,
            FEDERATION_PDB_REPLAY,
            PDB_DDL_REPLAY,
            LOGMINER_SKIP_TRANSACTION,
            SEQ_UPDATE_TRANSACTION
        };

        static std::string_view toString(KEY attributeName) {
            const auto& N = names();
            const size_t idx = static_cast<size_t>(attributeName);
            if (idx < N.size()) return N[idx];
            return std::string_view("UNKNOWN");
        }

        static const std::unordered_map<std::string_view, KEY>& fromString() {
            static const std::unordered_map<std::string_view, KEY> M = []{
                std::unordered_map<std::string_view, KEY> tmp;
                const auto& N = names();
                for (size_t i = 0; i < N.size(); ++i) {
                    tmp.emplace(N[i], static_cast<KEY>(i));
                }
                return tmp;
            }();
            return M;
        }

    private:
        static const std::vector<std::string_view>& names() {
            static const std::vector<std::string_view> N = {
                "version",
                "audit session id",
                "session number",
                "serial number",
                "current user name",
                "login username",
                "client info",
                "os username",
                "machine name",
                "os terminal",
                "os process id",
                "os program name",
                "transaction name",
                "client id",
                "ddl transaction",
                "space management transaction",
                "recursive transaction",
                "logminer internal transaction",
                "db open in migrate mode",
                "lsby ignore",
                "logminer no transaction chunking",
                "logminer stealth transaction",
                "lsby preserve",
                "logminer marker transaction",
                "transaction in pragma'ed plsql",
                "disabled logical replication transaction",
                "datapump import transaction",
                "transaction audit CV flags undefined",
                "federation pdb replay",
                "pdb ddl replay",
                "logminer skip transaction",
                "seq$ update transaction"
            };
            return N;
        }
    };

    typedef std::map<Attribute::KEY, std::string> AttributeMap;

}
#endif
