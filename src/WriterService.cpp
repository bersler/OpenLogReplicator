/* Thread writing to GRPC
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "WriterService.h"

using namespace std;

namespace OpenLogReplicator {

    WriterService::WriterService(const char *alias, OracleAnalyzer *oracleAnalyzer, const char *uri, uint64_t pollInterval,
            uint64_t checkpointInterval, uint64_t queueSize, typescn startScn, typeseq startSeq, const char* startTime,
            uint64_t startTimeRel) :
        Writer(alias, oracleAnalyzer, 0, pollInterval, checkpointInterval, queueSize, startScn, startSeq, startTime, startTimeRel),
        uri(uri) {

#ifdef LINK_LIBRARY_GRPC
        builder.AddListeningPort(uri, InsecureServerCredentials());
        builder.RegisterService(&service_);
        cq_ = builder.AddCompletionQueue();
        server_ = builder.BuildAndStart();
        cout << "GRPC server listening on " << uri << endl;
#endif /* LINK_LIBRARY_GRPC */
    }

    WriterService::~WriterService() {
#ifdef LINK_LIBRARY_GRPC
        server_->Shutdown();
        cq_->Shutdown();

        void* ignored_tag;
        bool ignored_ok;
        while (cq_->Next(&ignored_tag, &ignored_ok)) {}
#endif /* LINK_LIBRARY_GRPC */
    }

    void WriterService::sendMessage(OutputBufferMsg *msg) {
    }

    string WriterService::getName() {
        return "Service:" + uri;
    }

    void WriterService::pollQueue(void) {
    }
}
