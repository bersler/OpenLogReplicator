/* Test client for GRPC
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

#include <grpcpp/grpcpp.h>
#include <unistd.h>

#include "OraProtoBuf.pb.h"
#include "OraProtoBuf.grpc.pb.h"

using namespace grpc;
using namespace std;
using namespace OpenLogReplicator;

int main(int argc, char** argv) {
    if (argc < 4) {
        cout << "Use: ClientGRPC <uri> <scn> <database>" << endl;
        return 0;
    }

    string uri(argv[1]);
    shared_ptr<Channel> channel(grpc::CreateChannel(uri, grpc::InsecureChannelCredentials()));
    unique_ptr<pb::RedoStreamService::Stub> stub_(pb::RedoStreamService::NewStub(channel));
    ClientContext context;
    shared_ptr<ClientReaderWriter<pb::Request, pb::Response>> stream(stub_->RedoStream(&context));

    pb::Request request;
    pb::Response response;

    request.set_request_code(pb::RequestCode::INITIALIZE);
    cout << "Initialize" << endl;
    stream->Write(request);
    if (!stream->Read(&response))
        return 1;

    if (response.response_code() == pb::ResponseCode::READY) {
        request.Clear();
        request.set_request_code(pb::RequestCode::START);
        request.set_scn(atoi(argv[2]));
        request.set_database_name(argv[3]);
        cout << "Setting SCN: " << dec << request.scn() << ", database: " << request.database_name() << endl;
        stream->Write(request);
        if (!stream->Read(&response))
            return 0;
    }

    if (response.response_code() != pb::ResponseCode::STARTED) {
        cout << "returned code: " << response.response_code() << endl;
        return 1;
    }
    cout << "Last confirmed SCN during start: " << dec << response.scn() << endl;

    for (;;) {
        if (stream->Read(&response)) {
            cout << "code: " << response.response_code() << " scn: " << response.scn() << endl;
        }// else
        //    break;
        sleep(1);
    }
    return 0;
}
