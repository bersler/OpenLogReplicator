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
#include "types.h"

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
    unique_ptr<pb::OpenLogReplicator::Stub> stub_(pb::OpenLogReplicator::NewStub(channel));
    unique_ptr<ClientContext> context(new ClientContext);
    //shared_ptr<ClientReaderWriter<pb::Request, pb::Response>> stream(stub_->RedoStream(&context));

    pb::RedoRequest request;
    pb::RedoResponse response;
    request.set_code(pb::RequestCode::INFO);
    request.set_database_name(argv[3]);
    cerr << "OpenLogReplicator v." PACKAGE_VERSION " test client (C) 2018-2020 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information" << endl;
    unique_ptr<ClientReaderWriter<pb::RedoRequest, pb::RedoResponse>> readerWriter = stub_->Redo(context.get());

    if (!readerWriter->Write(request)) {
        cerr << "error writing RPC" << endl;
        return 0;
    }
    if (!readerWriter->Read(&response)) {
        cerr << "error reading RPC" << endl;
        return 0;
    }
    cerr << "- code: " << (uint64_t)response.code() << ", scn: " << response.scn() << endl;
    uint64_t scn = 0;

    if (response.code() == pb::ResponseCode::STARTED) {
        scn = response.scn();
    } else if (response.code() == pb::ResponseCode::READY) {

        request.Clear();
        request.set_code(pb::RequestCode::START);
        request.set_scn(atoi(argv[2]));
        request.set_database_name(argv[3]);

        cout << "Setting SCN: " << dec << request.scn() << ", database: " << request.database_name() << endl;
        if (!readerWriter->Write(request)) {
            cerr << "error writing RPC" << endl;
            return 0;
        }
        if (!readerWriter->Read(&response)) {
            cerr << "error reading RPC" << endl;
            return 0;
        }
        cerr << "- code: " << (uint64_t)response.code() << ", scn: " << response.scn() << endl;

        if (response.code() == pb::ResponseCode::STARTED) {
            scn = response.scn();
        } else {
            cout << "returned code: " << response.code() << endl;
            return 1;
        }
    }

    cout << "Last confirmed SCN during start: " << dec << scn << endl;

    for (;;) {
        if (!readerWriter->Read(&response)) {
            cerr << "error reading RPC" << endl;
            return 0;
        }
        cerr << "stream: " << (uint64_t)response.code() << ", scn: " << response.scn() << endl;
    }

    return 0;
}
