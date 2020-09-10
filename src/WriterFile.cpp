/* Thread writing to stdout
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

#include <fstream>
#include <thread>

#include "ConfigurationException.h"
#include "OracleAnalyser.h"
#include "RuntimeException.h"
#include "WriterFile.h"

using namespace std;

namespace OpenLogReplicator {

    WriterFile::WriterFile(const char *alias, OracleAnalyser *oracleAnalyser, const char *name) :
        Writer(alias, oracleAnalyser, 0),
        name(name),
        fileOpen(false) {
        if (this->name.length() == 0) {
            this->name = "stdout";
            output = &cout;
        } else {
            output = new ofstream();
            ((ofstream*)output)->open(this->name.c_str(),  ofstream::out | std::ofstream::app);
            if (((ofstream*)output)->fail()) {
                RUNTIME_FAIL("error opening file to write: " << dec << this->name);
            }
            fileOpen = true;
        }
    }

    WriterFile::~WriterFile() {
        if (output != nullptr) {
            if (fileOpen) {
                ((ofstream*)output)->close();
                fileOpen = false;
                delete output;
            }
            output = nullptr;
        }
    }

    void WriterFile::sendMessage(uint8_t *buffer, uint64_t length, bool dealloc) {
        output->write((const char*)buffer, length);
        if (((ofstream*)output)->fail()) {
            RUNTIME_FAIL("error writing to write: " << dec << name);
        }
        *output << endl;
        if (dealloc)
            free(buffer);
    }

    string WriterFile::getName() {
        return name;
    }
}
