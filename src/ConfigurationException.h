/* Header for ConfigurationException class
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of Open Log Replicator.

Open Log Replicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

Open Log Replicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with Open Log Replicator; see the file LICENSE.txt  If not see
<http://www.gnu.org/licenses/>.  */

#include <exception>
#include <iostream>

#ifndef CONFIGURATIONEXCEPTION_H_
#define CONFIGURATIONEXCEPTION_H_

using namespace std;

namespace OpenLogReplicator {

    class ConfigurationException: public exception {
    public:
        const char *msg;

        ConfigurationException(const char* msg);
        virtual ~ConfigurationException();

        friend ostream& operator<<(ostream& os, const ConfigurationException& ors);
    };
}

#endif
