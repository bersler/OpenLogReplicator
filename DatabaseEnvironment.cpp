/* Generic class for environment variables
   Copyright (C) 2018-2019 Adam Leszczynski.

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

#include "DatabaseEnvironment.h"

namespace OpenLogReplicator {

	DatabaseEnvironment::DatabaseEnvironment() :
		bigEndian(false),
		read16(read16Little),
		read32(read32Little),
		read48(read48Little),
		read56(read56Little),
		read64(read64Little),
		write16(write16Little),
		write32(write32Little),
		write48(write48Little),
		write56(write56Little),
		write64(write64Little) {
	}

	void DatabaseEnvironment::initialize(bool bigEndian) {
		this->bigEndian = bigEndian;

		if (bigEndian) {
			read16 = read16Big;
			read32 = read32Big;
			read48 = read48Big;
			read56 = read56Big;
			read64 = read64Big;
			write16 = write16Big;
			write32 = write32Big;
			write48 = write48Big;
			write56 = write56Big;
			write64 = write64Big;
		}
	}

	DatabaseEnvironment::~DatabaseEnvironment() {
	}

	uint16_t DatabaseEnvironment::read16Little(const uint8_t* buf) {
		return (uint16_t)buf[0] | ((uint16_t)buf[1] << 8);
	}

	uint16_t DatabaseEnvironment::read16Big(const uint8_t* buf) {
		return ((uint16_t)buf[0] << 8) | (uint16_t)buf[1];
	}

	uint32_t DatabaseEnvironment::read32Little(const uint8_t* buf) {
		return (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) |
				((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24);
	}

	uint32_t DatabaseEnvironment::read32Big(const uint8_t* buf) {
		return ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) |
				((uint32_t)buf[2] << 8) | (uint32_t)buf[3];
	}

	uint64_t DatabaseEnvironment::read48Little(const uint8_t* buf) {
		return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
				((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
				((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40);
	}

	uint64_t DatabaseEnvironment::read48Big(const uint8_t* buf) {
		return (((uint64_t)buf[0] << 40) | ((uint64_t)buf[1] << 32) |
				((uint64_t)buf[2] << 24) | ((uint64_t)buf[3] << 16) |
				((uint64_t)buf[4] << 8) | (uint64_t)buf[5]);
	}

	uint64_t DatabaseEnvironment::read56Little(const uint8_t* buf) {
		return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
				((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
				((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40) |
				((uint64_t)buf[6] << 48);
	}

	uint64_t DatabaseEnvironment::read56Big(const uint8_t* buf) {
		return (((uint64_t)buf[0] << 48) | ((uint64_t)buf[1] << 40) |
				((uint64_t)buf[2] << 32) | ((uint64_t)buf[3] << 24) |
				((uint64_t)buf[4] << 16) | ((uint64_t)buf[5] << 8) |
				(uint64_t)buf[6]);
	}

	uint64_t DatabaseEnvironment::read64Little(const uint8_t* buf) {
		return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
				((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
				((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40) |
				((uint64_t)buf[6] << 48) | ((uint64_t)buf[7] << 56);
	}

	uint64_t DatabaseEnvironment::read64Big(const uint8_t* buf) {
		return ((uint64_t)buf[0] << 56) | ((uint64_t)buf[1] << 48) |
				((uint64_t)buf[2] << 40) | ((uint64_t)buf[3] << 32) |
				((uint64_t)buf[4] << 24) | ((uint64_t)buf[5] << 16) |
				((uint64_t)buf[6] << 8) | (uint64_t)buf[7];
	}


	void DatabaseEnvironment::write16Little(uint8_t* buf, uint16_t val) {
		buf[0] = val & 0xFF;
		buf[1] = (val >> 8) & 0xFF;
	}

	void DatabaseEnvironment::write16Big(uint8_t* buf, uint16_t val) {
		buf[0] = (val >> 8) & 0xFF;
		buf[1] = val & 0xFF;
	}

	void DatabaseEnvironment::write32Little(uint8_t* buf, uint32_t val) {
		buf[0] = val & 0xFF;
		buf[1] = (val >> 8) & 0xFF;
		buf[2] = (val >> 16) & 0xFF;
		buf[3] = (val >> 24) & 0xFF;
	}

	void DatabaseEnvironment::write32Big(uint8_t* buf, uint32_t val) {
		buf[0] = (val >> 24) & 0xFF;
		buf[1] = (val >> 16) & 0xFF;
		buf[2] = (val >> 8) & 0xFF;
		buf[3] = val & 0xFF;
	}

	void DatabaseEnvironment::write48Little(uint8_t* buf, uint64_t val) {
		buf[0] = val & 0xFF;
		buf[1] = (val >> 8) & 0xFF;
		buf[2] = (val >> 16) & 0xFF;
		buf[3] = (val >> 24) & 0xFF;
		buf[4] = (val >> 32) & 0xFF;
		buf[5] = (val >> 40) & 0xFF;
	}

	void DatabaseEnvironment::write48Big(uint8_t* buf, uint64_t val) {
		buf[0] = (val >> 40) & 0xFF;
		buf[1] = (val >> 32) & 0xFF;
		buf[2] = (val >> 24) & 0xFF;
		buf[3] = (val >> 16) & 0xFF;
		buf[4] = (val >> 8) & 0xFF;
		buf[5] = val & 0xFF;
	}

	void DatabaseEnvironment::write56Little(uint8_t* buf, uint64_t val) {
		buf[0] = val & 0xFF;
		buf[1] = (val >> 8) & 0xFF;
		buf[2] = (val >> 16) & 0xFF;
		buf[3] = (val >> 24) & 0xFF;
		buf[4] = (val >> 32) & 0xFF;
		buf[5] = (val >> 40) & 0xFF;
		buf[6] = (val >> 48) & 0xFF;
	}

	void DatabaseEnvironment::write56Big(uint8_t* buf, uint64_t val) {
		buf[0] = (val >> 48) & 0xFF;
		buf[1] = (val >> 40) & 0xFF;
		buf[2] = (val >> 32) & 0xFF;
		buf[3] = (val >> 24) & 0xFF;
		buf[4] = (val >> 16) & 0xFF;
		buf[5] = (val >> 8) & 0xFF;
		buf[6] = val & 0xFF;
	}

	void DatabaseEnvironment::write64Little(uint8_t* buf, uint64_t val) {
		buf[0] = val & 0xFF;
		buf[1] = (val >> 8) & 0xFF;
		buf[2] = (val >> 16) & 0xFF;
		buf[3] = (val >> 24) & 0xFF;
		buf[4] = (val >> 32) & 0xFF;
		buf[5] = (val >> 40) & 0xFF;
		buf[6] = (val >> 48) & 0xFF;
		buf[7] = (val >> 56) & 0xFF;
	}

	void DatabaseEnvironment::write64Big(uint8_t* buf, uint64_t val) {
		buf[0] = (val >> 56) & 0xFF;
		buf[1] = (val >> 48) & 0xFF;
		buf[2] = (val >> 40) & 0xFF;
		buf[3] = (val >> 32) & 0xFF;
		buf[4] = (val >> 24) & 0xFF;
		buf[5] = (val >> 16) & 0xFF;
		buf[6] = (val >> 8) & 0xFF;
		buf[7] = val & 0xFF;
	}
}
