/* Memory buffer for handling JSON data
   Copyright (C) 2018-2020 Adam Leszczynski.

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

#include <iostream>
#include <string.h>

#include "types.h"
#include "CommandBuffer.h"
#include "RedoLogRecord.h"
#include "MemoryException.h"

namespace OpenLogReplicator {

    CommandBuffer::CommandBuffer(uint64_t outputBufferSize) :
            shutdown(false),
            writer(nullptr),
            posStart(0),
            posEnd(0),
            posEndTmp(0),
            posSize(0),
            outputBufferSize(outputBufferSize) {
        intraThreadBuffer = new uint8_t[outputBufferSize];

        if (intraThreadBuffer == nullptr) {
            cerr << "ERROR: could not allocate memory for output buffer (" << dec << outputBufferSize << " bytes)" << endl;
            throw MemoryException("out of memory");
        }

    }

    void CommandBuffer::stop(void) {
        this->shutdown = true;
    }

    CommandBuffer* CommandBuffer::appendEscape(const uint8_t *str, uint64_t length) {
        if (this->shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length * 2 >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (1)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }
            if (this->shutdown)
                return this;
        }

        if (posEndTmp + length * 2 >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (1)" << endl;
            return this;
        }

        while (length > 0) {
            if (*str == '\t') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 't';
            } else if (*str == '\r') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'r';
            } else if (*str == '\n') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'n';
            } else if (*str == '\f') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'f';
            } else if (*str == '\b') {
                intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = 'b';
            } else {
                if (*str == '"' || *str == '\\' || *str == '/')
                    intraThreadBuffer[posEndTmp++] = '\\';
                intraThreadBuffer[posEndTmp++] = *(str++);
            }
            --length;
        }

        return this;
    }

    CommandBuffer* CommandBuffer::appendHex(uint64_t val, uint64_t length) {
        static const char* digits = "0123456789abcdef";
        if (this->shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (2)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }
        }

        if (posEndTmp + length >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (5)" << endl;
            return this;
        }

        for (uint64_t i = 0, j = (length - 1) * 4; i < length; ++i, j -= 4)
            intraThreadBuffer[posEndTmp + i] = digits[(val >> j) & 0xF];
        posEndTmp += length;

        return this;
    }

    CommandBuffer* CommandBuffer::append(const string str) {
        if (this->shutdown)
            return this;

        uint64_t length = str.length();
        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + length >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (2)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }
        }

        if (posEndTmp + length >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (2)" << endl;
            return this;
        }

        memcpy(intraThreadBuffer + posEndTmp, str.c_str(), length);
        posEndTmp += length;

        return this;
    }

    char CommandBuffer::translationMap[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    CommandBuffer* CommandBuffer::appendRowid(typeobj objn, typeobj objd, typedba bdba, typeslot slot) {
        uint32_t afn =  bdba >> 22;
        bdba &= 0x003FFFFF;
        append(translationMap[(objd >> 30) & 0x3F]);
        append(translationMap[(objd >> 24) & 0x3F]);
        append(translationMap[(objd >> 18) & 0x3F]);
        append(translationMap[(objd >> 12) & 0x3F]);
        append(translationMap[(objd >> 6) & 0x3F]);
        append(translationMap[objd & 0x3F]);
        append(translationMap[(afn >> 12) & 0x3F]);
        append(translationMap[(afn >> 6) & 0x3F]);
        append(translationMap[afn & 0x3F]);
        append(translationMap[(bdba >> 30) & 0x3F]);
        append(translationMap[(bdba >> 24) & 0x3F]);
        append(translationMap[(bdba >> 18) & 0x3F]);
        append(translationMap[(bdba >> 12) & 0x3F]);
        append(translationMap[(bdba >> 6) & 0x3F]);
        append(translationMap[bdba & 0x3F]);
        append(translationMap[(slot >> 12) & 0x3F]);
        append(translationMap[(slot >> 6) & 0x3F]);
        append(translationMap[slot & 0x3F]);
        return this;
    }

    CommandBuffer* CommandBuffer::append(char chr) {
        if (this->shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + 1 >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (3)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }
        }

        if (posEndTmp + 1 >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (3)" << endl;
            return this;
        }

        intraThreadBuffer[posEndTmp++] = chr;

        return this;
    }

    CommandBuffer* CommandBuffer::beginTran() {
        if (this->shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 && posEndTmp + 8 >= posStart) {
                cerr << "WARNING, JSON buffer full, log reader suspended (8)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }
        }

        if (posEndTmp + 8 >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (8)" << endl;
            return this;
        }

        *((uint64_t*)(intraThreadBuffer + posEndTmp)) = 0;
        posEndTmp += 8;

        return this;
    }

    CommandBuffer* CommandBuffer::commitTran() {
        if (posEndTmp == posEnd) {
            cerr << "WARNING: JSON buffer - commit of empty transaction" << endl;
            return this;
        }

        {
            unique_lock<mutex> lck(mtx);
            *((uint64_t*)(intraThreadBuffer + posEnd)) = posEndTmp - posEnd;
            posEndTmp = (posEndTmp + 7) & 0xFFFFFFFFFFFFFFF8;
            posEnd = posEndTmp;

            readersCond.notify_all();
        }

        if (posEndTmp + 1 >= outputBufferSize) {
            cerr << "ERROR: JSON buffer overflow (8)" << endl;
            return this;
        }

        return this;
    }

    CommandBuffer* CommandBuffer::rewind() {
        if (this->shutdown)
            return this;

        {
            unique_lock<mutex> lck(mtx);
            while (posSize > 0 || posStart == 0) {
                cerr << "WARNING, JSON buffer full, log reader suspended (5)" << endl;
                writerCond.wait(lck);
                if (this->shutdown)
                    return this;
            }

            posSize = posEnd;
            posEnd = 0;
            posEndTmp = 0;
        }

        return this;
    }

    uint64_t CommandBuffer::currentTranSize() {
        return posEndTmp - posEnd;
    }

    CommandBuffer::~CommandBuffer() {
        if (intraThreadBuffer != nullptr) {
            delete[] intraThreadBuffer;
            intraThreadBuffer = nullptr;
        }
    }
}
