/*
 *     Copyright 2011 Membase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.couchbase.norbye.protocol;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
class BinaryMessage {

    static byte COMMAND = (byte) 0x80;
    static byte RESPONSE = (byte) 0x81;
    static final int HEADER_SIZE = 24;

    private static void doRead(InputStream in, byte[] array) throws IOException {
        int offset = 0;
        do {
            int nr = in.read(array, offset, array.length - offset);
            if (nr == -1) {
                throw new EOFException();
            }
            offset += nr;
        } while (offset < array.length);
    }

    static BinaryMessage next(InputStream in) throws IOException {
        ByteBuffer header;
        byte h[] = new byte[HEADER_SIZE];
        header = ByteBuffer.wrap(h);

        doRead(in, h);

        int body = header.getInt(8);
        byte[] data;
        if (body > 0) {
            data = new byte[header.getInt(8)];
            doRead(in, data);
        } else {
            data = new byte[0];
        }

        if (h[0] == COMMAND) {
            return new BinaryCommand(h, data);
        } else if (h[0] == RESPONSE) {
            return new BinaryResponse(h, data);
        }

        throw new RuntimeException("Protocol error");
    }
    byte[] array;
    ByteBuffer bytebuffer;

    BinaryMessage() {
    }

    byte[] array() {
        return array;
    }

    int getDataSize() {
        return bytebuffer.getInt(8) - bytebuffer.getShort(2) - bytebuffer.get(4);
    }

    ComCode getCode() {
        return ComCode.valueOf(bytebuffer.get(1));
    }

    int getOpaque() {
        return bytebuffer.getInt(12);
    }

    String getKey() {
        return null;
    }

    ErrorCode getStatus() {
        throw new RuntimeException("Can only be called on response messages");
    }

    void dump() {
        System.out.println(getClass().getName());
    }
}
