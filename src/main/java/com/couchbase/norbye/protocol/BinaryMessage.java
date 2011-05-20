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

import java.nio.ByteBuffer;

/**
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
class BinaryMessage {

    static byte COMMAND = Magic.REQUEST.cc();
    static byte RESPONSE = Magic.RESPONSE.cc();
    static final int HEADER_SIZE = 24;

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
    
    void setOpaque(int opaque) {
        bytebuffer.putInt(12, opaque);
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
