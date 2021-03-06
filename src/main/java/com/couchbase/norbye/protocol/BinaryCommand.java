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
class BinaryCommand extends BinaryMessage {
    BinaryCommand() {

    }

    BinaryCommand(byte[] h, byte[] data) {
        array = new byte[h.length + data.length];
        System.arraycopy(h, 0, array, 0, h.length);
        System.arraycopy(data, 0, array, h.length, data.length);
        bytebuffer = ByteBuffer.wrap(array);
    }

    @Override
    byte[] array() {
        if (bytebuffer.get(0) != COMMAND) {
            throw new RuntimeException("Unexpected protocol element");
        }
        return array;
    }
}
