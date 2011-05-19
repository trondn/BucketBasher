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
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
 class BinarySetCommand extends BinaryCommand {
    BinarySetCommand(String key, short vbucket, byte[] data) {
        this(key, vbucket, data, 0, 0, 0);
    }

    BinarySetCommand(String key, short vbucket, byte[] data, int flags, int exp) {
        this(key, vbucket, data, 0, flags, exp);
    }

    BinarySetCommand(String key, short vbucket, byte[] data, long cas, int flags, int exp) {
        array = new byte[HEADER_SIZE + key.length() + data.length + 8];

        bytebuffer = ByteBuffer.wrap(array);
        bytebuffer.put((byte)0x80); // magic
        bytebuffer.put(ComCode.SET.cc()); // com code
        bytebuffer.putShort((short) key.length()); // keylen
        bytebuffer.put((byte)8); //extlen
        bytebuffer.put((byte)0); //datatype
        bytebuffer.putShort(vbucket); // bucket
        bytebuffer.putInt(array.length - HEADER_SIZE); // bodylen
        bytebuffer.putInt(0xdeadbeef); // opaque
        bytebuffer.putLong(cas); // cas
        bytebuffer.putInt(flags); // flags
        bytebuffer.putInt(exp); // exp
        bytebuffer.put(key.getBytes()); // add the key
        bytebuffer.put(data);
    }
}
