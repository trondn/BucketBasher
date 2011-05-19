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
class BinaryGetCommand extends BinaryCommand {
    public BinaryGetCommand(String key, short vbucket) {
        array = new byte[HEADER_SIZE + key.length()];

        bytebuffer = ByteBuffer.wrap(array);
        bytebuffer.put((byte)0x80); // magic
        bytebuffer.put(ComCode.GET.cc()); // com code
        bytebuffer.putShort((short) key.length()); // keylen
        bytebuffer.put((byte)0); //extlen
        bytebuffer.put((byte)0); //datatype
        bytebuffer.putShort(vbucket); // bucket
        bytebuffer.putInt(array.length - HEADER_SIZE); // bodylen
        bytebuffer.putInt(0xdeadbeef); // opaque
        bytebuffer.putLong(0); // cas
        bytebuffer.put(key.getBytes()); // add the key
    }
}
