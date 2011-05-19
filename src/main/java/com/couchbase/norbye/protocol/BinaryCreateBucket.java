/*
 *     Copyright 2011 Couchbase, Inc.
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
class BinaryCreateBucket extends BinaryCommand {

    public BinaryCreateBucket(String bucket, String module, String config) {
        int length = bucket.length() + module.length() + 1 + config.length();

        array = new byte[HEADER_SIZE + length];
        bytebuffer = ByteBuffer.wrap(array);
        bytebuffer.put((byte) 0x80); // magic
        bytebuffer.put(ComCode.CREATE_BUCKET.cc()); // com code
        bytebuffer.putShort((short) bucket.length()); // keylen
        bytebuffer.put((byte) 0); //extlen
        bytebuffer.put((byte) 0); //datatype
        bytebuffer.putShort((short) 0); // bucket
        bytebuffer.putInt(length); // bodylen
        bytebuffer.putInt(0xdeadbeef); // opaque
        bytebuffer.putLong(0); // cas
        bytebuffer.put(bucket.getBytes());
        bytebuffer.put(module.getBytes());
        bytebuffer.put((byte) 0x00);
        bytebuffer.put(config.getBytes());
    }
}
