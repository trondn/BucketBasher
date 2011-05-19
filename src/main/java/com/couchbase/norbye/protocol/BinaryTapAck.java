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
class BinaryTapAck extends BinaryMessage {

    public BinaryTapAck(BinaryTapCommand cmd, ErrorCode success) {
        array = new byte[HEADER_SIZE];
        bytebuffer = ByteBuffer.wrap(array);
        bytebuffer.put(RESPONSE); // magic
        bytebuffer.put(cmd.getCode().cc()); // com code
        bytebuffer.putShort((short) 0); // keylen
        bytebuffer.put((byte) 0); //extlen
        bytebuffer.put((byte) 0); //datatype
        bytebuffer.putShort(success.value()); // bucket
        bytebuffer.putInt(0); // bodylen
        bytebuffer.putInt(cmd.getOpaque()); // opaque
        bytebuffer.putLong(0); // cas
    }

}
