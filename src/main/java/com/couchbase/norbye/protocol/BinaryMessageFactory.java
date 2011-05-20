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
public class BinaryMessageFactory {

    static class BinaryNoopCommand extends BinaryCommand {

        public BinaryNoopCommand() {
            array = new byte[HEADER_SIZE];
            bytebuffer = ByteBuffer.wrap(array);
            bytebuffer.put((byte) 0x80); // magic
            bytebuffer.put(ComCode.NOOP.cc()); // com code
            bytebuffer.putShort((short) 0); // keylen
            bytebuffer.put((byte) 0); //extlen
            bytebuffer.put((byte) 0); //datatype
            bytebuffer.putShort((short) 0); // bucket
            bytebuffer.putInt(0); // bodylen
            bytebuffer.putInt(0xdeadbeef); // opaque
            bytebuffer.putLong(0); // cas
        }
    }

    static class BinaryGetCommand extends BinaryCommand {

        public BinaryGetCommand(String key, short vbucket) {
            array = new byte[HEADER_SIZE + key.length()];

            bytebuffer = ByteBuffer.wrap(array);
            bytebuffer.put((byte) 0x80); // magic
            bytebuffer.put(ComCode.GET.cc()); // com code
            bytebuffer.putShort((short) key.length()); // keylen
            bytebuffer.put((byte) 0); //extlen
            bytebuffer.put((byte) 0); //datatype
            bytebuffer.putShort(vbucket); // bucket
            bytebuffer.putInt(array.length - HEADER_SIZE); // bodylen
            bytebuffer.putInt(0xdeadbeef); // opaque
            bytebuffer.putLong(0); // cas
            bytebuffer.put(key.getBytes()); // add the key
        }
    }

    static class BinaryCreateBucket extends BinaryCommand {

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

    static class BinaryDeleteBucket extends BinaryCommand {

        static final String FORCE = "force=true";

        public BinaryDeleteBucket(String bucket, boolean force) {
            int length = bucket.length();
            if (force) {
                length += FORCE.length();
            }

            array = new byte[HEADER_SIZE + length];
            bytebuffer = ByteBuffer.wrap(array);
            bytebuffer.put((byte) 0x80); // magic
            bytebuffer.put(ComCode.DELETE_BUCKET.cc()); // com code
            bytebuffer.putShort((short) bucket.length()); // keylen
            bytebuffer.put((byte) 0); //extlen
            bytebuffer.put((byte) 0); //datatype
            bytebuffer.putShort((short) 0); // bucket
            bytebuffer.putInt(length); // bodylen
            bytebuffer.putInt(0xdeadbeef); // opaque
            bytebuffer.putLong(0); // cas
            bytebuffer.put(bucket.getBytes());
            if (force) {
                bytebuffer.put(FORCE.getBytes());
            }
        }
    }

    static class BinarySaslCommand extends BinaryCommand {

        public BinarySaslCommand(String auth, String passwd) {
            int length = 7 + auth.length() + passwd.length();
            array = new byte[HEADER_SIZE + length];
            bytebuffer = ByteBuffer.wrap(array);
            bytebuffer.put((byte) 0x80); // magic
            bytebuffer.put(ComCode.SASL_AUTH.cc()); // com code
            bytebuffer.putShort((short) 5); // keylen
            bytebuffer.put((byte) 0); //extlen
            bytebuffer.put((byte) 0); //datatype
            bytebuffer.putShort((short) 0); // bucket
            bytebuffer.putInt(length); // bodylen
            bytebuffer.putInt(0xdeadbeef); // opaque
            bytebuffer.putLong(0); // cas
            bytebuffer.put("PLAIN".getBytes());
            bytebuffer.put((byte) 0);
            bytebuffer.put(auth.getBytes());
            bytebuffer.put((byte) 0);
            bytebuffer.put(passwd.getBytes());
        }
    }

    static class BinarySelectBucket extends BinaryCommand {

        public BinarySelectBucket(String bucket) {
            int length = bucket.length();
            array = new byte[HEADER_SIZE + length];
            bytebuffer = ByteBuffer.wrap(array);
            bytebuffer.put((byte) 0x80); // magic
            bytebuffer.put(ComCode.SELECT_BUCKET.cc()); // com code
            bytebuffer.putShort((short) bucket.length()); // keylen
            bytebuffer.put((byte) 0); //extlen
            bytebuffer.put((byte) 0); //datatype
            bytebuffer.putShort((short) 0); // bucket
            bytebuffer.putInt(length); // bodylen
            bytebuffer.putInt(0xdeadbeef); // opaque
            bytebuffer.putLong(0); // cas
            bytebuffer.put(bucket.getBytes());
        }
    }

    static class BinarySetCommand extends BinaryCommand {

        BinarySetCommand(String key, short vbucket, byte[] data) {
            this(key, vbucket, data, 0, 0, 0);
        }

        BinarySetCommand(String key, short vbucket, byte[] data, int flags, int exp) {
            this(key, vbucket, data, 0, flags, exp);
        }

        BinarySetCommand(String key, short vbucket, byte[] data, long cas, int flags, int exp) {
            array = new byte[HEADER_SIZE + key.length() + data.length + 8];

            bytebuffer = ByteBuffer.wrap(array);
            bytebuffer.put((byte) 0x80); // magic
            bytebuffer.put(ComCode.SET.cc()); // com code
            bytebuffer.putShort((short) key.length()); // keylen
            bytebuffer.put((byte) 8); //extlen
            bytebuffer.put((byte) 0); //datatype
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

    static class BinarySetQCommand extends BinarySetCommand {

        BinarySetQCommand(String key, short vbucket, byte[] data) {
            this(key, vbucket, data, 0, 0, 0);
        }

        BinarySetQCommand(String key, short vbucket, byte[] data, int flags, int exp) {
            this(key, vbucket, data, 0, flags, exp);
        }

        BinarySetQCommand(String key, short vbucket, byte[] data, long cas, int flags, int exp) {
            super(key, vbucket, data, cas, flags, exp);
            bytebuffer.put(1, ComCode.SETQ.cc());
        }
    }

    static class BinarySetVbucketCommand extends BinaryCommand {

        public BinarySetVbucketCommand(short vbid, VBucketState state) {
            array = new byte[HEADER_SIZE + 4];
            bytebuffer = ByteBuffer.wrap(array);
            bytebuffer.put(COMMAND); // magic
            bytebuffer.put(ComCode.SET_VBUCKET.cc()); // com code
            bytebuffer.putShort((short) 0); // keylen
            bytebuffer.put((byte) 4); //extlen
            bytebuffer.put((byte) 0); //datatype
            bytebuffer.putShort(vbid); // bucket
            bytebuffer.putInt(array.length - HEADER_SIZE); // bodylen
            bytebuffer.putInt(0xdeadbeef); // opaque
            bytebuffer.putLong(0); // cas
            bytebuffer.putInt(state.cc());
        }
    }
}
