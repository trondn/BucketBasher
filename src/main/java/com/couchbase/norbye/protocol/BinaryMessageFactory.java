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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class BinaryMessageFactory {

    private static final short EMPTY_KEY_LENGTH = 0;
    private static final byte EMPTY_EXT_LENGTH = 0;
    private static final int EMPTY_BODY_LENGTH = 0;

    static class BinaryNoopCommand extends BinaryCommand {

        public BinaryNoopCommand() {
            array = new byte[HEADER_SIZE];
            bytebuffer = ByteBuffer.wrap(array);
            bytebuffer.put(Magic.REQUEST.cc()); // magic
            bytebuffer.put(ComCode.NOOP.cc()); // com code
            bytebuffer.putShort(EMPTY_KEY_LENGTH); // keylen
            bytebuffer.put(EMPTY_EXT_LENGTH); //extlen
            bytebuffer.put(Datatype.RAW.cc()); // datatype
            bytebuffer.putShort((short) 0); // bucket
            bytebuffer.putInt(EMPTY_BODY_LENGTH); // bodylen
            bytebuffer.putInt(0xdeadbeef); // opaque
            bytebuffer.putLong(0); // cas
        }
    }

    static class BinaryGetCommand extends BinaryCommand {

        public BinaryGetCommand(String key, short vbucket) {
            array = new byte[HEADER_SIZE + key.length()];

            bytebuffer = ByteBuffer.wrap(array);
            bytebuffer.put(Magic.REQUEST.cc()); // magic
            bytebuffer.put(ComCode.GET.cc()); // com code
            bytebuffer.putShort((short) key.length()); // keylen
            bytebuffer.put(EMPTY_EXT_LENGTH); //extlen
            bytebuffer.put(Datatype.RAW.cc()); // datatype
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
            bytebuffer.put(Magic.REQUEST.cc()); // magic
            bytebuffer.put(ComCode.CREATE_BUCKET.cc()); // com code
            bytebuffer.putShort((short) bucket.length()); // keylen
            bytebuffer.put(EMPTY_EXT_LENGTH); //extlen
            bytebuffer.put(Datatype.RAW.cc()); // datatype
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
            bytebuffer.put(Magic.REQUEST.cc()); // magic
            bytebuffer.put(ComCode.DELETE_BUCKET.cc()); // com code
            bytebuffer.putShort((short) bucket.length()); // keylen
            bytebuffer.put(EMPTY_EXT_LENGTH); //extlen
            bytebuffer.put(Datatype.RAW.cc()); // datatype
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
            bytebuffer.put(Magic.REQUEST.cc()); // magic
            bytebuffer.put(ComCode.SASL_AUTH.cc()); // com code
            bytebuffer.putShort((short) 5); // keylen
            bytebuffer.put(EMPTY_EXT_LENGTH); //extlen
            bytebuffer.put(Datatype.RAW.cc()); // datatype
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
            bytebuffer.put(Magic.REQUEST.cc()); // magic
            bytebuffer.put(ComCode.SELECT_BUCKET.cc()); // com code
            bytebuffer.putShort((short) bucket.length()); // keylen
            bytebuffer.put(EMPTY_EXT_LENGTH); //extlen
            bytebuffer.put(Datatype.RAW.cc()); // datatype
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
            bytebuffer.put(Magic.REQUEST.cc()); // magic
            bytebuffer.put(ComCode.SET.cc()); // com code
            bytebuffer.putShort((short) key.length()); // keylen
            bytebuffer.put((byte) 8); //extlen
            bytebuffer.put(Datatype.RAW.cc()); // datatype
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
            bytebuffer.putShort(EMPTY_KEY_LENGTH); // keylen
            bytebuffer.put((byte) 4); //extlen
            bytebuffer.put(Datatype.RAW.cc()); // datatype
            bytebuffer.putShort(vbid); // bucket
            bytebuffer.putInt(array.length - HEADER_SIZE); // bodylen
            bytebuffer.putInt(0xdeadbeef); // opaque
            bytebuffer.putLong(0); // cas
            bytebuffer.putInt(state.cc());
        }
    }

    static class BinaryTapConnectCommand extends BinaryCommand {

        public static final int TAP_CONNECT_FLAG_BACKFILL = 0x01;
        public static final int TAP_CONNECT_FLAG_DUMP = 0x02;
        public static final int TAP_CONNECT_FLAG_LIST_VBUCKETS = 0x04;
        public static final int TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS = 0x08;
        public static final int TAP_CONNECT_SUPPORT_ACK = 0x10;
        public static final int TAP_CONNECT_REQUEST_KEYS_ONLY = 0x20;
        public static final int TAP_CONNECT_CHECKPOINT = 0x40;
        public static final int TAP_CONNECT_REGISTERED_CLIENT = 0x80;

        private static Collection<Integer> getList() {
            ArrayList<Integer> ret = new ArrayList<Integer>();
            ret.add(Integer.valueOf(0));
            return ret;
        }

        public BinaryTapConnectCommand(String name, boolean dump) {
            this(name, getList(), false, dump);
        }

        public BinaryTapConnectCommand(String name, Collection<Integer> vbucket, boolean takeover) {
            this(name, vbucket, takeover, false);
        }

        public BinaryTapConnectCommand(String name, Collection<Integer> vbucket, boolean takeover, boolean dump) {
            array = new byte[HEADER_SIZE + 4 + name.length() + vbucket.size() * 2 + 2];
            int flags = TAP_CONNECT_SUPPORT_ACK | TAP_CONNECT_FLAG_LIST_VBUCKETS;
            if (takeover) {
                flags |= TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS;
            }

            if (dump) {
                flags |= TAP_CONNECT_FLAG_DUMP;
            }
            
            bytebuffer = ByteBuffer.wrap(array);
            bytebuffer.put(COMMAND); // magic
            bytebuffer.put(ComCode.TAP_CONNECT.cc()); // com code
            bytebuffer.putShort((short) name.length()); // keylen
            bytebuffer.put((byte) 4); //extlen
            bytebuffer.put((byte) 0); //datatype
            bytebuffer.putShort((short) 0); // bucket
            bytebuffer.putInt(array.length - HEADER_SIZE); // bodylen
            bytebuffer.putInt(0xdeadbeef); // opaque
            bytebuffer.putLong(0); // cas
            bytebuffer.putInt(flags); // flags
            bytebuffer.put(name.getBytes());
            bytebuffer.putShort((short) vbucket.size());
            for (Integer i : vbucket) {
                bytebuffer.putShort(i.shortValue());
            }
        }
    
    }

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
        byte h[] = new byte[BinaryMessage.HEADER_SIZE];
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

        if (h[0] == Magic.REQUEST.cc()) {
            return new BinaryCommand(h, data);
        } else if (h[0] == Magic.RESPONSE.cc()) {
            return new BinaryResponse(h, data);
        }

        throw new RuntimeException("Protocol error");
    }
}
