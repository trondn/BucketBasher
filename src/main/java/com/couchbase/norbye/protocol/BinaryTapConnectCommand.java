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
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
 class BinaryTapConnectCommand extends BinaryCommand {

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
    
    public BinaryTapConnectCommand(String name) {
        this(name, getList(), false);
    }

    public BinaryTapConnectCommand(String name, Collection<Integer> vbucket, boolean takeover) {
        array = new byte[HEADER_SIZE + 4 + name.length() + vbucket.size() * 2 + 2];
        int flags = TAP_CONNECT_SUPPORT_ACK | TAP_CONNECT_FLAG_LIST_VBUCKETS;
        if (takeover) {
            flags |= TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS;
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
        bytebuffer.putShort((short)vbucket.size());
        for (Integer i : vbucket) {
            bytebuffer.putShort(i.shortValue());
        }
    }
}
