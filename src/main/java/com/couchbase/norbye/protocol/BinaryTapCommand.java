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
 class BinaryTapCommand extends BinaryCommand {
    public static final int TAP_FLAG_ACK = 0x01;
    public static final int TAP_FLAG_NO_VALUE = 0x02;

    private BinaryTapCommand(byte[] data) {
        array = new byte[data.length];
        System.arraycopy(data, 0, array, 0, data.length);
        bytebuffer = ByteBuffer.wrap(array);
    }

    public int getTapFlags() {
        return (int)bytebuffer.getShort(26);
    }

    public int getTapTTL() {
        return (int)bytebuffer.get(28);
    }

    public static BinaryTapCommand wrap(BinaryCommand cmd) {
        ComCode cc = cmd.getCode();
        switch (cc) {
            case TAP_DELETE:
            case TAP_FLUSH:
            case TAP_MUTATION:
            case TAP_OPAQUE:
            case TAP_VBUCKET_SET:
                return new BinaryTapCommand(cmd.array);
            default:
                System.out.println(cc.toString());

                return null;
        }
    }
}
