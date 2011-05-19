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

/**
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
 class BinarySetQCommand extends BinarySetCommand {
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
