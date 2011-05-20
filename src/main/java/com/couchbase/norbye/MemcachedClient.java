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
package com.couchbase.norbye;

import com.couchbase.norbye.protocol.ErrorCode;
import com.couchbase.norbye.protocol.TapConsumer;
import com.couchbase.norbye.protocol.VBucketState;
import java.io.IOException;
import java.util.Collection;

/**
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public interface MemcachedClient {

    ErrorCode createBucket(String name, String module, String config) throws IOException;

    ErrorCode selectBucket(String name) throws IOException;

    ErrorCode deleteBucket(String name, boolean force) throws IOException;

    boolean set(String key, short vbucket, byte[] data, int flags, int exptime) throws IOException;

    byte[] get(String key, short vbucket) throws IOException;

    boolean setVBucket(short id, VBucketState state) throws IOException;

    void takeover(String name, TapConsumer consumer, Collection<Integer> vbuckets) throws IOException;

    void tap(String name, TapConsumer consumer) throws IOException;

    public void dump(String name, TapConsumer consumer) throws IOException;
}
