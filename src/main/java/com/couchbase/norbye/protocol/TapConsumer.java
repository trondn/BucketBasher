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

import com.couchbase.norbye.protocol.ErrorCode;

/**
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public interface TapConsumer {

    ErrorCode mutation(String key, byte[] data, int offset, int len, int flags, int expiration);

    ErrorCode delete(String key);

    ErrorCode flush();

    ErrorCode opaque(byte[] data);

    ErrorCode vbucketSet(byte[] data);

    ErrorCode checkpointStart(byte[] data);

    ErrorCode checkpointEnd(byte[] data);
}
