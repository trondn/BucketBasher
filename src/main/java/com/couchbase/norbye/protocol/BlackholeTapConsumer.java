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

import java.util.Random;

/**
 * Just swallow all of the data
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class BlackholeTapConsumer implements TapConsumer {
    @Override
    public ErrorCode mutation(String key, byte[] data, int offset, int len, int flags, int expiration) {
        return ErrorCode.SUCCESS;
    }

    @Override
    public ErrorCode delete(String key) {
        return ErrorCode.SUCCESS;
    }

    @Override
    public ErrorCode flush() {
        return ErrorCode.SUCCESS;
    }

    @Override
    public ErrorCode opaque(byte[] data) {
        return ErrorCode.SUCCESS;
    }

    @Override
    public ErrorCode vbucketSet(byte[] data) {
        return ErrorCode.SUCCESS;
    }

    @Override
    public ErrorCode checkpointStart(byte[] data) {
        return ErrorCode.SUCCESS;
    }

    @Override
    public ErrorCode checkpointEnd(byte[] data) {
        return ErrorCode.SUCCESS;
    }
}
