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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a TAP consumer that experience problems consuming tap events.
 * The current implementation use a 10% fixed temporary failures and no
 * hard failures.
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class RandomErrorTapConsumer implements TapConsumer {
    private Random random;

    public RandomErrorTapConsumer() {
        random = new Random();
    }

    private ErrorCode next(ComCode cc) {
        int rand = random.nextInt(100);

        if (rand == 0) { // 1% of ENOMEMS
            return ErrorCode.ENOMEM;
        }
        if (rand > 94) {  // 5% temp failures
            Logger.getLogger(this.getClass().getName()).log(Level.FINE, "Sending ETMPFAIL for: {0}", cc.name());
            return ErrorCode.ETMPFAIL;
        }

        return ErrorCode.SUCCESS;
    }

    @Override
    public ErrorCode mutation(String key, byte[] data, int offset, int len, int flags, int expiration) {
        return next(ComCode.TAP_MUTATION);
    }

    @Override
    public ErrorCode delete(String key) {
        return next(ComCode.TAP_DELETE);
    }

    @Override
    public ErrorCode flush() {
        return next(ComCode.TAP_FLUSH);
    }

    @Override
    public ErrorCode opaque(byte[] data) {
        return next(ComCode.TAP_OPAQUE);
    }

    @Override
    public ErrorCode vbucketSet(byte[] data) {
        return next(ComCode.TAP_VBUCKET_SET);
    }

    @Override
    public ErrorCode checkpointStart(byte[] data) {
        return next(ComCode.TAP_CHECKPOINT_START);
    }

    @Override
    public ErrorCode checkpointEnd(byte[] data) {
        return next(ComCode.TAP_CHECKPOINT_END);
    }
}
