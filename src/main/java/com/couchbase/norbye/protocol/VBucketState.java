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

/**
 * The different states a vbucket may be in
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public enum VBucketState {

    ACTIVE(1), REPLICA(2), PENDING(3), DEAD(4);
    private final int value;

    VBucketState(int value) {
        this.value = (byte) value;
    }

    public int cc() {
        return value;
    }

    public static VBucketState valueOf(int cc) {
        switch (cc) {
            case 1:
                return ACTIVE;
            case 2:
                return REPLICA;
            case 3:
                return PENDING;
            case 4:
                return DEAD;
            default:
                throw new RuntimeException("Invalid vbucket state");
        }
    }

    public static String toString(VBucketState cc) {
        switch (cc) {
            case ACTIVE:
                return "active";
            case REPLICA:
                return "replica";
            case PENDING:
                return "pending";
            case DEAD:
                return "dead";
            default:
                throw new RuntimeException("Invalid vbucket state");
        }
    }
}
