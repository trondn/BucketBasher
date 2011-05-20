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
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public enum Datatype {

    RAW((byte) 0x0), UNKNOWN((byte) 0xff);
    private final byte value;

    Datatype(byte value) {
        this.value = value;
    }

    public byte cc() {
        return value;
    }

    public static Datatype valueOf(byte cc) {
        if (cc == 0) {
            return RAW;
        } else {
            return UNKNOWN;
        }
    }

    public static String toString(Datatype cc) {
        if (cc == RAW) {
            return "raw";
        } else {
            return "unknown";
        }
    }
}
