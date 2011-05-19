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

import com.couchbase.norbye.protocol.BinaryClientImpl;

/**
 * Factory class used to create the class to use.. As of now I only support
 * a single server..
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class MemcachedClientFactory {
    public static MemcachedClient create(String host, int port) {
        return new BinaryClientImpl(host, port);
    }

    private MemcachedClientFactory() {
    }
}
