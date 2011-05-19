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

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @todo fixme!
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class MixedLoadClient extends AutoClient {

    public MixedLoadClient(MemcachedClient client) {
        super(client);
    }

    @Override
    public void execute() {
        short vbucket = (short) random.nextInt(1024);
        int ii = random.nextInt(100);
        try {
            if (ii < 50) {
                client.set("key-" + ii, vbucket, new byte[random.nextInt(32767)], 0, 0);
            } else {
                client.get("key-" + ii, vbucket);
            }
        } catch (IOException exp) {
            Logger.getLogger(this.getClass().getName()).log(Level.WARNING, "Sending ETMPFAIL for:", exp);
        }
    }
}