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

    private void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException ex) {
            Logger.getLogger(MixedLoadClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private boolean get(String key) throws IOException {
        return (client.get(key, (short) 0) == null) ? false : true;
    }

    private boolean set(String key, int size) throws IOException {
        int ii = 0;
        while (!client.set("key-" + key, (short) 0, new byte[size], 0, 0)) {
            ++ii;
            if (ii < 10) {
                sleep(ii * 1000);
            } else {
                Logger.getLogger(MixedLoadClient.class.getName()).log(Level.SEVERE, "Failed to set item: {0}", key);
                return false;
            }
        }
        return true;
    }

    @Override
    public void execute() {
        try {
            String key = "key-" + random.nextInt(10000);
            if (random.nextInt(3) == 1) {
                set(key, random.nextInt(32));
            } else {
                get(key);
            }
        } catch (IOException exp) {
            Logger.getLogger(this.getClass().getName()).log(Level.WARNING, "Failed setting object:", exp);
        }
    }
}
