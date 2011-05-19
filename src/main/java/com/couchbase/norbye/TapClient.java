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

import com.couchbase.norbye.protocol.RandomErrorTapConsumer;
import java.io.EOFException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class TapClient extends AutoClient {

    public TapClient(MemcachedClient client) {
        super(client);
    }

    @Override
    public void execute() {
        try {
            client.tap(Thread.currentThread().getName(), new RandomErrorTapConsumer());
        } catch (EOFException ex) {
            Logger.getLogger(TapClient.class.getName()).log(Level.FINE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(TapClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
