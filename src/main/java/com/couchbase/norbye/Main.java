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

import com.couchbase.norbye.protocol.VBucketState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a small test application used to beat the shit out of
 * memcached with bucket engine running on the local host. I need this to
 * create a tap threads walking the iterator while I'm killing the bucket
 * to try to get into the race condition thing...
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException {
        // TODO code application logic here

        MemcachedClient client = MemcachedClientFactory.create("localhost", 11211);

        try {
            List<Integer> vbuckets = new ArrayList<Integer>();
            for (int ii = 0; ii < 1024; ++ii) {
                if (!client.setVBucket((short) ii, VBucketState.ACTIVE)) {
                    System.out.println("Failed to create vbucket: " + ii);
                }
                vbuckets.add(ii);
            }

            List<AutoClient> clients = new ArrayList<AutoClient>();
            for (int ii = 0; ii < 10; ++ii) {
                clients.add(new TapClient(MemcachedClientFactory.create("localhost", 11211)));

            }

            for (int ii = 0; ii < 20; ++ii) {
                clients.add(new MixedLoadClient(MemcachedClientFactory.create("localhost", 11211)));
            }



            List<Thread> threads = new ArrayList<Thread>();
            for (AutoClient c : clients) {
                Thread t = new Thread(c);
                threads.add(t);
                t.start();
            }


            // wait for all of them to stop..
            for (Thread t : threads) {
                t.join();
            }
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private Main() {
    }
}
