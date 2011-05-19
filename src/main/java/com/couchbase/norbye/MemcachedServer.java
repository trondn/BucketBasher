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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A small utility class to spin up a memcached server with bucket engine
 * on the local host. By default it will load ep-engine.
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class MemcachedServer implements Runnable {

    String memcached;
        Process p = null;

    public MemcachedServer(File root) {
        memcached = (new File(new File(root, "bin"), "memcached")).getAbsolutePath();
    }

    @Override
    public void run() {
        List<String> cmd = new ArrayList<String>();
        cmd.add(memcached);
        cmd.add("-E");
        cmd.add("bucket_engine.so");
        cmd.add("-e");
        cmd.add("engine=ep.so;default=true;admin=admin;auto_create=true");

        StringBuilder sb = new StringBuilder();
        for (String s : cmd) {
            sb.append(s);
            sb.append(' ');
        }
        System.out.println(sb.toString().trim());


        ProcessBuilder builder = new ProcessBuilder(cmd);
        try {
            p = builder.start();
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String s;
            while ((s = in.readLine()) != null) {
                System.out.println(s);
            }
            p.waitFor();
        } catch (Exception ex) {
            Logger.getLogger(MemcachedServer.class.getName()).log(Level.SEVERE, "An exception occurred", ex);
        }
        Logger.getLogger(MemcachedServer.class.getName()).warning("The memcached server stopped!!!");
    }

    public void shutdown() {
        p.destroy();
    }
}
