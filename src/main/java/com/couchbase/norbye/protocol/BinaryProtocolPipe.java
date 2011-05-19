/*
 *     Copyright 2011 Membase, Inc.
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
 class BinaryProtocolPipe {
    private BufferedInputStream in;
    private BufferedOutputStream out;


    public BinaryProtocolPipe(Socket sock) throws IOException {
        in = new BufferedInputStream(sock.getInputStream());
        out = new BufferedOutputStream(sock.getOutputStream());
    }

    public void send(BinaryMessage cmd) throws IOException {
        send(cmd, true);
    }

    public void send(BinaryMessage cmd, boolean flush) throws IOException {
        out.write(cmd.array());
        if (flush) {
            out.flush();
        }
    }

    public BinaryMessage next() throws IOException {
        BinaryMessage ret;
        do {
            ret = BinaryMessage.next(in);
        } while (ret.getCode() == ComCode.NOOP); // @todo fixme
        
        return ret;
    }
}
