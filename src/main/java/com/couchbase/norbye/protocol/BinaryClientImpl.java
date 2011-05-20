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

import com.couchbase.norbye.MemcachedClient;
import java.io.IOException;
import java.net.Socket;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class BinaryClientImpl implements MemcachedClient {

    private Socket socket;
    private BinaryProtocolPipe pipe;
    private final String host;
    private final int port;

    public BinaryClientImpl(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private void ensurePipe() throws IOException {
        if (pipe == null) {
            socket = new Socket(host, port);
            pipe = new BinaryProtocolPipe(socket);
        }
    }

    private void resetPipe() {
        try {
            socket.close();
        } catch (IOException ex) {
            Logger.getLogger(BinaryClientImpl.class.getName()).log(Level.FINE, "Failed to close pipe", ex);
        }
        pipe = null;

    }

    private ErrorCode dispatchTapCommand(BinaryTapCommand cmd, TapConsumer consumer) {
        if (consumer == null) {
            return ErrorCode.SUCCESS;
        }
        switch (cmd.getCode()) {
            case TAP_FLUSH:
                return consumer.flush();
            case TAP_DELETE:
                return consumer.delete(cmd.getKey());
            case TAP_MUTATION:
                // @todo fix me
                return consumer.mutation(cmd.getKey(), cmd.array(), 0, 0, 0, 0);
            case TAP_OPAQUE:
                return consumer.opaque(cmd.array());
            case TAP_VBUCKET_SET:
                return consumer.vbucketSet(cmd.array());
            case TAP_CHECKPOINT_START:
                return consumer.checkpointStart(cmd.array());
            case TAP_CHECKPOINT_END:
                return consumer.checkpointEnd(cmd.array());

            default:
                throw new RuntimeException("Unexpected command received");
        }
    }

    public void doTap(String name, TapConsumer consumer) throws IOException {
        ensurePipe();
        pipe.send(new BinaryTapConnectCommand(name));

        while (socket.isConnected()) {
            BinaryMessage msg = pipe.next();
            if (msg instanceof BinaryCommand) {
                BinaryTapCommand cmd = BinaryTapCommand.wrap((BinaryCommand) msg);
                if (cmd == null) {
                    throw new RuntimeException("Unexpected message");
                } else {
                    ErrorCode success = dispatchTapCommand(cmd, consumer);
                    if (success != ErrorCode.SUCCESS || (cmd.getTapFlags() & BinaryTapCommand.TAP_FLAG_ACK) != 0) {
                        pipe.send(new BinaryTapAck(cmd, success));
                    }
                }
            }
        }
    }

    @Override
    public void tap(String name, TapConsumer consumer) throws IOException {
        try {
            doTap(name, consumer);
        } catch (IOException exp) {
            resetPipe();
            throw exp;
        }
    }

    @Override
    public void takeover(String name, TapConsumer consumer, Collection<Integer> vbuckets) throws IOException {
        ensurePipe();
        pipe.send(new BinaryTapConnectCommand(name, vbuckets, true));

        while (socket.isConnected()) {
            BinaryMessage msg = pipe.next();
            if (msg instanceof BinaryCommand) {
                BinaryTapCommand cmd = BinaryTapCommand.wrap((BinaryCommand) msg);
                if (cmd == null) {
                    throw new RuntimeException("Unexpected message");
                } else {
                    ErrorCode success = dispatchTapCommand(cmd, consumer);
                    if (success != ErrorCode.SUCCESS || (cmd.getTapFlags() & BinaryTapCommand.TAP_FLAG_ACK) != 0) {
                        pipe.send(new BinaryTapAck(cmd, success));
                    }
                }
            }
        }
    }

    @Override
    public boolean setVBucket(short id, VBucketState state) throws IOException {
        ensurePipe();
        pipe.send(new BinarySetVbucketCommand(id, state));
        BinaryMessage msg = pipe.next();
        if (msg instanceof BinaryResponse) {
            BinaryResponse rsp = (BinaryResponse)msg;
            return rsp.getStatus() == ErrorCode.SUCCESS;
        } else {
            throw new RuntimeException("Unexpected message");
        }
    }

    @Override
    public boolean set(String key, short vbucket, byte[] data, int flags, int exptime) throws IOException {
        ensurePipe();
        pipe.send(new BinarySetCommand(key, vbucket, data, flags, exptime));
        BinaryMessage msg = pipe.next();
        if (msg instanceof BinaryResponse) {
            BinaryResponse rsp = (BinaryResponse)msg;
            return rsp.getStatus() == ErrorCode.SUCCESS;
        } else {
            throw new RuntimeException("Unexpected message");
        }
    }

    @Override
    public byte[] get(String key, short vbucket) throws IOException {
        return null;
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ErrorCode createBucket(String name, String module, String config) throws IOException {
        ensurePipe();
        pipe.send(new BinaryCreateBucket(name, module, config));
        BinaryMessage msg = pipe.next();
        if (msg instanceof BinaryResponse) {
            BinaryResponse rsp = (BinaryResponse)msg;
            return rsp.getStatus();
        } else {
            throw new RuntimeException("Unexpected message");
        }
    }

    @Override
    public ErrorCode selectBucket(String name) throws IOException {
        ensurePipe();
        pipe.send(new BinarySelectBucket(name));
        BinaryMessage msg = pipe.next();
        if (msg instanceof BinaryResponse) {
            BinaryResponse rsp = (BinaryResponse)msg;
            return rsp.getStatus();
        } else {
            throw new RuntimeException("Unexpected message");
        }
    }

    @Override
    public ErrorCode deleteBucket(String name, boolean force) throws IOException {
        ensurePipe();
        pipe.send(new BinaryDeleteBucket(name, force));
        BinaryMessage msg = pipe.next();
        if (msg instanceof BinaryResponse) {
            BinaryResponse rsp = (BinaryResponse)msg;
            return rsp.getStatus();
        } else {
            throw new RuntimeException("Unexpected message");
        }
    }
}
