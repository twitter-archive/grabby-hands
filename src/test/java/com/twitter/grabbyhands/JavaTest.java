/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.grabbyhands;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class JavaTest {
    public JavaTest() {
        System.out.println("java test");
    }

    public void run() {
        testCreate();
        testWriteRead();
    }

    protected void testCreate() {
        System.out.println("run testCreate");
        List<String> servers = Arrays.asList("localhost:22133");
        List<String> queues = Arrays.asList("grabby_test");

        Config config = new Config(servers);
        HashMap<String, ConfigQueue> queueConfigs = config.addQueues(queues);
        System.out.println("1");
        assert(queueConfigs.containsKey(queues.get(0)));

        config.setRecvNumConnections(4);
        config.setSendNumConnections(5);
        System.out.println("2");
        assert(config.recvNumConnections() == 4);
        assert(config.getRecvNumConnections() == 4);
        System.out.println("2a");
        assert(config.sendNumConnections() == 5);
        assert(config.getSendNumConnections() == 5);

        config.setMaxMessageBytes(100);
        System.out.println("2z");
        assert(config.getMaxMessageBytes() == 100);

        ConfigQueue configQueue = queueConfigs.get(queues.get(0));
        System.out.println("3");
        assert(configQueue.recvNumConnections() == 4);
        System.out.println("3a");
        assert(configQueue.getRecvNumConnections() == 4);

        System.out.println("4");
        assert(configQueue.recvQueueDepth() == 4);
        assert(configQueue.getRecvQueueDepth() == 4);

        System.out.println("5");
        assert(configQueue.sendNumConnections() == 5);
        assert(configQueue.getSendNumConnections() == 5);

        System.out.println("6");
        assert(configQueue.sendQueueDepth() == 5);
        assert(configQueue.getSendQueueDepth() == 5);

        GrabbyHands grabbyHands = new GrabbyHands(config);
        System.out.println("pass testCreate");
    }

    protected void testWriteRead() {
        System.out.println("run testWriteRead");
        // TODO: Implement
        System.exit(-1);
    }

    public static void main(String[] args) {
        JavaTest javaTest = new JavaTest();
        javaTest.run();
    }
}
