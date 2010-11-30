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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.*;

public class JavaTest {
  protected List<String> servers = Arrays.asList("localhost:22133");
  protected String queue = "grabby_javatest";
  protected List<String> queues = Arrays.asList(queue);
  protected GrabbyHands grabbyHands = null;

  @Before @After public void cleanup() {
    if (grabbyHands != null) {
      grabbyHands.join();
      grabbyHands = null;
    }
  }

  @Test public void testCreate() {
    Config config = new Config();
    config.addServers(servers);

    assertFalse(config.getRecvTransactional());
    assertFalse(config.getSendTransactional());
    config.setRecvTransactional(true);

    config.setRecvNumConnections(4);
    config.setSendNumConnections(5);
    assertEquals(config.recvNumConnections(), 4);
    assertEquals(config.getRecvNumConnections(), 4);
    assertEquals(config.sendNumConnections(), 5);
    assertEquals(config.getSendNumConnections(), 5);
    assertTrue(config.getRecvTransactional());
    assertFalse(config.getSendTransactional());

    config.setMaxMessageBytes(100);
    assertEquals(config.getMaxMessageBytes(), 100);

    HashMap<String, ConfigQueue> queueConfigs = config.addQueues(queues);
    assertTrue(queueConfigs.containsKey(queues.get(0)));
    ConfigQueue configQueue = queueConfigs.get(queues.get(0));
    assertEquals(configQueue.recvNumConnections(), 4);
    assertEquals(configQueue.getRecvNumConnections(), 4);
    assertTrue(configQueue.getRecvTransactional());

    assertEquals(configQueue.recvQueueDepth(), 4);
    assertEquals(configQueue.getRecvQueueDepth(), 4);

    assertEquals(configQueue.sendNumConnections(), 5);
    assertEquals(configQueue.getSendNumConnections(), 5);

    assertEquals(configQueue.sendQueueDepth(), 5);
    assertEquals(configQueue.getSendQueueDepth(), 5);
  }

  @Test public void testWriteRead() {
    Config config = new Config();
    config.addServers(servers);
    config.setRecvTransactional(true);
    config.addQueues(queues);
    grabbyHands = new GrabbyHands(config);
    BlockingQueue<Write> send = grabbyHands.getSendQueue(queue);
    BlockingQueue<Read> recv = grabbyHands.getRecvTransQueue(queue);

    String sendText = "text";
    Write write = new Write(sendText);
    assertFalse(write.written());
    assertFalse(write.cancelled());
    try {
      send.put(write);
      Read read = recv.poll(4, TimeUnit.SECONDS);
      assertNotNull(read);

      String recvText = new String(read.getMessage().array());
      assertEquals(recvText, sendText);
      assertFalse(read.completed());
      assertFalse(read.cancelled());

      send.put(new Write(read));
      read.awaitComplete(4, TimeUnit.SECONDS);
      assertTrue(read.completed());
      assertFalse(read.cancelled());

      read = recv.poll(4, TimeUnit.SECONDS);
      assertNotNull(read);
      read.close(false);

      read = recv.poll(1, TimeUnit.SECONDS);
      assertNull(read);
    } catch (InterruptedException e) {
      fail("caught unexpected exception");
    }
  }
}
