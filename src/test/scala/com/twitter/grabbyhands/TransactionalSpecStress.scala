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
package com.twitter.grabbyhands

import java.nio.ByteBuffer
import java.util.concurrent.{BlockingQueue,TimeUnit}

object TransactionalSpecStress extends SpecBase(50) {

  def transactionalStress(testMessages:Int, testNumQueues: Int,
                  testConnectionsPerServer: Int, testLength: Int, serial: Boolean) {
    log.fine("testNumQueues " + testNumQueues + " testConnectionsPerServer " +
             testConnectionsPerServer + " serial " + serial)
    testMessages % testNumQueues must be_==(0)
    config = new Config()
    config.addServer(host + ":" + port)
    config.maxMessageBytes = testLength
    config.sendQueueDepth = 225
    config.recvQueueDepth = 225
    config.reconnectHolddownMs = 50
    config.recvTransactional = true
    testNumQueues must be_<=(numQueues)
    val testQueues = queues.slice(0, testNumQueues).force
    val queueConfig = config.addQueues(testQueues)

    ctor()
    grab must notBeNull
    val recvQueues = new Array[BlockingQueue[Read]](testNumQueues)
    val sendQueues = new Array[BlockingQueue[Write]](testNumQueues)
    for (idx <- 0 to testNumQueues - 1) {
      log.warning("queue " + idx + " " + queues(idx))
      recvQueues(idx) = grab.getRecvTransQueue(queues(idx))
      sendQueues(idx) = grab.getSendQueue(queues(idx))
    }

    val sendText = genAsciiString(testLength)

    var startMs = System.currentTimeMillis
    var lastWrite: Write = null
    var queueIdx = 0
    for (idx <- 1 to testMessages) {
      lastWrite = new Write(sendText)
      sendQueues(queueIdx).put(lastWrite)
      queueIdx += 1
      if (queueIdx == testNumQueues) queueIdx = 0
    }
    if (serial) {
      log.fine("waiting for last message to write")
      lastWrite.awaitWrite(1, TimeUnit.MINUTES)
    }
    var endMs = System.currentTimeMillis
    var deltaMs = endMs - startMs
    log.info("enqueue " + testMessages + " in " + deltaMs + " ms, " +
             ((1000*testMessages) / deltaMs) + " messages/sec")

    startMs = System.currentTimeMillis
    for (idx <- 1 to testMessages) {
      val buffer = recvQueues(queueIdx).poll(250, TimeUnit.SECONDS)
      queueIdx += 1
      if (queueIdx == testNumQueues) queueIdx = 0
      buffer must notBeNull
      val recvTest = new String(buffer.message.array)
      buffer.close(true)
      recvTest must be_==(sendText)
    }
    endMs = System.currentTimeMillis
    deltaMs = endMs- startMs

    log.info("dequeue " + testMessages + " in " + deltaMs + " ms, " +
             ((1000*testMessages) / deltaMs) + " messages/sec")
  }

  "basicstress" should {
    doBefore {
      noDetailedDiffs()  // else large string compare goes berzerk
      val meta = new MetaRequest(hostPort, None)
      queues.foreach(queue => meta.deleteQueue(queue))

      defaults()
      grab = null
    }

    doAfter {
      if (grab != null) {
        grab.join()
        grab.counters.threads.get must be_==(0)
      }
    }

    "many messages to one queue one connection serial" in {
      transactionalStress(10000, 1, 1, 2048, true)
    }

    "many messages to one queue one connection parallel" in {
      transactionalStress(10000, 1, 1, 2048, false)
    }

    "many messages to one queue many connections serial" in {
      transactionalStress(10000, 1, 10, 2048, true)
    }

    "many messages to one queue many connections parallel" in {
      transactionalStress(10000, 1, 10, 2048, false)
    }

    "many messages to many queues many connections serial" in {
      transactionalStress(10000, 10, 10, 2048, true)
    }

    "many messages to many queues many connections parallel" in {
      transactionalStress(10000, 10, 10, 2048, false)
    }

    "several large messages to one queue one server serial" in {
      transactionalStress(5000, 1, 1, 32000, true)
    }

    "several large messages to one queue one server parallel" in {
      transactionalStress(5000, 1, 1, 32000, false)
    }
  }
}
