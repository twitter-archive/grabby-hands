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
import java.util.concurrent.TimeUnit

object BasicSpecStress extends SpecBase(50) {

  def basicStress(testMessages:Int, testNumQueues: Int,
                  testConnectionsPerServer: Int, testLength: Int)(readWrite: Boolean) {
    config = new Config()
    config.addServer(host + ":" + port)
    config.maxMessageBytes = testLength
    config.sendQueueDepth = 100
    config.recvQueueDepth = 100
    testNumQueues must be_<=(numQueues)
    val testQueues = queues.slice(0, testNumQueues).force
    testQueues.size must be_==(testNumQueues)
    val queueConfig = config.addQueues(testQueues)

    ctor()
    grab must notBeNull
    val send = grab.getSendQueue(queue) // XXX needs to be an array of queues
    val recv = grab.getRecvQueue(queue) // XXX needs to be an array of queues

    val sendText = genAsciiString(testLength)

    var startMs = System.currentTimeMillis
    var lastWrite: Write = null
    for (idx <- 1 to testMessages) {
      lastWrite = new Write(sendText)
      send.put(lastWrite)
    }
    if (!readWrite) {
      log.fine("waiting for last message to write")
      lastWrite.awaitWrite(1, TimeUnit.MINUTES)
    }
    var endMs = System.currentTimeMillis
    var deltaMs = endMs - startMs
    log.info("enqueue " + testMessages + " in " + deltaMs + " ms, " +
             ((1000*testMessages) / deltaMs) + " messages/sec")

    startMs = System.currentTimeMillis
    for (idx <- 1 to testMessages) {
      val buffer = recv.poll(250, TimeUnit.SECONDS)
      buffer must notBeNull
      val recvTest = new String(buffer.array)
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
      val adhoc = new AdHocRequest(new ServerCounters(), hostPort)
      queues.foreach(queue => adhoc.deleteQueue(queue))

      defaults()
      grab = null
    }

    doAfter {
      if (grab != null) {
        grab.join()
        grab.counters.threads.get must be_==(0)
      }
    }

    "many messages to one queue one connection" in {
      def fn = basicStress(10000, 1, 1, 2048)_
      "serial" in {
        fn(false)
      }
      "parallel" in {
        fn(true)
      }
    }

    "many messages to one queue many connections" in {
      def fn = basicStress(10000, 1, 10, 2048)_
      "serial" in {
        fn(false)
      }
      "parallel" in {
        fn(true)
      }
    }

    "many messages to many queues one server" in {
    }

    "several large messages to one queue one server" in {
      def fn = basicStress(5000, 1, 1, 32000)_
      "serial" in {
        fn(false)
      }
      "parallel" in {
        fn(true)
      }
    }
  }
}
