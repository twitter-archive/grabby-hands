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

object DownKestrelSpecStress extends SpecBase(50) {

  def connStress(host: String, port: Int) {
    val testNumQueues = 1
    val testLength = 950
    val testMessages = 5

    config = new Config()
    config.addServer(host + ":" + port)
    config.maxMessageBytes = 1000
    config.sendQueueDepth = 225
    config.recvQueueDepth = 225
    config.kestrelReadTimeoutMs = 5
    config.connectTimeoutMs = 5
    config.readTimeoutMs = 5
    config.writeTimeoutMs = 5
    config.reconnectHolddownMs = 5
    testNumQueues must be_<=(numQueues)
    val testQueues = queues.slice(0, testNumQueues).toArray
    val queueConfig = config.addQueues(testQueues)

    ctor()
    grab must notBeNull
    val recvQueues = new Array[BlockingQueue[ByteBuffer]](testNumQueues)
    val sendQueues = new Array[BlockingQueue[Write]](testNumQueues)
    for (idx <- 0 to testNumQueues - 1) {
      log.warning("queue " + idx + " " + queues(idx))
      recvQueues(idx) = grab.getRecvQueue(queues(idx))
      sendQueues(idx) = grab.getSendQueue(queues(idx))
    }

    val sendText = genAsciiString(testLength)

    var lastWrite: Write = null
    var queueIdx = 0
    for (idx <- 1 to testMessages) {
      lastWrite = new Write(sendText)
      sendQueues(queueIdx).put(lastWrite)
      queueIdx += 1
      if (queueIdx == testNumQueues) queueIdx = 0
    }
  }


  "downkestrelstress" should {
    val seconds = 15

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
        Thread.sleep(250)
      }
    }

    "localhost unused port" in {
      connStress("localhost", 22199)
      Thread.sleep(seconds * 1000)
    }

    "192.168.0.254 unused port" in {
      connStress("192.168.0.254", 22199)
      Thread.sleep(seconds * 1000)
    }

    "gromit unused port" in {
      connStress("gromit", 22199)
      Thread.sleep(seconds * 1000)
    }
  }
}
