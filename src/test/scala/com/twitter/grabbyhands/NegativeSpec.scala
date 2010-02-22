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

object NegativeSpec extends SpecBase(2) {

  "negative" should {

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

    "tolerate down kestrel" in {
      // assume that nothing is running on (default kestrel port + 10)
      val badServer = host + ":" + (port + 10)
      config = new Config()
      config.addServer(badServer)
      config.connectTimeoutMs = 200
      config.addQueue(queue)

      ctor()

      // Assume reconnect holddown will prevent more than one connect attempt
      config.reconnectHolddownMs must be_>(config.connectTimeoutMs * 2)
      Thread.sleep(config.connectTimeoutMs + 50)

      val serverCount = grab.serverCounters(badServer)
      serverCount.connectionOpenAttempt.get must be_==(2) // One for each direction
      serverCount.connectionOpenSuccess.get must be_==(0)
      serverCount.connectionOpenTimeout.get must be_==(0)
      serverCount.connectionCurrent.get must be_==(0)
      serverCount.connectionExceptions.get must be_==(2)
    }

    "make progress with only one up kestrel and several down kestrels" in {
      // Send more than one message to avoid a fluke delivery. Expect all to arrive quickly.
      val depth = 20
      config = new Config()
      val badServer = host + ":" + (port + 10)
      config.addServer(hostPort)
      // assume that nothing is running on (default kestrel port + 10 ...)
      val badHostPort = host + ":" + (port + 10)
      config.addServer(badHostPort)
      config.connectTimeoutMs = 100
      config.reconnectHolddownMs = 10
      config.sendQueueDepth = depth
      config.recvQueueDepth = depth
      config.addQueue(queues(0))
      ctor()

      val send = grab.getSendQueue(queue)
      val recv = grab.getRecvQueue(queue)

      // Wait for a connection failure.
      Thread.sleep(config.connectTimeoutMs + 50)

      val text = new Array[String](depth + 1)
      val writes = new Array[Write](depth + 1)
      for (idx <- 1 to depth) {
        text(idx) = "text" + idx
        writes(idx) = new Write(text(idx))
        send.put(writes(idx))
      }
      Thread.sleep(100)
      val startMs = System.currentTimeMillis
      for (idx <- 1 to depth) {
        val buffer = recv.poll(200, TimeUnit.MILLISECONDS)
        buffer must notBeNull
        new String(buffer.array) must be_==(text(idx))
        writes(idx).written must beTrue
      }
      val endMs = System.currentTimeMillis
      endMs - startMs must be_<(500L)

      val goodServerCount = grab.serverCounters(hostPort)
      goodServerCount.connectionOpenAttempt.get must be_==(2) // One for each direction
      goodServerCount.connectionOpenSuccess.get must be_==(2)
      goodServerCount.connectionOpenTimeout.get must be_==(0)
      goodServerCount.connectionCurrent.get must be_==(2)
      goodServerCount.connectionExceptions.get must be_==(0)

      val badServerCount = grab.serverCounters(badHostPort)
      // One for each direction
      badServerCount.connectionOpenAttempt.get.asInstanceOf[Long] must be_>=(2L)
      badServerCount.connectionOpenSuccess.get must be_==(0)
      badServerCount.connectionOpenTimeout.get must be_==(0)
      badServerCount.connectionCurrent.get must be_==(0)
      badServerCount.connectionExceptions.get.asInstanceOf[Long] must be_>=(2L)
    }

    "recover from receiving messages beyond expected length" in {
      config.maxMessageBytes = 20
      ctor()
      val errorText = genAsciiString(config.maxMessageBytes + 1)
      errorText.length must be_==(config.maxMessageBytes + 1)
      grab.getSendQueue(queue).put(new Write(errorText))

      val serverCount = grab.serverCounters(hostPort)
      val queueCount = grab.queueCounters(queue)

      var retries = 100
      while (retries > 0 && queueCount.protocolError.get == 0) {
        retries -= 1
        Thread.sleep(25)
      }
      queueCount.protocolError.get must be_==(1)
      queueCount.messagesSent.get must be_==(1)
      queueCount.bytesSent.get must be_==(errorText.length)
      queueCount.messagesRecv.get must be_==(0)
      queueCount.bytesRecv.get must be_==(0)

      serverCount.protocolError.get must be_==(1)
      serverCount.messagesSent.get must be_==(1)
      serverCount.bytesSent.get must be_==(errorText.length)
      serverCount.messagesRecv.get must be_==(0)
      serverCount.bytesRecv.get must be_==(0)

      // See if we can recover
      val sendText = genAsciiString(config.maxMessageBytes)
      sendText.length must be_==(config.maxMessageBytes)
      grab.getSendQueue(queue).put(new Write(sendText))

      val buffer = grab.getRecvQueue(queue).poll(2, TimeUnit.SECONDS)
      buffer must notBeNull

      val recvText = new String(buffer.array)
      recvText must be_==(sendText)

      queueCount.protocolError.get must be_==(1)
      queueCount.messagesSent.get must be_==(2)
      queueCount.bytesSent.get must be_==(errorText.length + sendText.length)
      queueCount.messagesRecv.get must be_==(1)
      queueCount.bytesRecv.get must be_==(sendText.length)

      serverCount.protocolError.get must be_==(1)
      serverCount.messagesSent.get must be_==(2)
      serverCount.bytesSent.get must be_==(errorText.length + sendText.length)
      serverCount.messagesRecv.get must be_==(1)
      serverCount.bytesRecv.get must be_==(sendText.length)
    }
  }
}
