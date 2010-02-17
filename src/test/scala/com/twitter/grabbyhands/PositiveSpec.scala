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

object PositiveSpec extends SpecBase {

  def genAsciiString(length: Int): String = {
    val sb = new StringBuffer()
    for (idx <- 0 to length - 1) {
      sb.append(('a' + (idx % ('z' - 'a'))).asInstanceOf[Char])
    }
    sb.toString
  }

  def genBinaryString(length: Int): String = {
    val sb = new StringBuffer()
    for (idx <- 1 to length) {
      sb.append(idx.asInstanceOf[Char])
    }
    sb.toString
  }

  "positive" should {

    doBefore {
      defaults()
      grab = null
    }

    doAfter {
      if (grab != null) {
        grab.join()
        grab.counters.threads.get must be_==(0)
      }
    }

    "write one, read one" in {
      // Delete queue before starting connections, which will recreate queue.
      val adhoc = new AdHocRequest(new ServerCounters(), hostPort)
      adhoc.deleteQueue(queue)

      ctor()
      grab must notBeNull
      val send = grab.getSendQueue(queue)
      val recv = grab.getRecvQueue(queue)

      val sendText = "text"
      val write = new Write(sendText)
      write.written.getCount() must be_==(1)
      write.cancel.getCount() must be_==(1)

      log.fine("before write")
      send.put(write)

      log.fine("before read")
      val buffer = recv.poll(1, TimeUnit.SECONDS)
      buffer must notBeNull

      val recvText = new String(buffer.array)
      recvText must be_==(sendText)

      write.written.getCount() must be_==(0)
      write.cancel.getCount() must be_==(1)

      val serverCount = grab.serverCounters(hostPort)
      serverCount.protocolError.get must be_==(0)
      serverCount.connectionWriteTimeout.get must be_==(0)
      serverCount.connectionReadTimeout.get must be_==(0)
      serverCount.messagesSent.get must be_==(1)
      serverCount.bytesSent.get must be_==(sendText.length)
      serverCount.messagesRecv.get must be_==(1)
      serverCount.bytesRecv.get must be_==(sendText.length)

      val queueCount = grab.queueCounters(queue)
      queueCount.protocolError.get must be_==(0)
      queueCount.messagesSent.get must be_==(1)
      queueCount.bytesSent.get must be_==(sendText.length)
      queueCount.messagesRecv.get must be_==(1)
      queueCount.bytesRecv.get must be_==(sendText.length)
      queueCount.kestrelGetTimeouts.get must be_==(0)
    }

    "connection counters" in {
      ctor()
      grab must notBeNull
      val serverCount = grab.serverCounters(hostPort)
      serverCount.connectionOpenAttempt.get must be_==(2) // One for each direction
      serverCount.connectionOpenSuccess.get must be_==(2)
      serverCount.connectionOpenTimeout.get must be_==(0)
      serverCount.connectionCurrent.get must be_==(2)
      serverCount.connectionExceptions.get must be_==(0)

      grab.join()
      serverCount.connectionCurrent.get must be_==(0)
      grab = null
    }

    "read empty queue" in {
      // Make kestrelReadTimeout much larger than readTimeout to avoid confusion
      val baseMs = 250
      val factor = 6
      config.readTimeoutMs = baseMs
      config.kestrelReadTimeoutMs = config.readTimeoutMs * factor

      val rounds = 2
      var sleepMs: Int = rounds * config.kestrelReadTimeoutMs
      // Normal read timeout is also included
      sleepMs += config.readTimeoutMs
      // Add a little bit more for slop
      sleepMs += config.readTimeoutMs >> 2
      sleepMs must be_<((rounds + 1) * config.kestrelReadTimeoutMs)

      ctor()
      grab.config.kestrelReadTimeoutMs must be_==(baseMs * factor)
      Thread.sleep(sleepMs)

      val queueCount = grab.queueCounters(queue)
      queueCount.kestrelGetTimeouts.get must be_==(rounds)
      queueCount.bytesRecv.get must be_==(0)
      queueCount.messagesRecv.get must be_==(0)
      queueCount.bytesSent.get must be_==(0)
      queueCount.messagesSent.get must be_==(0)
      queueCount.protocolError.get must be_==(0)
    }

    "messages of varying length" in {
      fail("XXX")
    }

    "messages with reserved characters" in {
      // newlines, END\r\n, VALUE, etc.
      fail("XXX")
    }

    "message up to length limit" in {
      ctor()
      val sendText = genAsciiString(shortMessageMax)
      sendText.length must be_==(shortMessageMax)
      grab.getSendQueue(queue).put(new Write(sendText))
      val buffer = grab.getRecvQueue(queue).poll(1, TimeUnit.SECONDS)
      buffer must notBeNull
      val recvText = new String(buffer.array)
      recvText must be_==(sendText)
    }

    "huge message" in {
      config.maxMessageBytes = 524288
//      config.maxMessageBytes = 68000
      ctor()
      val sendText = genAsciiString(config.maxMessageBytes)
      sendText.length must be_==(config.maxMessageBytes)
      grab.getSendQueue(queue).put(new Write(sendText))
      val buffer = grab.getRecvQueue(queue).poll(10, TimeUnit.SECONDS)
      buffer must notBeNull
      val recvText = new String(buffer.array)
      recvText.equals(sendText) must be_==(true)

      val queueCount = grab.queueCounters(queue)
      queueCount.protocolError.get must be_==(0)
      queueCount.messagesSent.get must be_==(1)
      queueCount.bytesSent.get must be_==(sendText.length)
      queueCount.messagesRecv.get must be_==(1)
      queueCount.bytesRecv.get must be_==(sendText.length)
      queueCount.kestrelGetTimeouts.get must be_==(0)
    }

    "binary messages" in {
      ctor()
      fail("XXX")
    }

    "utf-8 messages" in {
      ctor()
      fail("XXX")
    }

    "send two, receive two" in {
      ctor()
      fail("XXX")
    }

    "cancel a message" in {
      fail("XXX")
    }
  }
}
