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

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.{BlockingQueue, TimeUnit}
import java.util.logging.Level

protected class ConnectionSend(
  queue: Queue,
  connectionName: String,
  server: String
) extends ConnectionBase(queue,
                         connectionName,
                         server) {
  protected val sendQueue = queue.sendQueue

  protected var requestLength = 0
  protected val requests = new Array[ByteBuffer](5)
  requests.update(0, ByteBuffer.wrap(("set " + queueName + " 0 0 ").getBytes))
  // 1 set to each length
  requests.update(2, ByteBuffer.wrap("\r\n".getBytes))
  // 3 set to each payload
  requests.update(4, ByteBuffer.wrap("\r\n".getBytes))
  protected val staticLength = {
    var rv = 0
    requests.foreach(buffer => if (buffer != null) rv += buffer.capacity)
    rv
  }

  protected val expected = ByteBuffer.wrap("STORED\r\n".getBytes)
  protected val response = ByteBuffer.allocate(expected.capacity)

  protected var written = false
  protected var write: Write = null

  override def run2(): Boolean = {
    while (write == null) {
      if (haltLatch.getCount == 0) {
        return false
      }
      // Keep wait here symmetrical with recv side so pause is consistent
      write = sendQueue.poll(grabbyHands.config.kestrelReadTimeoutMs, TimeUnit.MILLISECONDS)
      if (write == null) {
        return true
      } else {
        // Don't redo this work when retrying same message
        requests(0).rewind()
        requests(1) = ByteBuffer.wrap(Integer.toString(write.message.limit).getBytes)
        requests(2).rewind()
        requests(3) = write.message
        requests(4).rewind()
        requestLength = staticLength + requests(1).capacity + requests(3).capacity
      }
    }

    if (write.cancel.getCount == 0) {
      queueCounters.sendCancelled.incrementAndGet()
      log.finer(connectionName + " write canceled")
      return true
    }

    // Start over with a fresh length
    if (!writeBufferVector(requestLength, requests)) {
      return false
    }

    response.clear()
    if (!readBuffer(response.capacity, response)) {
      return false
    }
    response.flip()

    if (response != expected) {
      serverCounters.protocolError.incrementAndGet()
      queueCounters.protocolError.incrementAndGet()
      response.rewind()
      log.warning(
        connectionName + " protocol error didn't get STORED, found |" +
        new String(response.array, 0, response.limit) + "|")
      // Disconnect on protocol error
      return false
    }

    queueCounters.messagesSent.incrementAndGet()
    serverCounters.messagesSent.incrementAndGet()
    queueCounters.bytesSent.addAndGet(write.message.capacity)
    serverCounters.bytesSent.addAndGet(write.message.capacity)
    write.written.countDown()
    write = null // Message sent, do not retry
    if (log.isLoggable(Level.FINEST)) log.finest(connectionName + " wrote ok " + requestLength)
    true
  }
}
