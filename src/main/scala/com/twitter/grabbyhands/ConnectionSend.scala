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
import scala.collection.mutable.ArrayBuffer

protected class ConnectionSend(
  queue: Queue,
  connectionName: String,
  server: String
) extends ConnectionBase(queue,
                         connectionName,
                         server) {
  protected val sendQueue = queue.sendQueue

  protected val newlineBuffer = ByteBuffer.wrap("\r\n".getBytes())
  protected val buffers = new Array[ByteBuffer](5)
  buffers.update(0, ByteBuffer.wrap(("set " + queueName + " 0 0 ").getBytes()))
  // 1 set to each length
  buffers.update(2, newlineBuffer)
  // 3 set to each payload
  buffers.update(4, newlineBuffer)
  protected val staticLength = {
    var rv = 0
    buffers.foreach(buffer => if (buffer != null) rv += buffer.capacity())
    rv
  }

  protected val expected = ByteBuffer.wrap("STORED\r\n".getBytes())
  protected val buffer = ByteBuffer.allocate(expected.capacity())

  protected var written = false
  protected var write: Write = null
  protected var writeLength = 0

  override def run2(): Boolean = {
    while (write == null) {
      if (haltLatch.getCount() == 0) {
        return false
      }
      write = sendQueue.poll(1, TimeUnit.SECONDS)
      if (write != null) {
        // Don't redo this work when retrying same message
        buffers(1) = ByteBuffer.wrap(Integer.toString(write.message.limit).getBytes())
        buffers(3) = write.message
        writeLength = staticLength + buffers(1).capacity() + buffers(3).capacity
      }
    }
    log.finest(connectionName + " message to send")

    if (write.cancel.getCount() == 0) {
      log.finest(connectionName + " write cancelled")
      return true
    }

    // Start over with a fresh length
    var remaining = writeLength.asInstanceOf[Long]

    while (remaining > 0) {
      if (haltLatch.getCount() == 0) {
        return false
      }
      if (selectWrite() == 0) {
        serverCounters.connectionWriteTimeout.incrementAndGet()
        log.fine(connectionName + " timeout writing response")
        return false
      }
      remaining -= socket.write(buffers.toArray)
    }
    log.finest(connectionName + " wrote")

    buffer.flip()
    while (buffer.hasRemaining()) {
      if (haltLatch.getCount() == 0) {
        return false
      }
      if (selectRead() == 0) {
        serverCounters.connectionReadTimeout.incrementAndGet()
        log.fine(connectionName + " timeout reading response")
        return false
      }
      socket.read(buffer)
    }

    if (buffer != expected) {
      serverCounters.protocolError.incrementAndGet()
      queueCounters.protocolError.incrementAndGet()
      log.fine(connectionName + " protocol error didn't get STORED")
    }

    queueCounters.messagesSent.incrementAndGet()
    serverCounters.messagesSent.incrementAndGet()
    queueCounters.bytesSent.addAndGet(writeLength)
    serverCounters.bytesSent.addAndGet(writeLength)
    write.written.countDown()

    true
  }
}
