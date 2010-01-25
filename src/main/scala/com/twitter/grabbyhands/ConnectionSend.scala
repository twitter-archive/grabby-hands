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

// ConnectionRecv owns the socket, ConnectionSend only writes into the socket if available.
protected class ConnectionSend(
  grabbyHands: GrabbyHands,
  connectionName: String,
  queueCounters: QueueCounters,
  server: String,
  queueName: String,
  sendQueue: BlockingQueue[Write]
) extends ConnectionBase(grabbyHands,
                         connectionName,
                         "ConnectionSend",
                         server) {
  var socket: SocketChannel = _

  override def run2() {
    var threadLocalSocket: SocketChannel = null
    var written = false
    var write: Write = null
    val sendRetryMs = grabbyHands.config.sendRetryMs

    val newlineBuffer = ByteBuffer.wrap("\r\n".getBytes())
    val buffers = new Array[ByteBuffer](5)
    buffers.update(0, ByteBuffer.wrap(("set " + queueName + " 0 0 ").getBytes()))
    // 1 set to each length
    buffers.update(2, newlineBuffer)
    // 3 set to each payload
    buffers.update(4, newlineBuffer)

    while (haltLatch.getCount() > 0) {
      write = null
      while (write == null && haltLatch.getCount() > 0) {
        write = sendQueue.poll(1, TimeUnit.SECONDS)
      }
      log.finest(connectionType + " " + connectionName + " new message")

      written = false
      while (!written && write.cancel.getCount() > 0 && haltLatch.getCount() > 0) {
        synchronized {
          while (socket == null) {
            log.finest(connectionType + " " + connectionType + " socket null, wait")
            this.wait()
          }
          threadLocalSocket = socket
        }
        buffers(1) = ByteBuffer.wrap(Integer.toString(write.message.limit).getBytes())
        buffers(3) = write.message
        try {
          threadLocalSocket.write(buffers.toArray)
        } catch {
          case ex: IOException => {
            threadLocalSocket.close()
            serverCounters.connectionExceptions.incrementAndGet()
            log.fine(connectionName + " write exception " + ex.toString())
            write.message.rewind()
          }
        }
        write.written.countDown()
        written = true
        if (!written) {
          Thread.sleep(sendRetryMs)
        }
      }
    }
  }

  protected[grabbyhands] def setSocket(socket: SocketChannel) {
    synchronized {
      this.socket = socket
      this.notify()
    }
  }

}
