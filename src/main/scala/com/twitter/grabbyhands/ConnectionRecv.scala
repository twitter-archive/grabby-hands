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

import java.lang.StringBuilder
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.{BlockingQueue, TimeUnit}

// ConnectionRecv owns the socket, ConnectionSend only writes into the socket if available.
protected class ConnectionRecv(
  queue: Queue,
  connectionName: String,
  server: String
) extends ConnectionBase(queue,
                         connectionName,
                         server) {
  val recvQueue = queue.recvQueue

  val maxMessageBytes = grabbyHands.config.maxMessageBytes
  protected val request = ByteBuffer.wrap((
    "get " + queueName + "/t=" + grabbyHands.config.kestrelReadTimeoutMs + "\r\n").getBytes)
  protected val expectEnd = ByteBuffer.wrap("END\r\n".getBytes)
  protected val expectHeader = ByteBuffer.wrap(("VALUE " + queueName + " 0 ").getBytes)
  protected val buffer = ByteBuffer.allocate(maxMessageBytes + 100)

  def run2(): Boolean = {
    log.finest(connectionName + " send start")
    request.rewind()
    while (request.hasRemaining()) {
      if (haltLatch.getCount() == 0) {
        return false
      }
      if (selectWrite() == 0) {
        serverCounters.connectionWriteTimeout.incrementAndGet()
        log.fine(connectionName + " timeout writing request")
        return false
      }
      socket.write(request)
    }

    log.finest(connectionName + " read header")
    // Both valid responses end with an expectEnd buffer
    buffer.rewind()
    buffer.limit(buffer.capacity())
    // Read until at least expectEnd bytes arrive
    while (buffer.position() < expectEnd.capacity()) {
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
    // May have END\r\n
    log.finest(connectionName + " read bytes" + buffer.position())

    var oldPosition = buffer.position()
    buffer.flip()
    expectEnd.rewind()
    if (buffer == expectEnd) {
      // Empty read, timeout
      log.finest(connectionName + " kestrel get timeout")
      queueCounters.kestrelGetTimeouts.getAndIncrement()
      return true
    }

    // Read enough for expected header and at least a 1 digit length
    buffer.position(oldPosition)
    while (buffer.position() < expectHeader.capacity() + 1) {
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
    log.finest(connectionName + " read bytes" + buffer.position())

    // At this point here, this is where you throw up your hands about how bad
    // the memcache protocol is.
    // Determine length of message
    oldPosition = buffer.position() // beginning of length
    var found = false
    while (!found && buffer.hasRemaining()) {
      if (buffer.get() == '\n') {
        found == true
      } else if (!buffer.hasRemaining()) {
        // Read more, in the unlikely case that the entire header didn't come over at once
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
    }
    if (!buffer.hasRemaining()) {
      log.fine(connectionName + " protocol error on header reading length")
      serverCounters.protocolError.getAndIncrement()
      queueCounters.protocolError.getAndIncrement()
      return false
    }
    val lengthEnd = buffer.position()
    val lengthLength = lengthEnd - oldPosition
    buffer.position(oldPosition)
    val lengthBuffer = new Array[Byte](lengthLength)
    buffer.get(lengthBuffer)
    val payloadLength = Integer.parseInt(new String(lengthBuffer))
    log.finest(connectionName + " payload length " + payloadLength)

    // Ensure that entire payload plus an expectEnd can be read
    val payloadStart = lengthEnd + 1
    val payloadEnd = payloadStart + payloadLength
    val responseEnd = payloadEnd + expectEnd.capacity()
    buffer.position(payloadStart)

    while (buffer.limit() < responseEnd) {
      if (haltLatch.getCount() == 0) {
        return false
      }
      if (selectRead() == 0) {
        serverCounters.connectionReadTimeout.incrementAndGet()
        log.fine(connectionName + " timeout reading payload")
        return false
      }
      socket.read(buffer)
    }

    /*
    var payloadLength = 0
    val lengthBuffer = new StringBuilder(expectLengthMax)
    while (payloadLength == 0 && buffer.position() < buffer.limit()) {
      val ch = buffer.get()
      if (ch >= '0' && ch <= '9') {
        lengthBuffer.append(ch)
      } else if (ch == '\n') {
        // just ignore the \r
        payloadLength = Integer.parseInt(lengthBuffer.toString())
      }
    }
    */
    buffer.position(payloadStart)
    val payload = new String(buffer.array(), 0, buffer.limit())
    recvQueue.offer(payload, 999999, TimeUnit.HOURS)

    // Consume the END footer
    buffer.rewind()
    expectEnd.rewind()
    buffer.position(payloadEnd)
    if (buffer != expectEnd) {
      log.fine(connectionName + " protocol error on footer")
      serverCounters.protocolError.getAndIncrement()
      queueCounters.protocolError.getAndIncrement()
      return false
    }
    true
  }
}
