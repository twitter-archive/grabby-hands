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
  grabbyHands: GrabbyHands,
  connectionName: String,
  queueCounters: QueueCounters,
  server: String,
  queueName: String,
  recvQueue: BlockingQueue[String],
  send: ConnectionSend
) extends ConnectionBase(grabbyHands,
                         connectionName,
                         "ConnectionRecv",
                         server) {
  val host = server.split(":")(0)
  val port = Integer.parseInt(server.split(":")(1))
  val maxMessageBytes = grabbyHands.config.maxMessageBytes
  protected val request = ByteBuffer.wrap((
    "get " + queueName + "/t=" + grabbyHands.config.kestrelReadTimeoutMs + "\r\n").getBytes)
  protected val expectEnd = ByteBuffer.wrap("END\r\n".getBytes)
  protected val expectHeader = ByteBuffer.wrap(("VALUE " + queueName + " 0 ").getBytes)
  // expectLengthMax cannot be too large, else very short messages cannot be read.
  // 1 byte message = 1 byte length + \r\n + 1 byte payload + 3 bytes END + \r\n
  // = 9, so cannot be larger than 9, else will block and deadlock
  protected val expectLengthMax = 8 // \r\n + 6 length digits
  protected val buffer = ByteBuffer.allocate(maxMessageBytes + 100)
  protected var socket: SocketChannel = _

  override def run2() {
    while (haltLatch.getCount() > 0) {
      try {
        socket = SocketChannel.open(new InetSocketAddress(host, port))
        var connected = true

        while (connected && haltLatch.getCount() > 0) {
          connected = process()
        }
      } catch {
        case ex: Exception => {
          log.info("ConnectionRecv " + connectionName + " exception " + ex.toString())
          serverCounters.connectionExceptions.getAndIncrement()
        }
      }
      socket.close()
    }
  }

  def process(): Boolean = {
    request.rewind()
    while (request.hasRemaining()) {
      socket.write(request)
    }

    buffer.rewind()
    // Read Shortest Possible Response
    while (buffer.hasRemaining()) {
      socket.read(buffer)
    }

    val oldPosition = buffer.position()

    buffer.flip()
    expectEnd.rewind()
    if (buffer == expectEnd) {
      // Empty read, timeout
      queueCounters.readTimeouts.getAndIncrement()
      return true
    }
    // Read looking for expected header
    buffer.position(oldPosition)
    buffer.limit(expectHeader.limit)
    while (buffer.hasRemaining()) {
      socket.read(buffer)
    }
    buffer.rewind()
    expectHeader.rewind()
    if (buffer != expectHeader) {
      log.warning("ConnectionRecv " + connectionName + " protocol error on header")
      serverCounters.protocolError.getAndIncrement()
      queueCounters.protocolError.getAndIncrement()
      return false
    }

    buffer.rewind()
    buffer.limit(expectLengthMax)
    while (buffer.position < expectLengthMax) {
      socket.read(buffer)
    }
    // At this point here, this is where you throw up your hands about how bad
    // the memcache protocol is.
    buffer.rewind()
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
    if (buffer.position() == buffer.limit() && payloadLength > maxMessageBytes ) {
      // Read to end of buffer w/o a valid integer
      log.warning("ConnectionRecv " + connectionName + " protocol error on length")
      serverCounters.protocolError.getAndIncrement()
      queueCounters.protocolError.getAndIncrement()
      return false
    }
    // Read just the payload
    buffer.compact()
    buffer.limit(payloadLength)
    while (buffer.hasRemaining()) {
      socket.read(buffer)
    }
    buffer.flip()
    val payload = buffer.asCharBuffer().toString()
    recvQueue.offer(payload, 999999, TimeUnit.HOURS)

    // Consume the END footer
    buffer.rewind()
    expectEnd.rewind()
    buffer.limit(expectEnd.limit())
    while (buffer.hasRemaining()) {
      socket.read(buffer)
    }
    if (buffer != expectEnd) {
      log.warning("ConnectionRecv " + connectionName + " protocol error on footer")
      serverCounters.protocolError.getAndIncrement()
      queueCounters.protocolError.getAndIncrement()
      return false
    }
    true
  }
}
