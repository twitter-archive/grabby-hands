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
import java.nio.channels.{SelectionKey, Selector, SocketChannel}
import java.util.logging.Logger

object AdHocRequest {
  val log = Logger.getLogger("grabbyhands")

  def deleteQueue(queue: String, host: String, port: Int) {
    log.fine("delete queue " + queue + " " + host + ":" + port)
    request("delete " + queue + "\r\n", 100, "END", host, port, 2000)
    log.fine("delete queue " + queue + " " + host + ":" + port)
  }

  def request(request: String,
              responseMaxBytes: Int,
              terminator: String,
              host: String,
              port: Int,
              timeoutMs: Int): String = {
    val socket = SocketChannel.open()
    socket.configureBlocking(false)
    socket.connect(new InetSocketAddress(host, port))
    val selector = Selector.open();
    socket.register(selector, SelectionKey.OP_CONNECT)
    if (selector.select(timeoutMs) == 0 || !socket.finishConnect()) {
      socket.close()
      throw new Exception("connect timeout " + host + ":" + port)
    }

    val req = ByteBuffer.wrap(request.getBytes)
    req.rewind()
    while (req.hasRemaining()) {
      socket.write(req)
    }

    socket.register(selector, SelectionKey.OP_READ)
    val response = ByteBuffer.allocate(responseMaxBytes)
    response.rewind()

    while (response.hasRemaining()) {
      if (selector.select(timeoutMs) == 0) {
        socket.close()
        throw new Exception("read timeout " + host + ":" + port)
      }
      socket.read(response)
      response.flip()
      val rv = new String(response.array(), 0, response.limit())
      if (rv.contains(terminator)) {
        socket.close()
        return rv
      }
      response.position(response.limit())
      response.limit(response.capacity())
    }
    socket.close()
    throw new Exception ("response not found")
    ""
  }
}
