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

/**
 * Performs meta requests on the Kestrel cluster, for example, deleting queues.
 *
 * @params server         Kestrel server to query, in host:port format.
 * @params serverCounters Optional statistics collection object.
 */
// TODO rename arg -- change socket to do so
protected[grabbyhands] class MetaRequest(serverArg: String,
                                         serverCountersArg: Option[ServerCounters]) extends Socket {
  def this(serverArg: String) = this(serverArg, None)
  server = serverArg
  socketName = "adhocrequest:" + server
  if (serverCountersArg.isDefined) {
    serverCounters = serverCountersArg.get
  } else {
    serverCounters = new ServerCounters()
  }

  /**
   * Deletes a queue.
   *
   * @params queue Name of queue to delete.
   */
  def deleteQueue(queue: String) {
    log.fine("start delete queue " + queue + " " + server)
    request("delete " + queue + "\r\n", 100, "END")
    log.fine("end delete queue " + queue + " " + server)
  }

  /**
   * Performs a low-level query on the server.
   *
   * @params request          String to send to server
   * @params responseMaxBytes Largest expected response
   * @params terminator       Response expected from server
   */
  def request(request: String, responseMaxBytes: Int, terminator: String): String = {
    openBlock()
    val req = ByteBuffer.wrap(request.getBytes)
    req.rewind()
    while (req.hasRemaining()) {
      if (!selectWrite()) {
        close()
        throw new Exception("write timeout " + server)
      }
      socket.write(req)
    }

    val response = ByteBuffer.allocate(responseMaxBytes)
    response.rewind()

    while (response.hasRemaining()) {
      if (!selectRead()) {
        close()
        throw new Exception("read timeout " + server)
      }
      socket.read(response)
      response.flip()
      val rv = new String(response.array(), 0, response.limit())
      if (rv.contains(terminator)) {
        close()
        return rv
      }
      response.position(response.limit())
      response.limit(response.capacity())
    }

    close()
    throw new Exception ("response not found")
    ""
  }
}
