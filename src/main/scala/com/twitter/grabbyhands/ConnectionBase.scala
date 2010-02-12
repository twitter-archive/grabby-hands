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

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, SocketChannel}
import java.util.concurrent.CountDownLatch

protected abstract class ConnectionBase(queue: Queue,
                                        connectionName: String,
                                        server: String) extends Thread with Socket {
  val grabbyHands = queue.grabbyHands
  val config = queue.config
  val queueCounters = queue.counters
  val queueName = config.name
  host = server.split(":")(0)
  port = Integer.parseInt(server.split(":")(1))
  connectTimeoutMs = grabbyHands.config.connectTimeoutMs
  readWriteTimeoutMs = grabbyHands.config.readWriteTimeoutMs
  protected val haltLatch = new CountDownLatch(1)
  protected val serverCounters = grabbyHands.serverCounters(server)

  this.setDaemon(true)

  override def run() {
    log.fine(connectionName + " thread start")
    grabbyHands.counters.threads.getAndIncrement()
    Thread.currentThread().setName(connectionName)

    while (haltLatch.getCount() > 0) {
      try {
        log.finer(connectionName + " open start")
        openBlock(haltLatch)
        log.finer(connectionName + " open end")
        var connected = true

        while (connected && haltLatch.getCount() > 0) {
          connected = run2()
        }

        if (!connected && haltLatch.getCount() > 0) {
          Thread.sleep(grabbyHands.config.reconnectHolddownMs)
        }
      } catch {
        case ex: Exception => {
          log.fine(connectionName + " exception " + ex.toString())
          serverCounters.connectionExceptions.getAndIncrement()
        }
      }
      close()
    }
    grabbyHands.counters.threads.getAndDecrement()
    log.fine(connectionName + " thread end")
  }

  def run2(): Boolean

  def halt() {
    log.fine(connectionName + " halt")
    haltLatch.countDown()
    this.interrupt()
  }
}
