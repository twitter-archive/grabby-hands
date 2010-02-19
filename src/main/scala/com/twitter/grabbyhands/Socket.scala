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
import java.util.concurrent.CountDownLatch
import java.util.logging.{Level, Logger}

trait Socket {
  val log = Logger.getLogger(GrabbyHands.logname)
  var socket: SocketChannel = _
  var opened = false
  //XXX See if we can go back to two selectors
  //  var readSelector: Selector = _
  //  var writeSelector: Selector = _
  var readWriteSelector: Selector = _
  var server: String = _
  var serverCounters: ServerCounters = _
  var socketName: String = _
  var connectTimeoutMs = Config.defaultConnectTimeoutMs
  var readTimeoutMs = Config.defaultReadTimeoutMs
  var writeTimeoutMs = Config.defaultWriteTimeoutMs
  var reconnectHolddownMs = Config.defaultReconnectHolddownMs

  protected def open() {
    log.finer(socketName + " open socket start")
    serverCounters.connectionOpenAttempt.incrementAndGet()
    socket = SocketChannel.open()
    socket.configureBlocking(false)
    socket.socket().setTcpNoDelay(true)
    val hostPort = server.split(":")
    socket.connect(new InetSocketAddress(hostPort(0), Integer.parseInt(hostPort(1))))
    val connectSelector = Selector.open()
    socket.register(connectSelector, SelectionKey.OP_CONNECT)

    if (connectSelector.select(connectTimeoutMs) == 0 || !socket.finishConnect()) {
      log.warning(socketName + " open socket timeout")
      connectSelector.close()
      socket.close()
      serverCounters.connectionOpenTimeout.incrementAndGet()
      throw new Exception("connect socket timeout " + server)
    }
    connectSelector.close()
    readWriteSelector = Selector.open()
    serverCounters.connectionOpenSuccess.incrementAndGet()
    opened = true
    serverCounters.connectionCurrent.incrementAndGet()
    log.finer(socketName + " open socket success")
  }

  def openBlock() {
    val dummy = new CountDownLatch(1)
    openBlock(dummy)
  }

  def openBlock(latch: CountDownLatch) {
    var connected = false
    while (latch.getCount > 0 && !connected) {
      try {
        open()
        connected = true
      } catch {
        case ex:Exception => {
          close()
          log.finer(socketName + " exception on open " + ex.toString + " holddown sleepMs " +
                    reconnectHolddownMs)
          serverCounters.connectionExceptions.incrementAndGet()
          Thread.sleep(reconnectHolddownMs)
        }
      }
    }
  }

  def selectRead(): Boolean = {
    if (log.isLoggable(Level.FINEST)) log.finest(socketName + " readselect start")
    socket.register(readWriteSelector, SelectionKey.OP_READ)
    readWriteSelector.select(readTimeoutMs)
    val keys = readWriteSelector.selectedKeys().iterator()
    var rv = false
    while (keys.hasNext) {
      rv = keys.next().isValid
      keys.remove()
    }
    if (!rv) {
      serverCounters.connectionReadTimeout.incrementAndGet()
      log.fine(socketName + " timeout reading response")
    }
    if (log.isLoggable(Level.FINEST)) log.finest(socketName + " readselect rv=" + rv)
    rv
  }

  def selectWrite(): Boolean = {
    if (log.isLoggable(Level.FINEST)) log.finest(socketName + " writeselect start")
    socket.register(readWriteSelector, SelectionKey.OP_WRITE)
    readWriteSelector.select(writeTimeoutMs)
    val keys = readWriteSelector.selectedKeys().iterator()
    var rv = false
    while (keys.hasNext) {
      rv = keys.next().isValid
      keys.remove()
    }
    if (!rv) {
      serverCounters.connectionWriteTimeout.incrementAndGet()
      log.fine(socketName + " timeout writing response")
    }
    if (log.isLoggable(Level.FINEST)) log.finest(socketName + " writeselect rv=" + rv)
    rv
  }

  def close() {
    //    if (readSelector != null) readSelector.close()
    //    if (writeSelector != null) writeSelector.close()
    if (readWriteSelector != null) {
      readWriteSelector.close()
      readWriteSelector = null
    }
    if (socket != null) {
      if (opened) {
        serverCounters.connectionCurrent.decrementAndGet()
        opened = false
      }
      socket.close()
      socket = null
    }
  }
}
