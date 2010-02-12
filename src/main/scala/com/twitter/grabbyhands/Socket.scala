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
import java.util.logging.Logger

trait Socket {
  val log = Logger.getLogger("grabbyhands")
  var socket: SocketChannel = _
  var readSelector: Selector = _
  var writeSelector: Selector = _
  var host: String = _
  var port: Int = _
  var connectTimeoutMs = 1000
  var readWriteTimeoutMs = 1000

  def open() {
    socket = SocketChannel.open()
    socket.configureBlocking(false)
    socket.connect(new InetSocketAddress(host, port))
    val connectSelector = Selector.open()
    socket.register(connectSelector, SelectionKey.OP_CONNECT)
    if (connectSelector.select(connectTimeoutMs) == 0 || !socket.finishConnect()) {
      connectSelector.close()
      socket.close()
      throw new Exception("connect timeout " + host + ":" + port)
    }
    connectSelector.close()
    readSelector = Selector.open()
    writeSelector = Selector.open()
    socket.register(readSelector, SelectionKey.OP_READ)
    socket.register(writeSelector, SelectionKey.OP_WRITE)
  }

  def openBlock() {
    val dummy = new CountDownLatch(1)
    openBlock(dummy)
  }

  def openBlock(latch: CountDownLatch) {
    var connected = false
    while (latch.getCount() > 0 && !connected) {
      try {
        open()
        connected = true
      } catch {
        case ex:Exception => null
      }
    }
  }

  def selectRead(): Int = {
    readSelector.select(readWriteTimeoutMs)
  }

  def selectWrite(): Int = {
    writeSelector.select(readWriteTimeoutMs)
  }

  def close() {
    if (readSelector != null) readSelector.close()
    if (writeSelector != null) writeSelector.close()
    if (socket != null) socket.close()
  }
}
