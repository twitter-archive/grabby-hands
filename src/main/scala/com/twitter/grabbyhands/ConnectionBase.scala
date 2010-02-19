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
import java.util.concurrent.{CountDownLatch, Semaphore}
import java.util.concurrent.atomic.AtomicBoolean

protected abstract class ConnectionBase(queue: Queue,
                                        connectionName: String,
                                        serverArg: String) extends Thread with Socket {
  val grabbyHands = queue.grabbyHands
  val config = queue.config
  val queueCounters = queue.counters
  val queueName = config.name

  server = serverArg
  socketName = connectionName

  serverCounters = grabbyHands.serverCounters(server)
  connectTimeoutMs = grabbyHands.config.connectTimeoutMs
  readTimeoutMs = grabbyHands.config.readTimeoutMs
  writeTimeoutMs = grabbyHands.config.writeTimeoutMs
  reconnectHolddownMs = grabbyHands.config.reconnectHolddownMs

  protected val haltLatch = new CountDownLatch(1)
  protected val startedLatch = new CountDownLatch(1)
  protected val paused = new AtomicBoolean(false)
  protected val pauseBarrier = new Semaphore(1)
  protected val resumeBarrier = new Semaphore(1)

  this.setDaemon(true)

  override def run() {
    log.fine(connectionName + " thread start")
    grabbyHands.counters.threads.incrementAndGet()
    Thread.currentThread().setName(connectionName)
    startedLatch.countDown()
    pauseBarrier.acquire()

    while (haltLatch.getCount > 0) {
      try {
        log.finer(connectionName + " open start")
        openBlock(haltLatch)
        log.finer(connectionName + " open end")
        var connected = true

        while (connected && haltLatch.getCount > 0) {
          if (paused.get()) {
            doPause()
          }
          connected = run2()
        }

        if (!connected && haltLatch.getCount > 0) {
          log.finer(connectionName + " connection failed, holddown sleep")
          Thread.sleep(grabbyHands.config.reconnectHolddownMs)
        }
      } catch {
        case ex: Exception => {
          log.fine(connectionName + " exception " + ex.toString)
          serverCounters.connectionExceptions.incrementAndGet()
        }
      }
      close()
    }
    grabbyHands.counters.threads.decrementAndGet()
    log.fine(connectionName + " thread end")
  }

  def run2(): Boolean

  protected def readBuffer(atleastUntilPosition: Int, buffer: ByteBuffer): Boolean = {
    while (buffer.position < atleastUntilPosition) {
      if (haltLatch.getCount == 0) {
        return false
      }
      if (!selectRead()) {
        return false
      }
      socket.read(buffer)
    }
    true
  }

  protected def writeBuffer(buffer: ByteBuffer): Boolean = {
    while (buffer.hasRemaining) {
      if (haltLatch.getCount == 0) {
        return false
      }
      if (!selectWrite()) {
        return false
      }
      socket.write(buffer)
    }
    true
  }

  protected def writeBufferVector(bytes: Int, buffers: Array[ByteBuffer]): Boolean = {
    var left = bytes.asInstanceOf[Long]
    while (left > 0) {
      if (haltLatch.getCount == 0) {
        return false
      }
      if (!selectWrite()) {
        return false
      }
      left -= socket.write(buffers.toArray)
    }
    true
  }

  def halt() {
    log.fine(connectionName + " halt")
    haltLatch.countDown()
    this.interrupt()
  }

  def started() {
    startedLatch.await()
  }

  protected def doPause() {
    pauseBarrier.release()
    var count = grabbyHands.counters.pausedThreads.incrementAndGet()
    log.fine(connectionName + " paused count=" + count)
    resumeBarrier.acquire()
    resumeBarrier.release()
    count = grabbyHands.counters.pausedThreads.decrementAndGet()
    paused.set(false)
    pauseBarrier.acquire()
    log.fine(connectionName + " resumed count=" + count)
  }

  def pause() {
    log.fine(connectionName + " pause")
    resumeBarrier.acquire()
    paused.set(true)
    pauseBarrier.acquire()
    pauseBarrier.release()
  }

  // Cannot be resume(), as Thread snagged the method name.
  def unPause() {
    log.fine(connectionName + " resume")
    resumeBarrier.release()
  }
}
