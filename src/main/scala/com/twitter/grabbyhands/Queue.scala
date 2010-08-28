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

import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}

protected case class Queue(grabbyHands: GrabbyHands, config: ConfigQueue) {
  protected val log = grabbyHands.log
  protected[grabbyhands] val counters = new QueueCounters()
  protected[grabbyhands] val recvQueue = new LinkedBlockingQueue[ByteBuffer](
    config.recvQueueDepth)
  protected[grabbyhands] val transRecvQueue = new LinkedBlockingQueue[Read](
    config.recvQueueDepth)
  protected[grabbyhands] val sendQueue = new LinkedBlockingQueue[Write](
    config.sendQueueDepth)
  val transactional = config.getRecvTransactional()
  val name = config.name

  log.fine("Connection threads starting")
  protected val connections: Array[ConnectionBase] = {
    val rv = new ArrayBuffer[ConnectionBase]()
    for (server <- grabbyHands.config.servers) {
      for (idx <- 1 to config.recvNumConnections) {
        val connectionName = server + ":recv:" + config.name + ":" + idx
        val connection = new ConnectionRecv(this, connectionName, server)
        rv += connection
        connection.start()
      }

      for (idx <- 1 to config.sendNumConnections) {
        val connectionName = server + ":send:" + config.name + ":" + idx
        val connection = new ConnectionSend(this, connectionName, server)
        rv += connection
        connection.start()
      }

    }
    rv.toArray
  }

  connections.foreach(connection => connection.started())
  log.fine("All connection threads running")

  def getCounters(): QueueCounters = {
    counters
  }

  def halt() {
    connections.foreach(_.halt)
  }

  def join() {
    halt()
    connections.foreach(_.join)
  }

  def pause() {
    connections.foreach(_.pause)
  }

  def resume() {
    connections.foreach(_.unPause)
  }
}

protected [grabbyhands] object Queue {
  def factory(grabbyHands: GrabbyHands): Map[String, Queue] = {
    val rv = new HashMap[String, Queue]()
    grabbyHands.config.queues.values.foreach(
      queue => rv + (queue.name -> new Queue(grabbyHands, queue)))
    rv.readOnly
  }
}
