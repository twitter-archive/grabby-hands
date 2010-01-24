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

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}

class Queue(grabbyHands: GrabbyHands, queueName: String) {
  val log = grabbyHands.log
  protected[grabbyhands] val queueCounters = new QueueCounters()
  protected[grabbyhands] val recvQueue = new LinkedBlockingQueue[String](
    grabbyHands.config.recvQueueDepth)
  protected[grabbyhands] val sendQueue = new LinkedBlockingQueue[Write](
    grabbyHands.config.sendQueueDepth)

  protected val connections: Array[Connection] = {
    val rv = new ArrayBuffer[Connection]()
    for (server <- grabbyHands.config.servers) {
      for (idx <- 1 to grabbyHands.config.connectionsPerQueue) {
        val connectionName = server + ":" + queueName + ":" + idx
        val connection = new Connection(
          grabbyHands, connectionName, queueCounters, server, queueName, recvQueue, sendQueue)
        rv += connection
      }
    }
    rv.toArray
  }

  def getCounters(): QueueCounters = {
    queueCounters
  }

  def halt() {
    connections.foreach(_.halt)
  }

  def join() {
    halt()
    connections.foreach(_.join)
  }
}

object Queue {
  def factory(grabbyHands: GrabbyHands): Map[String, Queue] = {
    val rv = new HashMap[String, Queue]()
    grabbyHands.config.queueNames.foreach(
      queueName => rv + (queueName -> new Queue(grabbyHands, queueName)))
    Map() ++ rv
  }
}
