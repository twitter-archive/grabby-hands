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
import java.util.concurrent.BlockingQueue
import java.util.logging.Logger
import scala.collection.mutable.HashMap

class GrabbyHands(val config: Config) {
  protected[grabbyhands] val log = Logger.getLogger("grabbyhands")

  val counters = new Counters()
  val serverCounters: Map[String, ServerCounters] = {
    val rv = new HashMap[String, ServerCounters]()
    config.servers.foreach(server => rv + (server -> new ServerCounters()))
    Map() ++ rv
  }

  protected[grabbyhands] val queues = Queue.factory(this)

  val queueCounters: Map[String, QueueCounters] = {
    val rv = new HashMap[String, QueueCounters]()
    queues.values.foreach(queue => rv + (queue.name -> queue.counters))
    Map() ++ rv
  }

  def getRecvQueue(queue: String): BlockingQueue[ByteBuffer] = {
    queues(queue).recvQueue
  }

  def getSendQueue(queue: String): BlockingQueue[Write] = {
    queues(queue).sendQueue
  }

  def deleteQueue(queue: String) {
    for (server <- config.servers) {
      val adhoc = new AdHocRequest(serverCounters(server), server)
      adhoc.deleteQueue(queue)
    }
  }

  def halt() {
    log.fine("grabbyhands halt")
    queues.values.foreach(_.halt)
  }

  def join() {
    log.fine("grabbyhands join start")
    halt()
    queues.values.foreach(_.join)
    log.fine("grabbyhands join end")
  }
}
