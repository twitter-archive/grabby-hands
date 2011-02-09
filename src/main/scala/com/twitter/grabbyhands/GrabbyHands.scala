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
import scala.collection.Map
import scala.collection.mutable.HashMap

/**
 * Represents a cluster of Kestrel servers that serve an identical set of queues.
 */
class GrabbyHands(val config: Config) {
  protected[grabbyhands] val log = Logger.getLogger(GrabbyHands.logname)
  config.record()

  val counters = new Counters()
  val serverCounters: Map[String, ServerCounters] = {
    val rv = new HashMap[String, ServerCounters]()
    config.servers.foreach(server => rv += (server -> new ServerCounters()))
    scala.collection.immutable.Map() ++ rv
  }

  protected[grabbyhands] val queues = Queue.factory(this)

  val queueCounters: Map[String, QueueCounters] = {
    val rv = new HashMap[String, QueueCounters]()
    queues.values.foreach(queue => rv += (queue.name -> queue.counters))
    scala.collection.immutable.Map() ++ rv
  }

  log.fine("grabbyhands started")

  /** Returns an internal queue that delivers new messages from Kestrel. */
  def getRecvQueue(queue: String): BlockingQueue[ByteBuffer] = {
    val q = queues(queue)
    if (q.config.recvTransactional) {
      throw new IllegalStateException("Transactional reads set, use getRecvTransQueue()")
    }
    q.recvQueue
  }

  /** Returns an internal queue that delivers new transactional messages from Kestrel. */
  def getRecvTransQueue(queue: String): BlockingQueue[Read] = {
    val q = queues(queue)
    if (!q.config.recvTransactional) {
      throw new IllegalStateException("Transactional reads not set, use getRecvQueue()")
    }
    q.transRecvQueue
  }

  /** Returns an internal queue that delivers messages to Kestrel. */
  def getSendQueue(queue: String): BlockingQueue[Write] = {
    queues(queue).sendQueue
  }

  /** Deletes a queue on the Kestrel cluster. */
  def deleteQueue(queue: String) {
    for (server <- config.servers) {
      val meta = new MetaRequest(server, Some(serverCounters(server)))
      meta.deleteQueue(queue)
    }
  }

  /** Halts the client, but does not wait for the client threads to join. */
  def halt() {
    log.fine("grabbyhands halt start")
    queues.values.foreach(_.halt)
    log.fine("grabbyhands halt end")
  }

  /** Halts the client and waits for the client threads to join. */
  def join() {
    log.fine("grabbyhands join start")
    halt()
    queues.values.foreach(_.join)
    log.fine("grabbyhands join end")
  }

  /** Pauses client threads. */
  def pause() {
    log.fine("grabbyhands pause start")
    queues.values.foreach(_.pause)
    log.fine("grabbyhands pause end")
  }

  /** Resumes client threads. */
  def resume() {
    log.fine("grabbyhands resume start")
    queues.values.foreach(_.resume)
    log.fine("grabbyhands resume end")
  }


  /** Returns counters in "name: value\n" format */
  def countersToString(): String = {
    countersToString(None)
  }

  /** Returns counters in "prefix.name: value\n" format */
  def countersToString(prefix: String): String = {
    countersToString(Some(prefix))
  }

  /** Returns counters in "prefix.name: value\n" format */
  def countersToString(prefix: Option[String]): String = {
    val sb = new StringBuffer()
    for ((name, value) <- countersToMap()) {
      if (prefix.isDefined) {
        sb.append(prefix.get)
        sb.append(".")
      }
      sb.append(name)
      sb.append(": ")
      sb.append(value)
      sb.append("\n")
    }
    sb.toString
  }

  /** Returns counters as a map */
  def countersToMap(): Map[String, Long] = {
    val rv = new HashMap[String, Long]()
    rv ++= counters.toMap()
    for ((server, serverCounters) <- serverCounters) {
      for ((name, value) <- serverCounters.toMap) {
        rv += "server." + server + "." + name -> value
      }
    }
    for ((queue, queueCounters) <- queueCounters) {
      for ((name, value) <- queueCounters.toMap) {
        rv += "queue." + queue + "." + name -> value
      }
    }
    scala.collection.immutable.Map() ++ rv
  }
}

protected [grabbyhands] object GrabbyHands {
  val logname = "grabbyhands"
}
