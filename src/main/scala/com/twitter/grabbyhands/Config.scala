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

import java.util.logging.Logger
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.reflect.BeanProperty

class Config() extends ConfigConnection {
  protected val log = Logger.getLogger(GrabbyHands.logname)
  protected[grabbyhands] val queues = new HashMap[String, ConfigQueue]()
  protected[grabbyhands] val servers = new ListBuffer[String]()
  val configConnection = new ConfigConnection()

  @BeanProperty var maxMessageBytes = Config.defaultMaxMessageBytes
  @BeanProperty var kestrelReadTimeoutMs = Config.defaultKestrelReadTimeoutMs
  @BeanProperty var connectTimeoutMs = Config.defaultConnectTimeoutMs
  @BeanProperty var readTimeoutMs = Config.defaultReadTimeoutMs
  @BeanProperty var writeTimeoutMs = Config.defaultWriteTimeoutMs
  @BeanProperty var reconnectHolddownMs = Config.defaultReconnectHolddownMs

  def addQueue(name: String): ConfigQueue = {
    val queue = new ConfigQueue(name, this)
    queues + (name -> queue)
    queue
  }

  // TODO: Rationalize java and scala addQueues()
  def addQueues(names: Seq[String]): Map[String, ConfigQueue] = {
    val rv = new HashMap[String, ConfigQueue]
    for (name <- names) {
      val queue = addQueue(name)
      rv + (name -> addQueue(name))
    }
    Map() ++ rv
  }

  // TODO: Rationalize java and scala addQueues()
  def addQueues(names: java.lang.Iterable[String]) : java.util.HashMap[String, ConfigQueue] = {
    val rv = new java.util.HashMap[String, ConfigQueue]()
    val iterator = names.iterator()
    while (iterator.hasNext) {
      val name = iterator.next()
      rv.put(name, addQueue(name))
    }
    rv
  }

  def addServer(server: String) {
    servers += server
  }

  def addServers(servers: java.lang.Iterable[java.lang.String]) {
    val iterator = servers.iterator()
    while (iterator.hasNext()) {
      addServer(iterator.next())
    }
  }

  def addServers(servers: Seq[String]) {
    servers.foreach(addServer)
  }


  def record() {
    log.config("servers=" + servers.mkString("[", ",", "]"))
    log.config(this.toString)
    log.config("maxMessageBytes=" + maxMessageBytes)
    log.config("kestrelReadTimeoutMs=" + kestrelReadTimeoutMs)
    log.config("connectTimeoutMs=" + connectTimeoutMs)
    log.config("readTimeoutMs=" + readTimeoutMs)
    log.config("writeTimeoutMs=" + writeTimeoutMs)
    log.config("reconnectHolddownMs=" + reconnectHolddownMs)
    queues.values.foreach(_.record())
  }
}

object Config {
  val defaultMaxMessageBytes = 65536
  val defaultKestrelReadTimeoutMs = 2000
  val defaultConnectTimeoutMs = 1000
  val defaultReadTimeoutMs = 1000
  val defaultWriteTimeoutMs = 1000
  val defaultReconnectHolddownMs = 1000
}
