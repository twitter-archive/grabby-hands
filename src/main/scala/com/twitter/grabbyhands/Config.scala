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
import scala.collection.Map
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.reflect.BeanProperty

/**
 * Configures GrabbyHands.
 * <p>
 * Parameters are inhereted by each queue's configuration at the point when a queue is added.
 * Queue parameters can be overridden after queue creation on a queue by queue basis by
 * setting the returned ConfigQueue object.
 *
 * @see com.twitter.grabbhands.ConfigQueue
 */
class Config() extends ConfigConnection {
  protected val log = Logger.getLogger(GrabbyHands.logname)
  protected[grabbyhands] val queues = new HashMap[String, ConfigQueue]()
  protected[grabbyhands] val servers = new ListBuffer[String]()
  val configConnection = new ConfigConnection()

  /** Configures the maximum number of bytes in a received message. */
  @BeanProperty var maxMessageBytes = Config.defaultMaxMessageBytes

  /** Configures the time, in milliseconds, that Kestrel should block waiting
   * for a message on this queue. Configures the /t= parameter to GET.
   */
  @BeanProperty var kestrelReadTimeoutMs = Config.defaultKestrelReadTimeoutMs

  /** Configures the time, in milliseconds, to wait for a TCP connection. */
  @BeanProperty var connectTimeoutMs = Config.defaultConnectTimeoutMs

  /** Configures the time, in milliseconds, to wait for data from Kestrel. */
  @BeanProperty var readTimeoutMs = Config.defaultReadTimeoutMs

  /** Configures the time, in milliseconds, to wait for buffer space to write to Kestrel. */
  @BeanProperty var writeTimeoutMs = Config.defaultWriteTimeoutMs

  /** Configures the time, in milliseconds, to wait between connection attempts. */
  @BeanProperty var reconnectHolddownMs = Config.defaultReconnectHolddownMs

  /** Adds a new queue, inheriting the current parameters. */
  def addQueue(name: String): ConfigQueue = {
    val queue = new ConfigQueue(name, this)
    queues + (name -> queue)
    queue
  }

  /** Adds several queues, inheriting the current parameters. */
  def addQueues(names: Seq[String]): Map[String, ConfigQueue] = {
    val rv = new HashMap[String, ConfigQueue]
    for (name <- names) {
      val queue = addQueue(name)
      rv + (name -> addQueue(name))
    }
    rv.readOnly
  }

  /** Adds several queues, inheriting the current parameters. */
  def addQueues(names: java.lang.Iterable[String]) : java.util.HashMap[String, ConfigQueue] = {
    val rv = new java.util.HashMap[String, ConfigQueue]()
    val iterator = names.iterator()
    while (iterator.hasNext) {
      val name = iterator.next()
      rv.put(name, addQueue(name))
    }
    rv
  }

  /** Adds a server in host:port format. */
  def addServer(server: String) {
    servers += server
  }

  /** Adds several hosts, each in host:port format. */
  def addServers(servers: java.lang.Iterable[java.lang.String]) {
    val iterator = servers.iterator()
    while (iterator.hasNext()) {
      addServer(iterator.next())
    }
  }

  /** Adds several hosts, each in host:port format. */
  def addServers(servers: Seq[String]) {
    servers.foreach(addServer)
  }

  /** Logs current configuration. */
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
  val defaultMaxMessageBytes = 16384
  val defaultKestrelReadTimeoutMs = 2000
  val defaultConnectTimeoutMs = 1000
  val defaultReadTimeoutMs = 1000
  val defaultWriteTimeoutMs = 1000
  val defaultReconnectHolddownMs = 1000
}
