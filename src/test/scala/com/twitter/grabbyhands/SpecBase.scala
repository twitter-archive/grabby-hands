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

import java.util.logging.{FileHandler,Level,Logger,SimpleFormatter}
import org.specs.Specification

class SpecBase(val numQueues: Int) extends Specification {
  val log = Logger.getLogger(GrabbyHands.logname)
  var grab: GrabbyHands = _
  var config: Config = _
  val queues = new Array[String](numQueues)
  for (idx <- 0 to numQueues - 1) {
    queues(idx) = "grabby_test" + idx
  }
  val queue = queues(0)
  val host = "localhost"
  val port = 22133
  val hostPort = host + ":" + port
  val shortMessageMax = 256

  def defaults(): Config = {
    config = new Config()
    config.addServer(host + ":" + port)
    config.maxMessageBytes = shortMessageMax
    config.addQueue(queue)
    config
  }

  def ctor(): GrabbyHands = {
    grab = new GrabbyHands(config)
    grab
  }

  def genAsciiString(length: Int): String = {
    val sb = new StringBuffer()
    for (idx <- 0 to length - 1) {
      sb.append(('a' + (idx % ('z' - 'a'))).asInstanceOf[Char])
    }
    sb.toString
  }

  def genBinaryArray(length: Int): Array[Byte] = {
    val rv = new Array[Byte](length)
    for (idx <- 0 to length - 1) {
      rv(idx) = idx.asInstanceOf[Byte]
    }
    rv
  }
}
