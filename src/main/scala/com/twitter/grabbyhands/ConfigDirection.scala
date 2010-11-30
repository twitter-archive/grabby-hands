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

import java.lang.Cloneable
import scala.reflect.BeanProperty

/** Configures a send or receive direction. */
protected[grabbyhands] class ConfigDirection(name: String) extends Cloneable {
  private var _numConnections = 1
  private var _transactional = false

  /**
   * Configures the depth of the internal queue and thus the number of outstanding messages.
   * Excessive values increase data loss risk or decrease liveness.
   */
  @BeanProperty var queueDepth = 1

  def numConnections: Int = _numConnections
  def transactional: Boolean = _transactional

  def numConnections_=(value: Int) {
    queueDepth *= value
    if (queueDepth == 0) queueDepth = 1 // Cannot create a queue with 0 depth
    _numConnections = value
 }

  def transactional_=(value: Boolean) = _transactional = value

  /** Configures the number of connections, and thus parallel transactions. */
  def setNumConnections(value: Int) = numConnections = value
  def getNumConnections(): Int = numConnections

  /** Configures if this queue is transactional. */
  def setTransactional(value: Boolean) = transactional = value
  def getTransactional(): Boolean = transactional

  override def toString(): String = {
    name + "NumConnections=" + numConnections + " " + name + "QueueDepth=" + queueDepth
  }

  override def clone(): ConfigDirection = {
    super.clone().asInstanceOf[ConfigDirection]
  }
}
