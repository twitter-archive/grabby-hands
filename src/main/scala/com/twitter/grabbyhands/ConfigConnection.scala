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

/**
 * Configures connection parameters, one ConfigDirection object per "direction", send
 * or receive.
 */
class ConfigConnection(sendConfig: ConfigDirection, recvConfig: ConfigDirection) {

  def this() = this(new ConfigDirection("send"), new ConfigDirection("recv"))
  def this(config: ConfigConnection) = this(config.send.clone(), config.recv.clone())

  protected[grabbyhands] val send = sendConfig
  protected[grabbyhands] val recv = recvConfig

  def sendNumConnections: Int = send.numConnections
  def sendNumConnections_=(value: Int) = send.numConnections = value
  def getSendNumConnections(): Int = sendNumConnections
  def setSendNumConnections(value: Int) = sendNumConnections = value

  def recvNumConnections: Int = recv.numConnections
  def recvNumConnections_=(value: Int) = recv.numConnections = value
  def getRecvNumConnections(): Int = recvNumConnections
  def setRecvNumConnections(value: Int) = recvNumConnections = value

  def sendQueueDepth: Int = send.queueDepth
  def sendQueueDepth_=(value: Int) = send.queueDepth = value
  def getSendQueueDepth(): Int = sendQueueDepth
  def setSendQueueDepth(value: Int) = sendQueueDepth = value

  def recvQueueDepth: Int = recv.queueDepth
  def recvQueueDepth_=(value: Int) = recv.queueDepth = value
  def getRecvQueueDepth(): Int = recvQueueDepth
  def setRecvQueueDepth(value: Int) = recvQueueDepth = value

  def sendTransactional: Boolean = send.transactional
  def sendTransactional_=(value: Boolean) = send.transactional = value
  def getSendTransactional(): Boolean = sendTransactional
  def setSendTransactional(value: Boolean) = sendTransactional = value

  def recvTransactional: Boolean = recv.transactional
  def recvTransactional_=(value: Boolean) = recv.transactional = value
  def getRecvTransactional(): Boolean = recvTransactional
  def setRecvTransactional(value: Boolean) = recvTransactional = value

  override def toString(): String = send.toString() + " " + recv.toString()

  override def clone(): ConfigConnection = new ConfigConnection(send.clone(), recv.clone())

}
