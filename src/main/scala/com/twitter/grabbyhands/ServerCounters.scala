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

import java.util.concurrent.atomic.AtomicLong
import scala.collection.Map
import scala.collection.mutable.HashMap

class ServerCounters() {
  val bytesRecv = new AtomicLong()
  val bytesSent = new AtomicLong()
  val messagesRecv = new AtomicLong()
  val messagesSent = new AtomicLong()

  val connectionOpenAttempt = new AtomicLong()
  val connectionOpenSuccess = new AtomicLong()
  val connectionOpenTimeout = new AtomicLong()

  val connectionCurrent = new AtomicLong()
  val connectionExceptions = new AtomicLong()
  val connectionReadTimeout = new AtomicLong()
  val connectionWriteTimeout = new AtomicLong()
  val protocolError = new AtomicLong()

  def toMap(): Map[String, Long] = {
    val rv = new HashMap[String, Long]()
    rv += ("bytesRecv" -> bytesRecv.get)
    rv += ("bytesSent" -> bytesSent.get)
    rv += ("messagesRecv" -> messagesRecv.get)
    rv += ("messagesSent" -> messagesSent.get)
    rv += ("connectionOpenAttempt" -> connectionOpenAttempt.get)
    rv += ("connectionOpenSuccess" -> connectionOpenSuccess.get)
    rv += ("connectionOpenTimeout" -> connectionOpenTimeout.get)
    rv += ("connectionCurrent" -> connectionCurrent.get)
    rv += ("connectionExceptions" -> connectionExceptions.get)
    rv += ("connectionReadTimeout" -> connectionReadTimeout.get)
    rv += ("connectionWriteTimeout" -> connectionWriteTimeout.get)
    rv += ("protocolError" -> protocolError.get)
    scala.collection.immutable.Map() ++ rv
  }
}
