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

import java.util.concurrent.BlockingQueue

protected class Connection(grabbyHands: GrabbyHands,
                           connectionName: String,
                           queueCounters: QueueCounters,
                           server: String,
                           queueName: String,
                           recvQueue: BlockingQueue[String],
                           sendQueue: BlockingQueue[Write]
                         ) {
  val log = grabbyHands.log
  protected val send = new ConnectionSend(
    grabbyHands, connectionName, queueCounters, server, queueName, sendQueue)
  protected val recv = new ConnectionRecv(
    grabbyHands, connectionName, queueCounters, server, queueName, recvQueue, send)
  recv.start()
  send.start()

  def newThread(runnable: Runnable): Thread = {
    val thread = new Thread(runnable)
    thread.setDaemon(true)
    thread.start()
    thread
  }

  def join() {
    log.fine("Connection " + connectionName + " join start")
    halt()
    recv.join()
    send.join()
    log.fine("Connection " + connectionName + " join end")
  }

  def halt() {
    log.fine("Connection " + connectionName + " halt")
    recv.halt()
    send.halt()
  }
}
