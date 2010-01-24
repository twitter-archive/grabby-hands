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

import java.util.concurrent.CountDownLatch

protected abstract class ConnectionBase(grabbyHands: GrabbyHands,
                                        connectionName: String,
                                        connectionType: String,
                                        server: String) extends Runnable {
  val log = grabbyHands.log
  protected val haltLatch = new CountDownLatch(1)
  val serverCounters = grabbyHands.serverCounters(server)

  def run() {
    log.fine(connectionType + " " + connectionName + " thread start")
    grabbyHands.counters.threads.getAndIncrement()
    Thread.currentThread().setName(connectionName)
    run2()
    grabbyHands.counters.threads.getAndDecrement()
    log.fine(connectionType + " " + connectionName + " thread end")
  }

  def run2()

  def halt() {
    log.fine(connectionType + " " + connectionName + " halt")
    haltLatch.countDown()
  }
}
