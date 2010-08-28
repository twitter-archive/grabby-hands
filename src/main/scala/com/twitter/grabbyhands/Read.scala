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
import java.util.concurrent.{CountDownLatch, TimeUnit}

/** Wraps outgoing messages. */
class Read(val message: ByteBuffer, val connection:ConnectionRecv )
{
  def this(str: String) = this(ByteBuffer.wrap(str.getBytes()), null)
  def this(bytes: Array[Byte]) = this(ByteBuffer.wrap(bytes), null)
  protected val completedLatch = new CountDownLatch(1)
  protected val abortLatch = new CountDownLatch(1)

  def getMessage(): ByteBuffer = message
  def getConnection(): ConnectionRecv = connection


  def awaitComplete(timeout: Int, units: TimeUnit) {
    completedLatch.await(timeout, units)
  }

  /** Returns only once the message has been sent to a Kestrel. */
  def awaitComplete() {
    completedLatch.await(99999, TimeUnit.DAYS)
  }

  /** Returns true if transaction is completed or aborted */
  def completed(): Boolean = {
    completedLatch.getCount == 0
  }

  protected[grabbyhands] def close(success:Boolean) {
    if(success) completedLatch.countDown()
    else abort()
  }

  /** Cancels a write waiting in the local queue. */
  def abort() {
    abortLatch.countDown()
    connection.abortRead()
  }

  /** Returns true if write was cancelled before it could be sent to a Kestrel server. */
  def cancelled(): Boolean = {
    abortLatch.getCount == 0
  }

}
