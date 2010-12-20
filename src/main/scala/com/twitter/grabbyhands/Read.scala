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
import java.util.concurrent.atomic.AtomicBoolean

/** Wraps incoming messages. */
class Read(val message: ByteBuffer, val connection: ConnectionRecv)
{
  def this(str: String) = this(ByteBuffer.wrap(str.getBytes()), null)
  def this(bytes: Array[Byte]) = this(ByteBuffer.wrap(bytes), null)
  protected val openLatch = new CountDownLatch(1)
  protected val wasCancelled = new AtomicBoolean(false)

  def getMessage(): ByteBuffer = message
  def getConnection(): ConnectionRecv = connection


  protected[grabbyhands] def complete(closed: Boolean) {
    if (closed) {
      close()
    } else {
      cancel()
    }
  }

  /** Returns only once the transaction has been closed, aborted, or timeout occurs. */
  def awaitComplete(timeout: Int, units: TimeUnit) {
    openLatch.await(timeout, units)
  }

  /** Returns only once the transaction has been closed or aborted. */
  def awaitComplete() {
    openLatch.await(99999, TimeUnit.DAYS)
  }

  /** Returns true if transaction is still open, that is neither completed nor cancelled */
  def open(): Boolean = {
    openLatch.getCount != 0
  }

  /** Closes a transaction on the Kestrel as successful. */
  def close() {
    openLatch.countDown()
  }

  /** Cancels this read transaction on the Kestrel. */
  def cancel() {
    connection.cancelRead()
    wasCancelled.set(true)
    openLatch.countDown()
  }

  /** Returns true if write was cancelled before it could be sent to a Kestrel server. */
  def cancelled(): Boolean = {
    wasCancelled.get()
  }
}
