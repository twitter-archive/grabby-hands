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
class Write(val message: ByteBuffer, val watcher: Boolean => Unit) {
  def this(read:Read) = this(read.message, read.close(_:Boolean))
  def this(message:ByteBuffer) = this(message, (Boolean) => false)
  def this(str: String) = this(ByteBuffer.wrap(str.getBytes()))
  def this(bytes: Array[Byte]) = this(ByteBuffer.wrap(bytes))
  protected val writtenLatch = new CountDownLatch(1)
  protected val cancelLatch = new CountDownLatch(1)

  /** Returns true iff the message has been sent to a Kestrel server. */
  def written(): Boolean = {
    writtenLatch.getCount == 0
  }

  def getMessage(): ByteBuffer = message
  def getWatcher(): Function[Boolean, Unit] = watcher

  /** Returns only once the message has been sent to a Kestrel server or timeout occurs. */
  def awaitWrite(timeout: Int, units: TimeUnit) {
    writtenLatch.await(timeout, units)
  }

  /** Returns only once the message has been sent to a Kestrel. */
  def awaitWrite() {
    writtenLatch.await(99999, TimeUnit.DAYS)
  }

  protected[grabbyhands] def write() {
    watcher(true)
    writtenLatch.countDown()
  }

  /** Cancels a write waiting in the local queue. */
  def cancel() {
    cancelLatch.countDown()
    watcher(false)
  }

  /** Returns true if write was cancelled before it could be sent to a Kestrel server. */
  def cancelled(): Boolean = {
    cancelLatch.getCount == 0
  }

  def close() {
    write()
  }

}
