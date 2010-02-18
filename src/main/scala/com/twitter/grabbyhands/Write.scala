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
import java.util.concurrent.{CountDownLatch}

class Write(val message: ByteBuffer) {
  def this(str: String) = this(ByteBuffer.wrap(str.getBytes()))
  def this(bytes: Array[Byte]) = this(ByteBuffer.wrap(bytes))
  protected val writtenLatch = new CountDownLatch(1)
  protected val cancelLatch = new CountDownLatch(1)

  def written(): Boolean = {
    writtenLatch.getCount == 0
  }

  protected[grabbyhands] def write() {
    cancelLatch.countDown()
  }

  def cancel() {
    cancelLatch.countDown()
  }

  def cancelled(): Boolean = {
    cancelLatch.getCount == 0
  }

}
