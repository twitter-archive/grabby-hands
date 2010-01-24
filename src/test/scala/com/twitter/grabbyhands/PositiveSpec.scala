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

import org.specs.Specification
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

object PositiveSpec extends Specification {

  var grab: GrabbyHands = _
  val queue = "grabby_test"

  def ctor(connsPerQueue: Int): GrabbyHands = {
    val config = new Config(
      Array("localhost:22133"),
      Array(queue),
      connsPerQueue,
      1,
      1,
      16384,
      1000,
      1000,
      50)
    grab = new GrabbyHands(config)
    grab
  }

  "positive" should {

    doBefore {
    }

    doAfter {
      grab.join()
      grab.getCounters.threads.get() must be_==(0)
      grab = null
    }

    "write one, read one" in {
      ctor(1)
      grab must notBeNull
      val send = grab.getSendQueue(queue)
      val recv = grab.getRecvQueue(queue)

      val write = new Write(ByteBuffer.wrap("text".getBytes()))
      write.written.getCount() must be_==(1)
      write.cancel.getCount() must be_==(1)
      send.put(write)

      val text = recv.poll(1, TimeUnit.SECONDS)
      text must notBeNull

      write.written.getCount() must be_==(0)
      write.cancel.getCount() must be_==(1)
    }

    "read empty queue" in {
      fail("see if readTimeouts increase, recv counters don't increase")
    }

    "messages up to expected limit" in {
      fail("XXX")
    }

    "huge messages" in {
      fail("XXX")
    }
  }
}
