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
import java.util.concurrent.TimeUnit

object PositiveSpec extends SpecBase {

  "positive" should {

    doBefore {
    }

    doAfter {
      if (grab != null) {
        grab.join()
        grab.getCounters.threads.get() must be_==(0)
        grab = null
      }
    }

    "write one, read one" in {
      AdHocRequest.deleteQueue(queue, host, port)
      ctor(1)
      grab must notBeNull
      val send = grab.getSendQueue(queue)
      val recv = grab.getRecvQueue(queue)

      val write = new Write(ByteBuffer.wrap("text".getBytes()))
      write.written.getCount() must be_==(1)
      write.cancel.getCount() must be_==(1)
      log.fine("before write")
      send.put(write)

      log.fine("before read")
      val text = recv.poll(1, TimeUnit.SECONDS)
      if (text == null) {
        //XXX
        log.fine("after read NULL")
      } else{
        //XXX
        log.fine("after read " + text)
      }
      text must notBeNull

      write.written.getCount() must be_==(0)
      write.cancel.getCount() must be_==(1)
    }

    "read empty queue" in {
      ctor(1)
      fail("see if readTimeouts increase, recv counters don't increase")
    }

    "messages up to expected limit" in {
      ctor(1)
      fail("XXX")
    }

    "huge messages" in {
      ctor(1)
      fail("XXX")
    }
  }
}
