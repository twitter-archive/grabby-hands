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

object CountersSpec extends SpecBase(1) {

  "counters" should {

    doAfter {
      if (grab != null) {
        grab.join()
        grab.counters.threads.get must be_==(0)
      }
    }

    "full results" in {
      val config = new Config()
      config.addServer("host1:1")
      config.addQueue("q")
      val grab = new GrabbyHands(config)
      Thread.sleep(50) // Let counters settle a bit
      grab.counters.threads.set(100)
      grab.counters.pausedThreads.set(200)

      val hc = grab.serverCounters("host1:1")
      hc.bytesRecv.set(10)
      hc.bytesSent.set(11)
      hc.messagesRecv.set(12)
      hc.messagesSent.set(13)
      hc.connectionOpenAttempt.set(14)
      hc.connectionOpenSuccess.set(15)
      hc.connectionOpenTimeout.set(16)
      hc.connectionCurrent.set(17)
      hc.connectionExceptions.set(18)
      hc.connectionReadTimeout.set(19)
      hc.connectionWriteTimeout.set(20)
      hc.protocolError.set(21)

      val qc = grab.queueCounters("q")
      qc.bytesRecv.set(30)
      qc.bytesSent.set(31)
      qc.messagesRecv.set(32)
      qc.messagesSent.set(33)
      qc.kestrelGetTimeouts.set(34)
      qc.protocolError.set(35)
      qc.sendCancelled.set(36)

      val cs = grab.countersToString()
      cs must include("threads: 100")
      cs must include("pausedThreads: 200")

      cs must include("server.host1:1.bytesRecv: 10")
      cs must include("server.host1:1.bytesSent: 11")
      cs must include("server.host1:1.messagesRecv: 12")
      cs must include("server.host1:1.messagesSent: 13")
      cs must include("server.host1:1.connectionOpenAttempt: 14")
      cs must include("server.host1:1.connectionOpenSuccess: 15")
      cs must include("server.host1:1.connectionOpenTimeout: 16")
      cs must include("server.host1:1.connectionCurrent: 17")
      cs must include("server.host1:1.connectionExceptions: 18")
      cs must include("server.host1:1.connectionReadTimeout: 19")
      cs must include("server.host1:1.connectionWriteTimeout: 20")
      cs must include("server.host1:1.protocolError: 21")

      cs must include("queue.q.bytesRecv: 30")
      cs must include("queue.q.bytesSent: 31")
      cs must include("queue.q.messagesRecv: 32")
      cs must include("queue.q.messagesSent: 33")
      cs must include("queue.q.kestrelGetTimeouts: 34")
      cs must include("queue.q.protocolError: 35")
      cs must include("queue.q.sendCancelled: 36")
    }

    "prefix" in {
      val config = new Config()
      config.addServer("host1:1")
      config.addQueue("q")
      val grab = new GrabbyHands(config)
      val cs = grab.countersToString("prefix")

      cs must include("prefix.threads:")
      cs must include("prefix.server.host1:1.bytesRecv: ")
      cs must include("prefix.queue.q.bytesRecv: ")
    }

    "two queues two hosts" in {
      val config = new Config()
      config.addServer("host1:1")
      config.addServer("host2:2")
      config.addQueue("q1")
      config.addQueue("q2")
      val grab = new GrabbyHands(config)
      val cs = grab.countersToString()

      cs must include("threads:")

      cs must include("server.host1:1.bytesRecv: ")
      cs must include("queue.q1.bytesRecv: ")

      cs must include("server.host2:2.bytesRecv: ")
      cs must include("queue.q2.bytesRecv: ")
    }
  }
}
