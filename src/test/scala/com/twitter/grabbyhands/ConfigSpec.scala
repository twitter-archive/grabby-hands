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

object ConfigSpec extends SpecBase(2) {

  "config" should {

    "set servers" in {
      val config = new Config()
      config.addServers(Array("host1:1", "host2:2"))
      config.servers.size must be_==(2)
      config.servers(0).name must be_==("host1:1")
      config.servers(1).name must be_==("host2:2")
    }

    "set other values" in {
      val config = new Config()
      config.addServer("host1:1")
      config.maxMessageBytes = 100
      config.maxMessageBytes must be_==(100)
    }

    "overrides work" in {
      val config = new Config()
      config.addServer("host:1")
      config.sendNumConnections must be_==(1)
      config.recvNumConnections must be_==(1)
      config.sendQueueDepth must be_==(1)
      config.recvQueueDepth must be_==(1)

      config.sendNumConnections = 2
      config.recvNumConnections = 3
      config.sendQueueDepth = 4
      config.recvQueueDepth = 5

      val cq1 = config.addQueue("q")
      cq1.name must be_==("q")
      cq1.sendNumConnections must be_==(2)
      cq1.recvNumConnections must be_==(3)
      cq1.sendQueueDepth must be_==(4)
      cq1.recvQueueDepth must be_==(5)

      cq1.sendNumConnections = 6
      cq1.recvNumConnections = 7
      cq1.sendQueueDepth = 8
      cq1.recvQueueDepth = 9

      config.queues.size must be_==(1)
      config.queues.get("q") must beSome[ConfigQueue]
      val cq2 = config.queues("q")
      cq2.name must be_==("q")
      cq2.sendNumConnections must be_==(6)
      cq2.recvNumConnections must be_==(7)
      cq2.sendQueueDepth must be_==(8)
      cq2.recvQueueDepth must be_==(9)
    }

    "automatically increase queue depth" in {
      val config = new Config()
      config.addServer("host:1")
      config.sendNumConnections must be_==(1)
      config.recvNumConnections must be_==(1)
      config.sendQueueDepth must be_==(1)
      config.recvQueueDepth must be_==(1)

      config.sendNumConnections = 2
      config.sendQueueDepth must be_==(2)
      config.sendQueueDepth = 5
      config.sendQueueDepth must be_==(5)
      config.sendNumConnections = 2
      config.sendQueueDepth must be_==(10)

      config.recvNumConnections = 3
      config.recvQueueDepth must be_==(3)
      config.recvQueueDepth = 7
      config.recvQueueDepth must be_==(7)
      config.recvNumConnections = 4
      config.recvQueueDepth must be_==(28)
    }

    "support multiple queues" in {
      val config = new Config()
      config.addServer("host:1")

      val queues = config.addQueues(List("q1", "q2"))
      queues.size must be_==(2)

      config.queues.size must be_==(2)
      queues.get("q1") must beSome[ConfigQueue]
      queues.get("q2") must beSome[ConfigQueue]
      queues.get("q3") must beNone

      config.addQueue("q3")
      config.queues.size must be_==(3)
      config.queues.get("q1") must beSome[ConfigQueue]
      config.queues.get("q2") must beSome[ConfigQueue]
      config.queues.get("q3") must beSome[ConfigQueue]
      config.queues.get("q4") must beNone
    }

    "silently handle queue collisions" in {
      val config = new Config()
      config.addServer("host:1")

      val queues = config.addQueues(List("q1", "q1"))
      queues.size must be_==(1)
      config.queues.get("q1") must beSome[ConfigQueue]

      config.addQueue("q1")
      queues.size must be_==(1)
      config.queues.get("q1") must beSome[ConfigQueue]

      config.addQueues(List("q1"))
      queues.size must be_==(1)
      config.queues.get("q1") must beSome[ConfigQueue]
    }

    "set transactional" in {
      val config = new Config()
      config.addServer("host:1")
      val queues = config.addQueues(List("q1"))
      config.recvTransactional = true

      queues.size must be_==(1)
      config.recvTransactional must be_==(true)
    }

  }
}
