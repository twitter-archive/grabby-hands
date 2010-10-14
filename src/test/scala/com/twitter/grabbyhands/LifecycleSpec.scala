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

object LifecycleSpec extends SpecBase(3) {

  "lifecycle" should {

    doBefore {
      val meta = new MetaRequest(hostPort, None)
      queues.foreach(queue => meta.deleteQueue(queue))

      defaults()
      grab = null
    }

    doAfter {
      grab.join()
      grab.counters.threads.get must be_==(0)
    }

    "join doesn't leak threads" in {
      ctor()
      grab must notBeNull
      grab.counters.threads.get must be_==(2)
      grab.join()
      grab.counters.threads.get must be_==(0)
    }

    "correct number of threads created" in {
      val hosts = Array(host + ":" + port, host + ":" + port)
      val config = new Config()
      config.addServers(hosts)
      config.sendNumConnections = 3
      config.recvNumConnections = 4
      val queues2 = queues.slice(0, 2).toArray
      queues2.size  must be_==(2)
      config.addQueues(queues2)
      val threads =
        hosts.size * (config.sendNumConnections + config.recvNumConnections) * queues2.size

      grab = new GrabbyHands(config)
      grab.counters.threads.get must be_==(threads)
      grab.join()
      grab.counters.threads.get must be_==(0)
    }

    "test zero recv connections" in {
      val config = new Config()
      config.addServer(host + ":" + port)
      config.recvNumConnections = 0
      config.addQueues(queues.slice(0, 1).toArray)
      config.queues.size must be_==(1)

      grab = new GrabbyHands(config)
      grab.counters.threads.get must be_==(1)
      grab.join()
      grab.counters.threads.get must be_==(0)
    }

    "test zero send connections" in {
      val config = new Config()
      config.addServer(host + ":" + port)
      config.sendNumConnections = 0
      config.addQueues(queues.slice(0, 1).toArray)
      config.queues.size must be_==(1)

      grab = new GrabbyHands(config)
      grab.counters.threads.get must be_==(1)

      grab.join()
      grab.counters.threads.get must be_==(0)
    }

    "test zero connection nonsense" in {
      val config = new Config()
      config.addServer(host + ":" + port)
      config.recvNumConnections = 0
      config.sendNumConnections = 0
      config.addQueues(queues.slice(0, 1).toArray)
      config.queues.size must be_==(1)

      grab = new GrabbyHands(config)
      grab.counters.threads.get must be_==(0)
      grab.join()
      grab.counters.threads.get must be_==(0)
    }

  }
}
