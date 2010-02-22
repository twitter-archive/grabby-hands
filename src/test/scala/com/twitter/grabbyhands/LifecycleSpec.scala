/** Copyright 2010 Twitter, Inc. */
package com.twitter.grabbyhands

object LifecycleSpec extends SpecBase(3) {

  "lifecycle" should {

    doBefore {
      val adhoc = new AdHocRequest(new ServerCounters(), hostPort)
      queues.foreach(queue => adhoc.deleteQueue(queue))

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
      val queues2 = queues.slice(0, 2).force
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
      config.addQueues(queues.slice(0, 1).force)
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
      config.addQueues(queues.slice(0, 1).force)
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
      config.addQueues(queues.slice(0, 1).force)
      config.queues.size must be_==(1)

      grab = new GrabbyHands(config)
      grab.counters.threads.get must be_==(0)
      grab.join()
      grab.counters.threads.get must be_==(0)
    }

  }
}
