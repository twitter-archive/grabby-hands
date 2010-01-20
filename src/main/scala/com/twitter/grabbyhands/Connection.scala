/** Copyright 2010 Twitter, Inc. */
package com.twitter.grabbyhands

import java.util.concurrent.{BlockingQueue, CountDownLatch}

protected class Connection(grabby: GrabbyHands,
                           host: String,
                           port: Int,
                           queue: String,
                           deliveryQueue: BlockingQueue[String]
                         ) extends Runnable {
  val name = host + ":" + port + ":" + queue
  protected val counters = new ConnectionCounters()
  protected val haltLatch = new CountDownLatch(1)

  def run() {
    println("connection " + name + " start")
    grabby.counters.threads.incrementAndGet()
    while (haltLatch.getCount() > 0) {
      Thread.sleep(100)
    }
    grabby.counters.threads.decrementAndGet()
    println("connection " + name + " end")
  }

  def halt() {
    haltLatch.countDown()
  }

  def getCounters(): ConnectionCounters = {
    counters
  }
}
