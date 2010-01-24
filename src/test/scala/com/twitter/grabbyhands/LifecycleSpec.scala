/** Copyright 2010 Twitter, Inc. */
package com.twitter.grabbyhands

import org.specs.Specification

object LifecycleSpec extends Specification {

  var grab: GrabbyHands = _

  def ctor(connsPerQueue: Int): GrabbyHands = {
    val config = new Config(
      Array("localhost:22133", "localhost:22134"),
      Array("grabby_test"),
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

  "lifecycle" should {

    doBefore {
    }

    doAfter {
      grab.join()
      grab.getCounters.threads.get() must be_==(0)
      grab = null
    }

    "join doesn't leak threads" in {
      ctor(1)
      grab must notBeNull
      grab.getCounters.threads.get() must be_==(4)
      grab.join()
      grab.getCounters.threads.get() must be_==(0)
    }

    "create duplicate threads for duplicate connections" in {
      val hosts = 2
      val connsPerQueue = 3
      ctor(connsPerQueue)
      grab.getCounters.threads.get() must be_==(connsPerQueue * hosts * 2)
    }
  }
}
