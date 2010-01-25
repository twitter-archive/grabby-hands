/** Copyright 2010 Twitter, Inc. */
package com.twitter.grabbyhands

object LifecycleSpec extends SpecBase {

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
