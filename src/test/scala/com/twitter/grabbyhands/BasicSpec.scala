package com.twitter.grabbyhands

import org.specs.Specification

object BasicSpec extends Specification {

  var grab: GrabbyHands = _

  def factory(): GrabbyHands = {
    grab = new GrabbyHands(
      Array("localhost:22133", "localhost:22134"),
      Array("grabby_test"),
      1,
      1,
      16384,
      1000,
      1000)
    grab
  }


  "Basic" should {

    doAfter {
      grab.join()
      grab.getCounters.threads.get() must be_==(0)
      grab = null
    }

    "have a java compatable factory" in {
      factory()
      grab must notBeNull
    }

    "join" in {
      factory()
      grab.getCounters.threads.get() must be_==(2)
      grab.join()
      grab.getCounters.threads.get() must be_==(0)
    }

  }
}
