package com.twitter.grabbyhands

import org.specs.Specification

object BasicSpec extends Specification {
  "Basic" should {
    "have a java compatable factory" in {
      val g = new GrabbyHands(Array("localhost:22133", "localhost:22134"))
      g must notBeNull
    }
  }
}
