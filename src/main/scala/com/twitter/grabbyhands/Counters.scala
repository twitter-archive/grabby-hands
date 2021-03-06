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

import java.util.concurrent.atomic.AtomicLong
import scala.collection.Map
import scala.collection.mutable.HashMap

class Counters() {
  val threads = new AtomicLong(0)
  val pausedThreads = new AtomicLong(0)

  def toMap(): Map[String, Long] = {
    val rv = new HashMap[String, Long]()
    rv += ("threads" -> threads.get)
    rv += ("pausedThreads" -> pausedThreads.get)
    scala.collection.immutable.Map() ++ rv
  }
}
