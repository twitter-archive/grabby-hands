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

class ServerCounters() {
  val bytesRecv = new AtomicLong()
  val bytesSend = new AtomicLong()
  val messagesRecv = new AtomicLong()
  val messagseSend = new AtomicLong()

  val connectionSuccess = new AtomicLong()
  val connectionFail = new AtomicLong()
  val connectionCurrent = new AtomicLong()
  val connectionExceptions = new AtomicLong()
  val protocolError = new AtomicLong()
}
