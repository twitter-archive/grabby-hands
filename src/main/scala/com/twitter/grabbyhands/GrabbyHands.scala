/** Copyright 2010 Twitter, Inc. */
package com.twitter.grabbyhands

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}

class GrabbyHands(servers: Array[String],
                  queues: Array[String],
                  maxQueueDepth: Int,
                  connectionsPerQueue:Int,
                  maxMessageBytes:Int,
                  readTimeoutMs: Int,
                  reconnectTimeoutMs: Int) {
  protected[grabbyhands] val counters = new Counters()

  protected val deliveryQueues: Map[String, BlockingQueue[String]] = {
    val rv = new HashMap[String, BlockingQueue[String]]()
    for (queue <- queues) {
      rv + (queue -> new LinkedBlockingQueue(maxQueueDepth))
    }
    Map() ++ rv
  }

  protected val connections: Array[Connection] = {
    val rv = new ArrayBuffer[Connection]()
    for (server <- servers) {
      val tokens = server.split(":")
      val host = tokens(0)
      val port = Integer.parseInt(tokens(1))
      for (queue <- queues) {
        for (idx <- 1 to connectionsPerQueue) {
          println("server " + server + " queue " + queue + " idx " + idx)
          val connection = new Connection(this, host, port, queue, deliveryQueues(queue))
          rv += connection
        }
      }
    }
    rv.toArray
  }

  protected val threads: Array[Thread] = {
    val rv = new ArrayBuffer[Thread]()
    for (connection <- connections) {
      val thread = new Thread(connection)
      thread.start()
      rv += thread
    }
    rv.toArray
  }


  def getCounters(): Counters = {
    counters
  }

  def getConnectionCounters(): List[ConnectionCounters] = {
    val rv = new ListBuffer[ConnectionCounters]()
    connections.foreach(rv + _.getCounters())
    List() ++ rv
  }

  def getDeliveryQueue(queue: String): BlockingQueue[String] = {
    deliveryQueues(queue)
  }

  def halt() {
    connections.foreach(_.halt)
  }

  def join() {
    halt()
    threads.foreach(_.join)
  }
}
