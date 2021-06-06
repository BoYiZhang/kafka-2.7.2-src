/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

// 保存一些字符串常量，比如线程名字。
object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

// 支持普通处理和抢占处理 Controller 事件的接口
// 目前只有 KafkaController实现了这个接口
trait ControllerEventProcessor {
  // process：接收一个 Controller 事件，并进行处理
  def process(event: ControllerEvent): Unit
  // preempt：接收一个 Controller 事件，并抢占队列之前的事件进行优先处理
  def preempt(event: ControllerEvent): Unit
}

// 事件队列上的事件对象。

// 每个QueuedEvent定义了两个字段
// event: ControllerEvent类，表示Controller事件
// enqueueTimeMs：表示Controller事件被放入到事件队列的时间戳

class QueuedEvent(val event: ControllerEvent,
                  val enqueueTimeMs: Long) {
  // 标识事件是否开始被处理
  val processingStarted = new CountDownLatch(1)
  // 标识事件是否被处理过
  val spent = new AtomicBoolean(false)

  // 处理事件
  def process(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processingStarted.countDown()
    // 调用ControllerEventProcessor的process方法处理事件
    processor.process(event)
  }

  // 抢占式处理事件
  def preempt(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processor.preempt(event)
  }

  // 阻塞等待事件被处理完成
  def awaitProcessing(): Unit = {
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

// ControllerEventManager 的伴生类，主要用于创建和管理事件处理线程和事件队列。
class ControllerEventManager(controllerId: Int,
                             processor: ControllerEventProcessor,
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventQueueTimeTimeoutMs: Long = 300000) extends KafkaMetricsGroup {
  import ControllerEventManager._

  @volatile private var _state: ControllerState = ControllerState.Idle

  private val putLock = new ReentrantLock()

  private val queue = new LinkedBlockingQueue[QueuedEvent]

  // Visible for test
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)

  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

  newGauge(EventQueueSizeMetricName, () => queue.size)

  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      clearAndPut(ShutdownEventThread)
      thread.awaitShutdown()
    } finally {
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
    }
  }

  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    // 构建QueuedEvent实例
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    // 插入到事件队列
    queue.put(queuedEvent)
    // 返回新建QueuedEvent实例
    queuedEvent
  }

  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock){

    val preemptedEvents = new ArrayList[QueuedEvent]()
    queue.drainTo(preemptedEvents)
    // 优先处理抢占式事件
    preemptedEvents.forEach(_.preempt(processor))
    // 调用上面的put方法将给定事件插入到事件队列
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  /**
   * 这个类就是一个普通的线程类，
   * 继承了 ShutdownableThread 基类，
   * 而后者是 Kafka 为很多线程类定义的公共父类。该父类是 Java Thread 类的子类
   *
   * @param name
   */
  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "


    // 首先是调用 LinkedBlockingQueue 的 take 方法，去获取待处理的 QueuedEvent 对象实例。
    //    注意，这里用的是 take 方法，这说明，如果事件队列中没有 QueuedEvent，那么，
    //    ControllerEventThread 线程将一直处于阻塞状态，直到事件队列上插入了新的待处理事件。

    // 一旦拿到 QueuedEvent 事件后，线程会判断是否是 ShutdownEventThread 事件。
    // 当 ControllerEventManager 关闭时，
    // 会显式地向事件队列中塞入ShutdownEventThread，
    // 表明要关闭 ControllerEventThread 线程。
    //
    // 如果是该事件，那么 ControllerEventThread 什么都不用做，毕竟要关闭这个线程了。
    // 相反地，如果是其他的事件，就调用 QueuedEvent 的 process 方法执行对应的处理逻辑，
    // 同时计算事件被处理的速率。

    override def doWork(): Unit = {
      // 从事件队列中获取待处理的Controller事件，否则等待
      val dequeued = pollFromEventQueue()
      dequeued.event match {
        // 如果是关闭线程事件，什么都不用做。关闭线程由外部来执行
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case controllerEvent =>
          _state = controllerEvent.state
          // 更新对应事件在队列中保存的时间
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            def process(): Unit = dequeued.process(processor)
            // 处理事件，同时计算处理速率
            rateAndTimeMetrics.get(state) match {
              case Some(timer) => timer.time { process() }
              case None => process()
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

  private def pollFromEventQueue(): QueuedEvent = {
    val count = eventQueueTimeHist.count()
    if (count != 0) {
      val event  = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
      if (event == null) {
        eventQueueTimeHist.clear()
        queue.take()
      } else {
        event
      }
    } else {
      queue.take()
    }
  }

}
