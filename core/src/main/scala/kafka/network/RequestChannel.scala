/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent._

import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.Meter
import kafka.log.LogConfig
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils.{Logging, NotNothing, Pool}
import kafka.utils.Implicits._
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData._
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}

import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

// kafka本质: 一个不断接收外部请求、处理请求，然后发送处理结果的 Java 进程。

/**
 * 当前 Kafka Broker 端所有网络线程都是在RequestChannel 中维护的。
 */

object RequestChannel extends Logging {
  private val requestLogger = Logger("kafka.request.logger")

  val RequestQueueSizeMetric = "RequestQueueSize"
  val ResponseQueueSizeMetric = "ResponseQueueSize"
  val ProcessorMetricTag = "processor"

  def isRequestLoggingEnabled: Boolean = requestLogger.underlying.isDebugEnabled

  // Scala 语言中的“trait”关键字，大致类似于 Java 中的 interface（接口）
  sealed trait BaseRequest

  case object ShutdownRequest extends BaseRequest

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
    val sanitizedUser: String = Sanitizer.sanitize(principal.getName)
  }

  class Metrics {

    private val metricsMap = mutable.Map[String, RequestMetrics]()

    (ApiKeys.enabledApis.asScala.toSeq.map(_.name) ++
        Seq(RequestMetrics.consumerFetchMetricName, RequestMetrics.followFetchMetricName)).foreach { name =>
      metricsMap.put(name, new RequestMetrics(name))
    }

    def apply(metricName: String) = metricsMap(metricName)

    def close(): Unit = {
       metricsMap.values.foreach(_.removeMetrics())
    }
  }


  /**
   *
   *
   * @param processor
   *    processor 是 Processor 线程的序号，即这个请求是由哪个 Processor 线程接收处理的。
   *    Broker 端参数 num.network.threads 控制了 Broker 每个监听器上创建的 Processor 线程数。
   *
   *    为什么要保存 Processor 线程序号呢？
   *        这是因为，当 Request 被后面的 I/O线程处理完成后，
   *        还要依靠 Processor 线程发送 Response 给请求发送方，
   *        因此，Request 中必须记录它之前是被哪个 Processor 线程接收的。
   *
   *        另外，这里我们要先明确一点：Processor 线程仅仅是网络接收线程，
   *        不会执行真正的 Request 请求处理逻辑，那是I/O 线程负责的事情
   *
   *
   *        在默认情况下，Broker 启动时会创建 6 个 Processor 线程，每 3 个为一组，
   *        分别给listeners 参数中设置的两个监听器使用，每组的序号分别是 0、1、2。
   *
   *
   * @param context
   *        context 是用来标识请求上下文信息的。
   *
   * @param startTimeNanos
   *        记录了 Request 对象被创建的时间，主要用于各种时间统计指标的计算
   *        它是以纳秒为单位的时间戳信息，可以实现非常细粒度的时间统计精度
   *
   *
   * @param memoryPool
   *        memoryPool 表示源码定义的一个非阻塞式的内存缓冲区，主要作用是避免 Request 对象无限使用内存

   *        实现类 : SimpleMemoryPool  使用堆内内存  ByteBuffer
   *
   *
   * @param buffer
   *        buffer 是真正保存 Request 对象内容的字节缓冲区。
   *        Request 发送方必须按照 Kafka RPC协议规定的格式向该缓冲区写入字节，
   *        否则将抛出 InvalidRequestException 异常。
   *        这个逻辑主要是由 RequestContext 的 parseRequest 方法实现的
   *
   * @param metrics
   *        metrics 是 Request 相关的各种监控指标的一个管理类。
   *        它里面构建了一个 Map，封装了所有的请求 JMX 指标。
   *
   */
  class Request(val processor: Int,
                val context: RequestContext,
                val startTimeNanos: Long,
                memoryPool: MemoryPool,
                @volatile private var buffer: ByteBuffer,
                metrics: RequestChannel.Metrics) extends BaseRequest {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeNanos = -1L
    @volatile var apiLocalCompleteTimeNanos = -1L
    @volatile var responseCompleteTimeNanos = -1L
    @volatile var responseDequeueTimeNanos = -1L
    @volatile var messageConversionsTimeNanos = 0L
    @volatile var apiThrottleTimeMs = 0L
    @volatile var temporaryMemoryBytes = 0L
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None

    val session = Session(context.principal, context.clientAddress)

    // 解析后的请求实体 [ 信息和长度 ]
    private val bodyAndSize: RequestAndSize = context.parseRequest(buffer)

    def header: RequestHeader = context.header
    def sizeOfBodyInBytes: Int = bodyAndSize.size

    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def requestDesc(details: Boolean): String = s"$header -- ${loggableRequest.toString(details)}"

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], @nowarn("cat=unused") nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    def loggableRequest: AbstractRequest = {

      def loggableValue(resourceType: ConfigResource.Type, name: String, value: String): String = {
        val maybeSensitive = resourceType match {
          case ConfigResource.Type.BROKER => KafkaConfig.maybeSensitive(KafkaConfig.configType(name))
          case ConfigResource.Type.TOPIC => KafkaConfig.maybeSensitive(LogConfig.configType(name))
          case ConfigResource.Type.BROKER_LOGGER => false
          case _ => true
        }
        if (maybeSensitive) Password.HIDDEN else value
      }

      bodyAndSize.request match {
        case alterConfigs: AlterConfigsRequest =>
          val loggableConfigs = alterConfigs.configs().asScala.map { case (resource, config) =>
            val loggableEntries = new AlterConfigsRequest.Config(config.entries.asScala.map { entry =>
                new AlterConfigsRequest.ConfigEntry(entry.name, loggableValue(resource.`type`, entry.name, entry.value))
            }.asJavaCollection)
            (resource, loggableEntries)
          }.asJava
          new AlterConfigsRequest.Builder(loggableConfigs, alterConfigs.validateOnly).build(alterConfigs.version())

        case alterConfigs: IncrementalAlterConfigsRequest =>
          val resources = new AlterConfigsResourceCollection(alterConfigs.data.resources.size)
          alterConfigs.data.resources.forEach { resource =>
            val newResource = new AlterConfigsResource()
              .setResourceName(resource.resourceName)
              .setResourceType(resource.resourceType)
            resource.configs.forEach { config =>
              newResource.configs.add(new AlterableConfig()
                .setName(config.name)
                .setValue(loggableValue(ConfigResource.Type.forId(resource.resourceType), config.name, config.value))
                .setConfigOperation(config.configOperation))
            }
            resources.add(newResource)
          }
          val data = new IncrementalAlterConfigsRequestData()
            .setValidateOnly(alterConfigs.data().validateOnly())
            .setResources(resources)
          new IncrementalAlterConfigsRequest.Builder(data).build(alterConfigs.version)

        case _ =>
          bodyAndSize.request
      }
    }

    trace(s"Processor $processor received request: ${requestDesc(true)}")

    def requestThreadTimeNanos: Long = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }

    def updateRequestMetrics(networkThreadTimeNanos: Long, response: Response): Unit = {
      val endTimeNanos = Time.SYSTEM.nanoseconds

      /**
       * Converts nanos to millis with micros precision as additional decimal places in the request log have low
       * signal to noise ratio. When it comes to metrics, there is little difference either way as we round the value
       * to the nearest long.
       */
      def nanosToMs(nanos: Long): Double = {
        val positiveNanos = math.max(nanos, 0)
        TimeUnit.NANOSECONDS.toMicros(positiveNanos).toDouble / TimeUnit.MILLISECONDS.toMicros(1)
      }

      val requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
      val apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos)
      val apiRemoteTimeMs = nanosToMs(responseCompleteTimeNanos - apiLocalCompleteTimeNanos)
      val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      val messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos)
      val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)
      val fetchMetricNames =
        if (header.apiKey == ApiKeys.FETCH) {
          val isFromFollower = body[FetchRequest].isFromFollower
          Seq(
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty
      val metricNames = fetchMetricNames :+ header.apiKey.name
      metricNames.foreach { metricName =>
        val m = metrics(metricName)
        m.requestRate(header.apiVersion).mark()
        m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs))
        m.localTimeHist.update(Math.round(apiLocalTimeMs))
        m.remoteTimeHist.update(Math.round(apiRemoteTimeMs))
        m.throttleTimeHist.update(apiThrottleTimeMs)
        m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs))
        m.responseSendTimeHist.update(Math.round(responseSendTimeMs))
        m.totalTimeHist.update(Math.round(totalTimeMs))
        m.requestBytesHist.update(sizeOfBodyInBytes)
        m.messageConversionsTimeHist.foreach(_.update(Math.round(messageConversionsTimeMs)))
        m.tempMemoryBytesHist.foreach(_.update(temporaryMemoryBytes))
      }

      // Records network handler thread usage. This is included towards the request quota for the
      // user/client. Throttling is only performed when request handler thread usage
      // is recorded, just before responses are queued for delivery.
      // The time recorded here is the time spent on the network thread for receiving this request
      // and sending the response. Note that for the first request on a connection, the time includes
      // the total time spent on authentication, which may be significant for SASL/SSL.
      recordNetworkThreadTimeCallback.foreach(record => record(networkThreadTimeNanos))

      if (isRequestLoggingEnabled) {
        val detailsEnabled = requestLogger.underlying.isTraceEnabled
        val responseString = response.responseString.getOrElse(
          throw new IllegalStateException("responseAsString should always be defined if request logging is enabled"))
        val builder = new StringBuilder(256)
        builder.append("Completed request:").append(requestDesc(detailsEnabled))
          .append(",response:").append(responseString)
          .append(" from connection ").append(context.connectionId)
          .append(";totalTime:").append(totalTimeMs)
          .append(",requestQueueTime:").append(requestQueueTimeMs)
          .append(",localTime:").append(apiLocalTimeMs)
          .append(",remoteTime:").append(apiRemoteTimeMs)
          .append(",throttleTime:").append(apiThrottleTimeMs)
          .append(",responseQueueTime:").append(responseQueueTimeMs)
          .append(",sendTime:").append(responseSendTimeMs)
          .append(",securityProtocol:").append(context.securityProtocol)
          .append(",principal:").append(session.principal)
          .append(",listener:").append(context.listenerName.value)
          .append(",clientInformation:").append(context.clientInformation)
        if (temporaryMemoryBytes > 0)
          builder.append(",temporaryMemoryBytes:").append(temporaryMemoryBytes)
        if (messageConversionsTimeMs > 0)
          builder.append(",messageConversionsTime:").append(messageConversionsTimeMs)
        requestLogger.debug(builder.toString)
      }
    }

    def releaseBuffer(): Unit = {
      if (buffer != null) {
        memoryPool.release(buffer)
        buffer = null
      }
    }

    override def toString = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer)"

  }


  /**
   *
   * 定义 Response 的抽象基类。每个 Response 对象都包含了对应的Request 对象。
   * 这个类里最重要的方法是 onComplete 方法，用来实现每类 Response被处理后需要执行的回调逻辑。
   *
   * 这个抽象基类只有一个属性字段：request。
   * 这就是说，每个 Response 对象都要保存它对应的 Request 对象。
   *
   *
   *
   * @param request
   */
  abstract class Response(val request: Request) {

    def processor: Int = request.processor

    def responseString: Option[String] = Some("")

    // onComplete 方法是调用指定回调逻辑的地方。
    def onComplete: Option[Send => Unit] = None

    override def toString: String
  }

  /**
   * Kafka 大多数 Request 处理完成后都需要执行一段回调逻辑，
   * SendResponse 就是保存返回结果的 Response 子类。
   *
   * SendResponse 类继承了 Response 父类，并重新定义了 onComplete 方法。
   *
   * 里面最重要的字段是onCompletionCallback，即指定处理完成之后的回调逻辑。
   *
   *
   *
   * responseAsString should only be defined if request logging is enabled
   * */
  class SendResponse(request: Request,
                     val responseSend: Send,
                     val responseAsString: Option[String],
                     val onCompleteCallback: Option[Send => Unit]) extends Response(request) {
    override def responseString: Option[String] = responseAsString


    /**
     * Scala 中的 Unit 类似于 Java 中的 void，
     * 而“Send => Unit”表示一个方法。
     *
     * 这个方法接收一个 Send 类实例，
     * 然后执行一段代码逻辑。Scala 是函数式编程语言，
     * 函数在 Scala 中是“一等公民”，
     * 因此，你可以把一个函数作为一个参数传给另一个函数，
     * 也可以把函数作为结果返回。
     *
     * 这里的 onComplete 方法就应用了第二种用法，也就是把函数赋值给另一个函数，并作为结果返回。
     * 这样做的好处在于，你可以灵活地变更 onCompleteCallback 来实现不同的回调逻辑。
     * @return
     */
    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String =
      s"Response(type=Send, request=$request, send=$responseSend, asString=$responseAsString)"
  }

  /**
   * 有些 Request 处理完成后无需单独执行额外的回调逻辑。
   * NoResponse就是为这类 Response 准备的。
   *
   * @param request
   */
  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=NoOp, request=$request)"
  }

  /**
   * 用于出错后需要关闭 TCP 连接的场景，
   * 此时返回CloseConnectionResponse 给 Request 发送方，
   * 显式地通知它关闭连接。
   *
   * @param request
   */
  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=CloseConnection, request=$request)"
  }

  /**
   * 用于通知 Broker 的 Socket Server 组件某个 TCP 连接通信通道开始被限流（throttling）。
   * @param request
   */
  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=StartThrottling, request=$request)"
  }

  /**
   * 与 StartThrottlingResponse 对应，通知 Broker 的SocketServer 组件某个 TCP 连接通信通道的限流已结束
   * @param request
   */
  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=EndThrottling, request=$request)"
  }
}

/**
 * RequestChannel 类实现了 KafkaMetricsGroup trait，
 * 后者封装了许多实用的指标监控方法，
 * 比如，newGauge 方法用于创建数值型的监控指标，
 * newHistogram 方法用于创建直方图型的监控指标。
 *
 * 就 RequestChannel 类本身的主体功能而言，它定义了最核心的 3 个属性：
 *  1. requestQueue
 *  2. queueSize
 *  3. processors
 *
 *
 * @param queueSize
 * @param metricNamePrefix
 * @param time
 */
class RequestChannel(val queueSize: Int,
                     val metricNamePrefix : String,
                     time: Time) extends KafkaMetricsGroup {
  import RequestChannel._
  val metrics = new RequestChannel.Metrics

  /**
   * 每个 RequestChannel 对象实例创建时，会定义一个队列来保存 Broker 接收到的各类请求，
   * 这个队列被称为请求队列或 Request 队列 [requestQueue]。
   *
   * Kafka 使用 Java 提供的阻塞队列ArrayBlockingQueue 实现这个请求队列，
   * 并利用它天然提供的线程安全性来保证多个线程能够并发安全高效地访问请求队列。
   *
   *
   *
   * 而字段 queueSize 就是 Request 队列的最大长度。
   *
   * 当 Broker 启动时，
   * SocketServer 组件会创建 RequestChannel 对象，
   * 并把 Broker 端参数 queued.max.requests 赋值给queueSize。
   * 因此，在默认情况下，每个 RequestChannel 上的队列长度是 500。
   *
   *  queueSize :
   *
   *
   */
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)


  /**
   * 字段 processors 封装的是 RequestChannel 下辖的 Processor 线程池。
   * 每个 Processor线程负责具体的请求处理逻辑。
   *
   * Processor 线程池 用 Java 的ConcurrentHashMap 数据结构去保存的。
   * Map 中的 Key 就是前面我们说的 processor序号，
   * 而 Value 则对应具体的 Processor 线程对象
   *
   */
  private val processors = new ConcurrentHashMap[Int, Processor]()


  /**
   * 度量相关
   */
  val requestQueueSizeMetricName = metricNamePrefix.concat(RequestQueueSizeMetric)
  val responseQueueSizeMetricName = metricNamePrefix.concat(ResponseQueueSizeMetric)

  newGauge(requestQueueSizeMetricName, () => requestQueue.size)

  newGauge(responseQueueSizeMetricName, () => {
    processors.values.asScala.foldLeft(0) {(total, processor) =>
      total + processor.responseQueueSize
    }
  })


  /**
   * 每当 Broker 启动时，它都会调用 addProcessor 方法，
   * 向RequestChannel 对象添加 num.network.threads 个 Processor 线程
   * @param processor
   */
  def addProcessor(processor: Processor): Unit = {

    // 添加Processor到Processor线程池
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId ${processor.id}")

    newGauge(responseQueueSizeMetricName, () => processor.responseQueueSize,
      // 为给定Processor对象创建对应的监控指标
      Map(ProcessorMetricTag -> processor.id.toString))
  }

  /**
   * Kafka 允许你动态地修改此参数值。比如，Broker 启动时指定 num.network.threads 为 8，
   * 之后你通过 kafka-configs 命令将其修改为 3。
   * 显然，这个操作会减少 Processor 线程池中的线程数量。
   * 在这个场景下，removeProcessor 方法会被调用.
   *
   * @param processorId
   */
  def removeProcessor(processorId: Int): Unit = {
    // 从Processor线程池中移除给定Processor线程
    processors.remove(processorId)
    removeMetric(responseQueueSizeMetricName, Map(ProcessorMetricTag -> processorId.toString))
  }

  /**
   * 发送request
   *
   * Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request): Unit = {
    // 所谓的发送 Request，仅仅是将 Request 对象放置在 Request 队列中而已
    requestQueue.put(request)
  }

  /**
   *
   * 发送 response
   * sendResponse 方法的逻辑其实非常简单。
   * 前面的一大段 if 代码块仅仅是构造 Trace 日志要输出的内容。
   * 根据不同类型的Response，代码需要确定要输出的 Trace 日志内容。
   * 接着，代码会找出 Response 对象对应的 Processor 线程。
   * 当 Processor 处理完某个Request 后，会把自己的序号封装进对应的 Response 对象。
   * 一旦找出了之前是由哪个Processor 线程处理的，代码直接调用该 Processor 的 enqueueResponse 方法，
   * 将Response 放入 Response 队列中，等待后续发送。
   *
   *
   * Send a response back to the socket server to be sent over the network */
  def sendResponse(response: RequestChannel.Response): Unit = {

    // 构造Trace日志输出字符串
    if (isTraceEnabled) {
      val requestHeader = response.request.header
      val message = response match {
        case sendResponse: SendResponse =>
          s"Sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} of ${sendResponse.responseSend.size} bytes."
        case _: NoOpResponse =>
          s"Not sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} as it's not required."
        case _: CloseConnectionResponse =>
          s"Closing connection for client ${requestHeader.clientId} due to error during ${requestHeader.apiKey}."
        case _: StartThrottlingResponse =>
          s"Notifying channel throttling has started for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
        case _: EndThrottlingResponse =>
          s"Notifying channel throttling has ended for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
      }
      trace(message)
    }

    response match {
      // We should only send one of the following per request
      case _: SendResponse | _: NoOpResponse | _: CloseConnectionResponse =>
        val request = response.request
        val timeNanos = time.nanoseconds()
        request.responseCompleteTimeNanos = timeNanos
        if (request.apiLocalCompleteTimeNanos == -1L)
          request.apiLocalCompleteTimeNanos = timeNanos
      // For a given request, these may happen in addition to one in the previous section, skip updating the metrics
      case _: StartThrottlingResponse | _: EndThrottlingResponse => ()
    }

    // 找出response对应的Processor线程，即request当初是由哪个Processor线程处理的
    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.

    // 将response对象放置到对应Processor线程的Response队列中
    if (processor != null) {

      // 将 Response 添加到 Response 队列的过程
      processor.enqueueResponse(response)
    }
  }

  /**
   *
   * 接收 Request , 有 timeout 控制
   *
   * 接收Request 则是从队列中取出 Request
   *
   * Get the next request or block until specified time has elapsed */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest = requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /**
   * 接收 Request  , take 阻塞
   *
   * 接收Request 则是从队列中取出 Request
   *
   * Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.BaseRequest = requestQueue.take()

  /**
   * 用于实现监控项的更新
   * @param apiKey
   * @param errors
   */
  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]): Unit = {
    errors.forKeyValue { (error, count) =>
      metrics(apiKey.name).markErrorMeter(error, count)
    }
  }

  def clear(): Unit = {
    requestQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
    metrics.close()
  }

  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

}

object RequestMetrics {
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"

  // 每秒处理的 Request 数，用来评估 Broker 的繁忙状态
  val RequestsPerSec = "RequestsPerSec"

  // 计算 Request 在 Request 队列中的平均等候时间，单位是毫秒。
  // 倘若 Request 在队列的等待时间过长，
  // 你通常需要增加后端 I/O 线程的数量，来加快队列中 Request 的拿取速度
  val RequestQueueTimeMs = "RequestQueueTimeMs"

  // 计算 Request 实际被处理的时间，单位是毫秒。
  // 一旦定位到这个监控项的值很大，
  // 你就需要进一步研究 Request 被处理的逻辑了，
  // 具体分析到底是哪一步消耗了过多的时间。
  val LocalTimeMs = "LocalTimeMs"

  /**
   * Kafka 的读写请求（PRODUCE 请求和 FETCH 请求）逻辑涉及等待其他 Broker 操作的步骤。
   * RemoteTimeMs 计算的，就是等待其他 Broker 完成指定逻辑的时间。
   * 因为等待的是其他 Broker，因此被称为 Remote Time。
   *
   * 这个监控项非常重要！
   * Kafka 生产环境中设置 acks=all 的 Producer 程序发送消息延时高的主要原因，
   * 往往就是 Remote Time 高。
   * 因此，如果你也碰到了这样的问题，不妨先定位一下Remote Time 是不是瓶颈
   *
   */
  val RemoteTimeMs = "RemoteTimeMs"
  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  /**
   * 计算 Request 被处理的完整流程时间。这是最实用的监控指标，没有之一！
   * 毕竟，我们通常都是根据 TotalTimeMs 来判断系统是否出现问题的。
   * 一旦发现了问题，我们才会利用前面的几个监控项进一步定位问题的原因
   */
  val TotalTimeMs = "TotalTimeMs"
  val RequestBytes = "RequestBytes"
  val MessageConversionsTimeMs = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes = "TemporaryMemoryBytes"
  val ErrorsPerSec = "ErrorsPerSec"
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {

  import RequestMetrics._

  val tags = Map("request" -> name)
  val requestRateInternal = new Pool[Short, Meter]()
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram(RequestQueueTimeMs, biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram(LocalTimeMs, biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram(RemoteTimeMs, biased = true, tags)
  // time a request is throttled, not part of the request processing time (throttling is done at the client level
  // for clients that support KIP-219 and by muting the channel for the rest)
  val throttleTimeHist = newHistogram(ThrottleTimeMs, biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = newHistogram(ResponseQueueTimeMs, biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram(ResponseSendTimeMs, biased = true, tags)
  val totalTimeHist = newHistogram(TotalTimeMs, biased = true, tags)
  // request size in bytes
  val requestBytesHist = newHistogram(RequestBytes, biased = true, tags)
  // time for message conversions (only relevant to fetch and produce requests)
  val messageConversionsTimeHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(MessageConversionsTimeMs, biased = true, tags))
    else
      None
  // Temporary memory allocated for processing request (only populated for fetch and produce requests)
  // This shows the memory allocated for compression/conversions excluding the actual request size
  val tempMemoryBytesHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(TemporaryMemoryBytes, biased = true, tags))
    else
      None

  private val errorMeters = mutable.Map[Errors, ErrorMeter]()
  Errors.values.foreach(error => errorMeters.put(error, new ErrorMeter(name, error)))

  def requestRate(version: Short): Meter = {
    requestRateInternal.getAndMaybePut(version, newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags + ("version" -> version.toString)))
  }

  class ErrorMeter(name: String, error: Errors) {
    private val tags = Map("request" -> name, "error" -> error.name)

    @volatile private var meter: Meter = null

    def getOrCreateMeter(): Meter = {
      if (meter != null)
        meter
      else {
        synchronized {
          if (meter == null)
             meter = newMeter(ErrorsPerSec, "requests", TimeUnit.SECONDS, tags)
          meter
        }
      }
    }

    def removeMeter(): Unit = {
      synchronized {
        if (meter != null) {
          removeMetric(ErrorsPerSec, tags)
          meter = null
        }
      }
    }
  }

  def markErrorMeter(error: Errors, count: Int): Unit = {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    for (version <- requestRateInternal.keys) removeMetric(RequestsPerSec, tags + ("version" -> version.toString))
    removeMetric(RequestQueueTimeMs, tags)
    removeMetric(LocalTimeMs, tags)
    removeMetric(RemoteTimeMs, tags)
    removeMetric(RequestsPerSec, tags)
    removeMetric(ThrottleTimeMs, tags)
    removeMetric(ResponseQueueTimeMs, tags)
    removeMetric(TotalTimeMs, tags)
    removeMetric(ResponseSendTimeMs, tags)
    removeMetric(RequestBytes, tags)
    removeMetric(ResponseSendTimeMs, tags)
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name) {
      removeMetric(MessageConversionsTimeMs, tags)
      removeMetric(TemporaryMemoryBytes, tags)
    }
    errorMeters.values.foreach(_.removeMeter())
    errorMeters.clear()
  }
}
