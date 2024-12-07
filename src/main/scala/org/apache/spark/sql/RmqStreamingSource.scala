


package org.apache.spark.sql

import com.customStreamingSource.rabbitMQUtils.{PathUtil, RmqEnvironmentFactory}
import com.rabbitmq.stream.MessageHandler.Context
import com.rabbitmq.stream._
import org.apache.qpid.proton.amqp.messaging.Data
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, CountDownLatch, TimeUnit}
import scala.collection.convert.ImplicitConversions.{`map AsJavaMap`, `map AsScalaConcurrentMap`, `map AsScala`}

class RmqStreamingSource(sqlContext: SQLContext, metadataPath: String, parameters: Map[String, String]) extends Source with Logging {
  private val prefetch: Long = parameters.getOrElse("rmq.fetchsize", "2001L").toLong
  private val readLimit: Long = parameters.getOrElse("rmq.maxbatchsize", "1000L").toLong
  private val queueName: String = parameters("rmq.queuename")
  private val fetchedCount: AtomicLong = new AtomicLong(0)
  private val readTimeoutSecond: Long = parameters.getOrElse("rmq.readtimeout", "300L").toLong

  private val checkpointPath: String = PathUtil.convertToSystemPath(parameters.getOrDefault("rmq.offsetcheckpointpath", new URI(metadataPath).getPath))
  private val customOffsetCheckpointPath = Paths.get(checkpointPath).resolve("customOffset")
  private val offsetManager: RmqOffsetManagerTrait = new RmqFileSystemRmqOffsetManager(customOffsetCheckpointPath)

  private val rmqEnv: Environment = RmqEnvironmentFactory.getEnvironment(parameters)
  private val rmqRetryEnv: Environment = RmqEnvironmentFactory.getEnvironment(parameters)

  private val buffer: ConcurrentNavigableMap[Long, (Message, Context)] = new ConcurrentSkipListMap[Long, (Message, Context)]()
  @volatile private var lastReadOffset: Long = offsetManager.readLongFromFile().getOrElse(-1L)
  private val consumer: Consumer = startConsume(lastReadOffset + 1)

  override def schema: StructType = RmqStreamingSchema.default

  override def getOffset: Option[Offset] = {
    val unreadMsg: ConcurrentNavigableMap[Long, (Message, Context)] = buffer.tailMap(lastReadOffset, false)
    getKeyByIndexOrLast(unreadMsg, readLimit) match {
      case None => None
      case Some(offsetValue) => Some(LongOffset(offsetValue))
    }
  }

  private def getKeyByIndexOrLast(map: ConcurrentNavigableMap[Long, (Message, Context)], index: Long): Option[Long] = {
    if (map.isEmpty) return None
    var count = 0
    var lastKey: Option[Long] = None
    val iterator = map.entrySet.iterator
    while (iterator.hasNext) {
      val entry = iterator.next
      lastKey = Some(entry.getKey) // Keep updating lastKey until the end of the loop
      count += 1
      if (count == index) return lastKey // Return the 500th entry if it exists
    }
    // If the loop completes without finding the 500th entry, return the last key found
    lastKey
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val fromOffset: Long = start match {
      case Some(offset) => offset.json().toLong
      case None => -1L
    }
    val toOffset: Long = end.json().toLong

    if (fromOffset < lastReadOffset) {
      readAgainFromSource(fromOffset, toOffset) //should be a very rare case, expensive operator is acceptable
    } else {
      readFromBuffer(fromOffset, toOffset)
    }
  }

  private def readAgainFromSource(fromOffset: Long, toOffset: Long) = {
    log.error("cache missed, read again from: " + fromOffset + " to: " + toOffset + ". Huge performance impact if this happened frequently")
    val latch: CountDownLatch = new CountDownLatch(1)
    val tempBuffer: ConcurrentNavigableMap[Long, (Message, Context)] = new ConcurrentSkipListMap[Long, (Message, Context)]()
    val tempConsumer = rmqRetryEnv.consumerBuilder()
      .offset(OffsetSpecification.offset(fromOffset + 1))
      .stream(queueName)
      .flow()
      .strategy(ConsumerFlowStrategy.creditOnProcessedMessageCount(1, 1))
      .builder()
      .messageHandler((context: Context, message: Message) => {
        if (context.offset() <= toOffset) {
          tempBuffer.put(context.offset(), (message, context))
          context.processed()
        } else {
          latch.countDown()
        }
      })
      .build()
    latch.await(readTimeoutSecond, TimeUnit.SECONDS)
    tempConsumer.close()
    toInternalDf(tempBuffer)
  }

  // (fromOffset, toOffset]
  private def readFromBuffer(fromOffset: Long, toOffset: Long) = {
    logDebug("processing from " + fromOffset + " to " + toOffset + " total " + (toOffset - fromOffset))
    val subMap: ConcurrentNavigableMap[Long, (Message, Context)] = buffer.subMap(fromOffset + 1, toOffset + 1)
    val internalDf: DataFrame = toInternalDf(subMap)
    lastReadOffset = toOffset
    //gc immediately after read to internal df
    fetchedCount.addAndGet(-1L * subMap.size().toLong)
    subMap.clear()
    internalDf
  }

  private def toInternalDf(subMap: ConcurrentNavigableMap[Long, (Message, Context)]) = {
    val internalRows = subMap.map(item => {
      val message: Message = item._2._1
      val context: Context = item._2._2
      val routingKey: UTF8String = getRoutingKey(message)
      val headers: ArrayBasedMapData = getHeaders(message).orNull
      val body: UTF8String = getBody(message)
      InternalRow(routingKey, headers, body, context.timestamp() * 1000)
    })
    val internalRdd = sqlContext.sparkContext.parallelize(internalRows.toList)
    sqlContext.internalCreateDataFrame(internalRdd, RmqStreamingSchema.default, isStreaming = true)
  }

  private def getBody(message: Message) = {
    val body: UTF8String = message.getBody match {
      case data: Data => UTF8String.fromBytes(data.getValue.getArray)
    }
    body
  }

  private def getRoutingKey(message: Message) = {
    val routingKey: UTF8String = message.getMessageAnnotations.get("x-routing-key") match {
      case null => null
      case key: String => UTF8String.fromString(key)
      case _ => null
    }
    routingKey
  }

  private def getHeaders(message: Message): Option[ArrayBasedMapData] = {
    Option(message.getApplicationProperties).map { properties =>
      val headerTuples = properties.toList.map { case (key, value) =>
        val utf8Key = UTF8String.fromString(key)
        val utf8Value = UTF8String.fromString(value.toString)
        (utf8Key, utf8Value)
      }
      val keyArray = ArrayData.toArrayData(headerTuples.map(_._1))
      val valueArray = ArrayData.toArrayData(headerTuples.map(_._2))
      new ArrayBasedMapData(keyArray, valueArray)
    }
  }

  override def commit(end: Offset): Unit = {
    val endOffset: Long = end.json().toLong
    logDebug("committing :" + endOffset)
    offsetManager.saveLongToFile(endOffset)
  }

  override def stop(): Unit = {
    consumer.close()
  }

  private def startConsume(startOffset: Long): Consumer = {
    rmqEnv.consumerBuilder()
      .offset(OffsetSpecification.offset(startOffset))
      .stream(queueName)
      .flow()
      .strategy(ConsumerFlowStrategy.creditWhenHalfMessagesProcessed(3))
      .builder()
      .messageHandler((context: Context, message: Message) => {
        if (fetchedCount.getAndAdd(1) >= prefetch) {
          Thread.sleep(100 * Math.max(0, fetchedCount.get() - prefetch))
        }
        buffer.put(context.offset(), (message, context))
        context.processed()
      })
      .build()
  }

}
