package org.apache.spark.sql

import org.apache.spark.sql.types.{MapType, StringType, StructType, TimestampType}

object RmqStreamingSchema {
  val default: StructType = new StructType()
    .add("routingKey", StringType)
    .add("headers", MapType(StringType, StringType))
    .add("body", StringType)
    .add("receiveTimestamp", TimestampType)
}
