package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class RmqStreamingSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    if (schema.nonEmpty) logError("Input schema will be override") else logInfo("Using default schema")
    (shortName(), RmqStreamingSchema.default)
  }

  override def shortName(): String = "RmqStreamQueueSource"

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType],
                            providerName: String, parameters: Map[String, String]): Source = {
    new RmqStreamingSource(sqlContext, metadataPath, parameters)
  }
}
