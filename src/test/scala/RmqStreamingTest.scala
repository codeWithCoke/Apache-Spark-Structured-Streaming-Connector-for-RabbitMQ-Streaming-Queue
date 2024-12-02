import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object RmqStreamingTest {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("customRmqStreamingQueueSrc")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputDF = spark.readStream
      .format("org.apache.spark.sql.RmqStreamingSourceProvider")
      .option("rmq.vhost", "/")
      .option("rmq.username", "guest")
      .option("rmq.password", "guest")
      .option("rmq.host", "localhost")
      .option("rmq.port", "5552")
      .option("rmq.queuename", "stream")
      .option("rmq.fetchsize", "20001")
      .option("rmq.maxbatchsize", "10000")
      .option("rmq.offsetcheckpointpath", "checkpoint")
      .load()

    // Define the streaming query to write data to the console
    val query: StreamingQuery = inputDF.writeStream
      .option("checkpointLocation", "checkpoint")
      .foreachBatch(printConsole _)
      .outputMode("append")
      .start()

    // Process the data (trigger the execution)
    spark.streams.awaitAnyTermination()

    // Stop the query and SparkSession
    query.stop()
    spark.stop()
  }

  private def printConsole(dataFrame: DataFrame, batchId: Long): Unit = {
    println("batch: " + batchId + " count: " + dataFrame.count())
    dataFrame.show(1, truncate = false)
    println("----------------------------------")
  }
}