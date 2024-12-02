package org.apache.spark.sql

trait RmqOffsetManagerTrait {
  def saveLongToFile(value: Long): Unit

  def readLongFromFile(): Option[Long]
}
