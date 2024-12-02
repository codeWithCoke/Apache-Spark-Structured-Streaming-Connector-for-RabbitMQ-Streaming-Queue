package org.apache.spark.sql

import org.apache.spark.internal.Logging

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, NoSuchFileException, Path}
import scala.util.Try

class RmqFileSystemRmqOffsetManager(path: Path) extends RmqOffsetManagerTrait with Logging {
  private val checkpointPath: Path = path
  Files.createDirectories(checkpointPath)

  def saveLongToFile(value: Long): Unit = {
    try {
      Files.write(checkpointPath.resolve("offsetcheckpoint"), value.toString.getBytes(StandardCharsets.UTF_8))
      logDebug("Successfully update the custom offset checkpoint")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def readLongFromFile(): Option[Long] = {
    try {
      val lines = Files.readAllLines(checkpointPath.resolve("offsetcheckpoint"), StandardCharsets.UTF_8)
      if (lines.size() > 0) Try(lines.get(0).toLong).toOption else None
    } catch {
      case _ : NoSuchFileException => logError("Missing custom checkpoint file")
        None
      case e: Exception =>
        e.printStackTrace()
        None
    }
  }
}
