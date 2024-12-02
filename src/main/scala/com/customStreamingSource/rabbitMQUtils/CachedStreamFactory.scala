package com.customStreamingSource.rabbitMQUtils

import com.rabbitmq.stream.Environment

import scala.collection.mutable

object CachedStreamFactory {
  private val environmentCache = mutable.Map[String, Environment]()

  def getEnvironment(option: Map[String, String]): Environment = synchronized {
    // Using ConnectionOption's string representation as the cache key
    val environment = environmentCache.getOrElseUpdate(option.toString, createEnvironment(option))
    environment
  }

  private def createEnvironment(option: Map[String, String]): Environment = {
    Environment.builder()
      .virtualHost(option.getOrElse("rmq.vhost", "/"))
      .username(option.getOrElse("rmq.username", "guest"))
      .password(option.getOrElse("rmq.password", "guest"))
      .host(option.getOrElse("rmq.host", "localhost"))
      .port(option.getOrElse("rmq.port", "5552").toInt)
      .build()
  }
}
