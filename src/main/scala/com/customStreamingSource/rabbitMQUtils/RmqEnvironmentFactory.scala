package com.customStreamingSource.rabbitMQUtils

import com.rabbitmq.stream.Environment

object RmqEnvironmentFactory {

  def getEnvironment(option: Map[String, String]): Environment = synchronized {
    val environment = createEnvironment(option)
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
