# Apache Spark Structured Streaming Connector for RabbitMQ Streaming Queue
 A customized streaming source for RabbitMQ streaming queue

# Constaints

- Table format is fixed as below
- | col        | type   | remark                                   |
  |------------|--------|------------------------------------------|
  | routingKey | String |                                          |
  | header     | String | json string                              |
  | body       | String |                                          |
  | receiveTimestamp | Timestamp| the timestamp that rmq broker receive it |

# How to use
Open input stream with this class:
- *org.apache.spark.sql.RmqStreamingSourceProvider*.

Following vm flags are required to start with jdk17+
- --add-opens java.base/sun.nio.ch=ALL-UNNAMED
- --add-opens java.base/sun.security.action=ALL-UNNAMED
- --add-opens java.base/java.io=ALL-UNNAMED
- --add-opens java.net.http/jdk.internal.net.http.common=ALL-UNNAMED

The behavior of the connector is controlled by below parameters:

- | field | example value | remark|
  |---|---|---|
  |rmq.vhost|/|
  |rmq.username|guest|
  |rmq.password|guest|
  |rmq.host|localhost| only support single host for now
  |rmq.port|5552|
  |rmq.queuename|stream|
  |rmq.fetchsize|20001| fetchsize must be greater than maxbatchsize
  |rmq.maxbatchsize|10000|
  |rmq.offsetcheckpointpath|checkpoint| Only file system based medium is supported, S3 interface is not supported yet

# Example code
For details, you could refer to the RmqStreamingTest file

# Bug/ Issue
It is just a Beta version, please raise any issues in the repo.