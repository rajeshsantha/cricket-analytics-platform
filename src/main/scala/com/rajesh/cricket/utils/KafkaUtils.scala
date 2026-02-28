package com.rajesh.cricket.utils

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import com.rajesh.cricket.config.AppConfig

import java.util.Properties
import scala.jdk.CollectionConverters._

/** Utility helpers for Kafka administration and configuration. */
object KafkaUtils {

  /** Create Kafka producer configuration properties. */
  def producerProps(bootstrapServers: String = AppConfig.kafkaBootstrapServers): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer",   classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks",             "all")
    props.put("retries",          "3")
    props.put("batch.size",       "16384")
    props.put("linger.ms",        "1")
    props.put("buffer.memory",    "33554432")
    props
  }

  /** Create Kafka consumer configuration properties. */
  def consumerProps(
    groupId: String             = AppConfig.kafkaConsumerGroup,
    bootstrapServers: String    = AppConfig.kafkaBootstrapServers
  ): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers",  bootstrapServers)
    props.put("group.id",           groupId)
    props.put("key.deserializer",   classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("auto.offset.reset",  "earliest")
    props.put("enable.auto.commit", "false")
    props
  }

  /**
   * Create a Kafka topic via the AdminClient.
   *
   * @param topicName           Name of the topic to create
   * @param numPartitions       Number of partitions
   * @param replicationFactor   Replication factor
   * @param bootstrapServers    Kafka bootstrap servers
   */
  def createTopic(
    topicName: String,
    numPartitions: Int     = 3,
    replicationFactor: Short = 1,
    bootstrapServers: String = AppConfig.kafkaBootstrapServers
  ): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    val admin = AdminClient.create(props)
    try {
      val newTopic = new NewTopic(topicName, numPartitions, replicationFactor)
      admin.createTopics(List(newTopic).asJava).all().get()
    } finally {
      admin.close()
    }
  }

  /** Serialize a value to a JSON string (pass-through for pre-serialized JSON). */
  def serialize(value: String): String = value

  /** Deserialize bytes to string using UTF-8. */
  def deserialize(bytes: Array[Byte]): String = new String(bytes, "UTF-8")
}
