package com.rajesh.cricket.ingestion.streaming

import com.rajesh.cricket.utils.KafkaUtils
import org.apache.kafka.clients.producer.{KafkaProducer => ApacheKafkaProducer, ProducerRecord}
import org.apache.logging.log4j.LogManager

/**
 * Wraps the Apache Kafka producer for easy JSON message publishing.
 * Configured with acks=all and retries for reliable delivery.
 */
class KafkaProducer(bootstrapServers: String) extends AutoCloseable {

  private val logger   = LogManager.getLogger(getClass)
  private val props    = KafkaUtils.producerProps(bootstrapServers)
  private val producer = new ApacheKafkaProducer[String, String](props)

  /**
   * Send a message to the specified Kafka topic.
   *
   * @param topic  Kafka topic name
   * @param key    Message key (used for partitioning)
   * @param value  Message value (JSON string)
   */
  def send(topic: String, key: String, value: String): Unit = {
    val record = new ProducerRecord[String, String](topic, key, value)
    producer.send(record, (metadata, exception) => {
      if (exception != null) {
        logger.error(s"Failed to send message to topic $topic: ${exception.getMessage}")
      } else {
        logger.debug(
          s"Sent to $topic partition=${metadata.partition()} offset=${metadata.offset()}"
        )
      }
    })
  }

  /** Flush pending messages and close the producer connection. */
  def flush(): Unit = producer.flush()

  override def close(): Unit = {
    producer.flush()
    producer.close()
    logger.info("KafkaProducer closed.")
  }
}
