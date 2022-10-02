package com.clearurdoubt.kafka.producer.simple

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties

object SimpleKafkaProducer {

  val logger: Logger = LoggerFactory.getLogger(SimpleKafkaProducer.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Kafka Producer")

    // Producer Properties
    val producerProps: Properties = {
      val props = new Properties()
      props.put(ProducerConfig.ACKS_CONFIG, "all")
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      props
    }

    // Create KafkaProducer
    val producer = new KafkaProducer[String, String](producerProps)
    val producerRecord = new ProducerRecord[String, String]("my_topic", null, "Hello World!")

    // Send data to Kafka
    producer.send(producerRecord)

    // Close the Producer
    producer.close()
  }
}
