package com.clearurdoubt.kafka.producer.simple

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties

object SimpleKafkaProducerWithCallbackMultipleRecords {
  val logger: Logger = LoggerFactory.getLogger(SimpleKafkaProducerWithCallbackMultipleRecords.getClass.getSimpleName)

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

    (1 to 10) foreach { counter =>
      // Create a record
      val producerRecord = new ProducerRecord[String, String]("my_topic", null, s"Hello World - $counter!")

      // Create a Callback
      val producerCallback: Callback = (metadata: RecordMetadata, exception: Exception) => Option(exception) match {
        case Some(error) => logger.info(s"Unable to send the record: ${error.getMessage}")
        case None => logger.info(
          s"""
             |Record sent successfully.
             |Topic: ${metadata.topic()}
             |Partition: ${metadata.partition()}
             |Offset: ${metadata.offset()}
             |Timestap: ${metadata.timestamp()}
             |""".stripMargin)
      }

      // Send data to Kafka
      producer.send(producerRecord, producerCallback)
    }

    // Close the Producer
    producer.close()
  }
}
