package com.clearurdoubt.kafka.simple.consumer

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import java.util.{Collections, Properties}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object SimpleKafkaConsumer {
  val logger: Logger = LoggerFactory.getLogger(SimpleKafkaConsumer.getClass.getName)

  def main(args: Array[String]): Unit = {
    val topic: String = "my_topic"

    val consumerProperties: Properties = {
      val props: Properties = new Properties()
      props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
      props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_group_id")
      props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      props
    }

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProperties)

    consumer.subscribe(Collections.singletonList(topic))

    while(true) {
      val records = consumer.poll(Duration.ofMinutes(1)).asScala

      for(record <- records) {
        logger.info(s"Key: ${record.key()}, Value: ${record.value()}")
      }
    }
  }
}
