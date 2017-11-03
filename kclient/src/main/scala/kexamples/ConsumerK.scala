/**
 * https://github.com/AdrianRaFo
 */
package kexamples

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

class ConsumerK {
  val config = new Properties()
  config.put("bootstrap.servers", "localhost:9092")
  config.put("group.id", "test")
  config.put("enable.auto.commit", "true")
  config.put("auto.commit.interval.ms", "1000")
  config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  config.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer")

  val consumer = new KafkaConsumer[String, Long](config)
  consumer.subscribe(List("WordsWithCountsTopic").asJava)

  def receive() {
    val recordsIt = consumer.poll(0).iterator()
    while (recordsIt.hasNext) {
      val record = recordsIt.next()
      if (record.key().nonEmpty) println(s"${record.key()} , ${record.value()}")
    }
  }
}
