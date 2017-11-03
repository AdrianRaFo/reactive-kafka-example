/**
 * https://github.com/AdrianRaFo
 */
package kexamples

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.StdIn

class ProducerK{

  val config = new Properties()
  config.put("bootstrap.servers", "localhost:9092")
  config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](config)

  def send() {
    println(">>> Press ENTER to send <<<")
    producer.send(new ProducerRecord[String, String]("TextLinesTopic", StdIn.readLine()))
  }
}
