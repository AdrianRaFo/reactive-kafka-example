/**
 * https://github.com/AdrianRaFo
 */
package kexamples

import java.lang.Long
import java.util.Properties

import keyValue.implicits._
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}
import org.apache.kafka.streams.{KafkaStreams,  StreamsConfig}

import scala.collection.JavaConverters._

class StreamK {
  val config = new Properties()
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)

  val builder: KStreamBuilder            = new KStreamBuilder()
  val textLines: KStream[String, String] = builder.stream("TextLinesTopic")
  val wordCounts: KTable[String, Long] = textLines
    .flatMapValues[String]((value: String) ⇒ value.toLowerCase.split(" ").toIterable.asJava)
    .map[String, Integer]((_: String, value: String) ⇒ (value, new Integer(1)))
    .groupByKey(Serdes.String(), Serdes.Integer())
    .count()
  wordCounts.to(Serdes.String(), Serdes.Long(), "WordsWithCountsTopic")

  val streams: KafkaStreams = new KafkaStreams(builder, config)
    streams.start()
}
