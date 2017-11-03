/**
 * https://github.com/AdrianRaFo
 */
package kexamples.actors

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

trait ProducerAK {
  val system = ActorSystem("afka-Producer")

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
  val kafkaProducer = producerSettings.createKafkaProducer()

  implicit val ec: ExecutionContextExecutor    = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer.create(system)

  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }
}

object ProducerSinkExample extends ProducerAK {
  def main(args: Array[String]): Unit = {
    val done = Source(1 to 100)
      .map(_.toString)
      .map { elem =>

        println(elem)
        new ProducerRecord[Array[Byte], String]("topic1", elem)
      }
      .runWith(Producer.plainSink(producerSettings))

    terminateWhenDone(done)
  }
}

object ProducerFlowExample extends ProducerAK {
  def main(args: Array[String]): Unit = {

    val done = Source(1 to 100)
      .map { n =>
        val partition = 0
        ProducerMessage.Message(
          new ProducerRecord[Array[Byte], String]("topic1", partition, null, n.toString),
          n)
      }
      .via(Producer.flow(producerSettings))
      .map { result =>
        val record = result.message.record
        println(
          s"${record.topic}/${record.partition} ${result.offset}: ${record.value}" + s"(${result.message.passThrough})")
        result
      }
      .runWith(Sink.ignore)

    terminateWhenDone(done)
  }
}
