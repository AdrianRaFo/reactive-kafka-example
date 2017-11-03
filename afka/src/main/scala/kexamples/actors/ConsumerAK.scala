/**
 * https://github.com/AdrianRaFo
 */
package kexamples.actors

import akka.Done
import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{
  ConsumerSettings,
  KafkaConsumerActor,
  ProducerMessage,
  ProducerSettings,
  Subscriptions
}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

trait ConsumerAK {
  val system                                = ActorSystem("example")
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  implicit val m: ActorMaterializer         = ActorMaterializer.create(system)

  val maxPartitions = 100

  // #settings
  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  //#settings
  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")

  def business[T] = Flow[T]

  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }
}
object ConsumerSinkExample extends ConsumerAK {
  def main(args: Array[String]): Unit = {
    val done = Consumer
      .committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .mapAsync(1) { msg =>
        println(s"$msg")
        Future.successful(Done).map(_ => msg)
      }
      .mapAsync(1) { msg =>
        msg.committableOffset.commitScaladsl()
      }
      .runWith(Sink.ignore)
    terminateWhenDone(done)

  }
}

object StreamSinkExample extends ConsumerAK {
  def main(args: Array[String]): Unit = {
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .map { msg =>
        println(s"topic1 -> topic2: $msg")
        ProducerMessage.Message(
          new ProducerRecord[Array[Byte], String]("topic2", msg.record.value),
          msg.committableOffset)
      }
      .runWith(Producer.commitableSink(producerSettings))
  }
}

object StreamWithBatchCommitsExample extends ConsumerAK {
  def main(args: Array[String]): Unit = {
    // #consumerToProducerFlowBatch
    val done = Consumer
      .committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .map(msg =>
        ProducerMessage.Message(
          new ProducerRecord[Array[Byte], String]("topic2", msg.record.value),
          msg.committableOffset))
      .via(Producer.flow(producerSettings))
      .map(_.message.passThrough)
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
        batch.updated(elem)
      }
      .mapAsync(3)(_.commitScaladsl())
      .runWith(Sink.ignore) // #consumerToProducerFlowBatch
    terminateWhenDone(done)
  }
}

object StreamFlowExample extends ConsumerAK {
  def main(args: Array[String]): Unit = {
    val done = Consumer
      .committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .map { msg =>
        println(s"topic1 -> topic2: $msg")
        ProducerMessage.Message(
          new ProducerRecord[Array[Byte], String]("topic2", msg.record.value),
          msg.committableOffset)
      }
      .via(Producer.flow(producerSettings))
      .mapAsync(producerSettings.parallelism) { result =>
        result.message.passThrough.commitScaladsl()
      }
      .runWith(Sink.ignore)

    terminateWhenDone(done)
  }
}

// Backpressure per partition with batch commit
object ConsumerWithPerPartitionBackpressure extends ConsumerAK {
  def main(args: Array[String]): Unit = {
    // #committablePartitionedSource
    val done = Consumer
      .committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
      .flatMapMerge(maxPartitions, _._2)
      .via(business)
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first.committableOffset)) {
        (batch, elem) =>
          batch.updated(elem.committableOffset)
      }
      .mapAsync(3)(_.commitScaladsl())
      .runWith(Sink.ignore) // #committablePartitionedSource
    terminateWhenDone(done)
  }
}

//externally controlled kafka consumer
object ExternallyControlledKafkaConsumer extends ConsumerAK {
  def main(args: Array[String]): Unit = {
    // #consumerActor
    //Consumer is represented by actor
    val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

    //Manually assign topic partition to it
    Consumer
      .plainExternalSource[Array[Byte], String](
        consumer,
        Subscriptions.assignment(new TopicPartition("topic1", 1)))
      .via(business)
      .runWith(Sink.ignore)

    //Manually assign another topic partition
    Consumer
      .plainExternalSource[Array[Byte], String](
        consumer,
        Subscriptions.assignment(new TopicPartition("topic1", 2)))
      .via(business)
      .runWith(Sink.ignore) // #consumerActor
  }
}
