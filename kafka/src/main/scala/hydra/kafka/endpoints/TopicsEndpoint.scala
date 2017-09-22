package hydra.kafka.endpoints

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.util.Timeout
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.common.util.ActorUtils
import hydra.core.http.HydraDirectives
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.consumer.KafkaConsumerProxy.{GetLatestOffsets, LatestOffsetsResponse}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.Map
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * A cluster metadata endpoint implemented exclusively with akka streams.
  *
  * Created by alexsilva on 3/18/17.
  */
class TopicsEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraDirectives with HydraKafkaJsonSupport {

  import hydra.kafka.util.KafkaUtils._

  implicit val jsonStreamingSupport = EntityStreamingSupport.json()

  implicit val ec = actorRefFactory.dispatcher

  private val consumerProxy = actorRefFactory
    .actorSelection(s"/user/service/${ActorUtils.actorName(classOf[KafkaConsumerProxy])}")

  override val route = path("transports" / "kafka" / "consumer" / "topics" / Segment) { topicName =>
    get {
      extractRequestContext { ctx =>
        parameters('format.?, 'group.?, 'n ? 10, 'start ? "earliest") { (format, groupId, n, startOffset) =>
          val settings = loadConsumerSettings[Any, Any](format.getOrElse("avro"), groupId.getOrElse("hydra"), startOffset)
          val offsets = latestOffsets(topicName)
          val source = Consumer.plainSource(settings, Subscriptions.topics(topicName))
            .initialTimeout(5.seconds)
            .zipWithIndex
            .takeWhile(rec => rec._2 <= n && !shouldCancel(offsets, rec._1))
            .map(rec => rec._1.value().toString)
            .watchTermination()((_, termination) => termination.failed.foreach {
              case cause => ctx.fail(cause)
            })
          complete(source)

        }
      }
    }
  }

  def shouldCancel(fpartitions: Future[Map[TopicPartition, Long]], record: ConsumerRecord[Any, Any]): Boolean = {
    if (fpartitions.isCompleted) {
      val partitions = Await.result(fpartitions, 1.millis)
      val tp = new TopicPartition(record.topic(), record.partition())
      partitions.get(tp) match {
        case Some(offset) => record.offset() >= offset
        case None => false
      }
    }
    else {
      false
    }

  }

  private def latestOffsets(topic: String): Future[Map[TopicPartition, Long]] = {
    implicit val timeout = Timeout(5 seconds)
    import akka.pattern.ask
    (consumerProxy ? GetLatestOffsets(topic)).mapTo[LatestOffsetsResponse].map(_.offsets)
  }


}

