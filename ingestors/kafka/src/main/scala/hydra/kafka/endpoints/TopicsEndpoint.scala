package hydra.kafka.endpoints

import akka.actor.ActorSelection
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.model.StatusCodes
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.pattern.ask
import akka.util.Timeout
import hydra.core.http.RouteSupport
import hydra.kafka.consumer.KafkaConsumerProxy.{GetLatestOffsets, LatestOffsetsResponse}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import hydra.core.monitor.HydraMetrics.addPromHttpMetric

import scala.collection.immutable.Map
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * A cluster metadata endpoint implemented exclusively with akka streams.
  *
  * Created by alexsilva on 3/18/17.
  */
class TopicsEndpoint(consumerProxy:ActorSelection)(implicit ec:ExecutionContext) extends RouteSupport {

  import hydra.kafka.util.KafkaUtils._

  implicit val jsonStreamingSupport = EntityStreamingSupport.json()

  override val route =
    path("transports" / "kafka" / "consumer" / "topics" / Segment) {
      topicName =>
        get {
          extractRequestContext { ctx =>
            parameters('format.?, 'group.?, 'n ? 10, 'start ? "earliest") {
              (format, groupId, n, startOffset) =>
                val settings = loadConsumerSettings[Any, Any](
                  format.getOrElse("avro"),
                  groupId.getOrElse("hydra"),
                  startOffset
                )
                val offsets = latestOffsets(topicName)
                val source = Consumer
                  .plainSource(settings, Subscriptions.topics(topicName))
                  .initialTimeout(5.seconds)
                  .zipWithIndex
                  .takeWhile(rec =>
                    rec._2 <= n && !shouldCancel(offsets, rec._1)
                  )
                  .map(rec => rec._1.value().toString)
                  .watchTermination()((_, termination) =>
                    termination.failed.foreach {
                      case cause => ctx.fail(cause)
                    }
                  )
                addPromHttpMetric(topicName, StatusCodes.OK.toString, "transports/kafka/consumer/topics/")
                complete(source)

            }
          }
        }
    }

  def shouldCancel(
      fpartitions: Future[Map[TopicPartition, Long]],
      record: ConsumerRecord[Any, Any]
  ): Boolean = {
    if (fpartitions.isCompleted) {
      val partitions = Await.result(fpartitions, 1.millis)
      val tp = new TopicPartition(record.topic(), record.partition())
      partitions.get(tp) match {
        case Some(offset) => record.offset() >= offset
        case None         => false
      }
    } else {
      false
    }

  }

  private def latestOffsets(
      topic: String
  ): Future[Map[TopicPartition, Long]] = {
    implicit val timeout = Timeout(5 seconds)
    (consumerProxy ? GetLatestOffsets(topic))
      .mapTo[LatestOffsetsResponse]
      .map(_.offsets)
  }

}
