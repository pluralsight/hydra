package hydra.kafka.endpoints

import akka.Done
import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCode, StatusCodes}
import akka.http.scaladsl.model.StatusCodes.ServiceUnavailable
import akka.http.scaladsl.server.ExceptionHandler
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Source
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.core.marshallers.GenericServiceResponse
import hydra.kafka.consumer.ConsumerSupport
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.Map
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Success, Try}

/**
  * A cluster metadata endpoint implemented exclusively with akka streams.
  *
  * Created by alexsilva on 3/18/17.
  */
class TopicsEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraDirectives with ConsumerSupport with HydraKafkaJsonSupport {

  implicit val jsonStreamingSupport = EntityStreamingSupport.json()

  implicit val ec = actorRefFactory.dispatcher

  override val route = path("transports" / "kafka" / "consumer" / "topics" / Segment) { topicName =>
    get {
      extractRequestContext { ctx=>
        parameters('format.?, 'group.?, 'n ? 10, 'start ? "earliest") { (format, groupId, n, startOffset) =>
          val settings = loadConsumerSettings[Any, Any](format.getOrElse("avro"), groupId.getOrElse("hydra"), startOffset)
          val offsets = latestOffsets(topicName)
          val source = Consumer.plainSource(settings, Subscriptions.topics(topicName))
            .initialTimeout(5.seconds)
            .zipWithIndex
            .takeWhile(rec => rec._2 <= n && !shouldCancel(offsets, rec._1))
            .map(rec => rec._1.value().toString)
            .watchTermination()((_, termination) => termination.onFailure {
              case cause => ctx.fail(cause)
            })
          complete(source)

        }
      }
    }
  }

//  private def onResponseStreamEnd(response: HttpResponse)(action: StatusCode => Unit): HttpResponse =
//    if (!response.status.allowsEntity() || response.entity.isKnownEmpty()) {
//      action(response.status)
//      response
//    } else {
//      val dataBytes =
//        onStreamEnd(response.entity) { result =>
//          val overallStatusCode =
//            result match {
//              case Success(_) =>
//                response.status
//
//              case Failure(e) =>
//                logger.error(e, s"error streaming response [${e.getMessage}]")
//                StatusCodes.InternalServerError
//            }
//
//          action(overallStatusCode)
//        }
//
//      response.withEntity(response.entity.contentLengthOption match {
//        case Some(length) => HttpEntity(response.entity.contentType, length, dataBytes)
//        case None         => HttpEntity(response.entity.contentType, dataBytes)
//      })
//    }
//
//  private def onStreamEnd(entity: HttpEntity)(onComplete: Try[Done] ⇒ Unit): Source[ByteString, _] =
//    entity.dataBytes.alsoTo { Sink.onComplete(onComplete) }


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
}

