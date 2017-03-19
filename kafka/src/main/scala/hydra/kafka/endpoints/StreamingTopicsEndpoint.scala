package hydra.kafka.endpoints

import akka.NotUsed
import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.{Marshaller, Marshalling}
import akka.http.scaladsl.model.ContentTypes
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.kafka.akka.ConsumerSettingsHelper._
import hydra.kafka.config.KafkaConfigSupport

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * A cluster metadata endpoint implemented exclusively with akka streams.
  *
  * Created by alexsilva on 3/18/17.
  */
class StreamingTopicsEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraDirectives with KafkaConfigSupport with SprayJsonSupport {

  import spray.json._

  implicit val jsonStreamingSupport = EntityStreamingSupport.json()

  implicit val stringFormat = Marshaller[String, ByteString] { ec ⇒ s ⇒
    Future.successful {
      List(Marshalling.WithFixedContentType(ContentTypes.`application/json`, () ⇒
        ByteString("\"" + s + "\"")) // "raw string" to be rendered as json element in our stream must be enclosed by ""
      )
    }
  }

  override val route = path("kafka"/ "stream" / "topics" / Segment) { topicName =>
    get {
      parameters('format.?, 'group.?) { (format, groupId) =>
        loadConsumerSettings[Any, Any](topicName, format.getOrElse("avro"), groupId.getOrElse("hydra")) match {
          case Success(consumerSettings) =>
            val source = Consumer.plainSource(consumerSettings, Subscriptions.topics(topicName))
              .map(consumerRecord => consumerRecord.value().toString)
              .keepAlive(20.seconds, () => "")
            complete(source)
          case Failure(ex) => failWith(ex)
        }
      }
    }
  }
}

