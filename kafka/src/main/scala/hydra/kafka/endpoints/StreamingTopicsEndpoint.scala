package hydra.kafka.endpoints

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.core.marshallers.GenericServiceResponse
import hydra.kafka.util.KafkaUtils._
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.util.KafkaUtils

import scala.concurrent.duration._
import scala.util.{Random, Try}

/**
  * A cluster metadata endpoint implemented exclusively with akka streams.
  *
  * Created by alexsilva on 3/18/17.
  */
class StreamingTopicsEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraDirectives  with HydraKafkaJsonSupport {

  implicit val ec = actorRefFactory.dispatcher

  implicit val jsonStreamingSupport = EntityStreamingSupport.json()

  override val route = path("transports" / "kafka" / "streaming" / Segment) { topicName =>
    get {
      parameters('format.?, 'group.?, 'timeout ? "300s") { (format, groupId, timeout) =>
        if (!KafkaUtils.topicExists(topicName)) {
          complete(404, GenericServiceResponse(404, s"Topic $topicName doesn't exist."))
        } else {
          complete(buildStream(timeout, format, topicName, groupId))
        }
      }
    }
  }

  private def buildStream(timeout: String, format: Option[String], topicName: String, groupId: Option[String]) = {
    val fd = Try(Duration(timeout)) collect { case d: FiniteDuration => d }
    val idleTimeout = fd.getOrElse(FiniteDuration(5, TimeUnit.SECONDS))
    val settings = loadConsumerSettings[Any, Any](format.getOrElse("avro"),
      groupId.getOrElse("hydra-" + Random.alphanumeric.take(8) mkString))
    Consumer.plainSource(settings, Subscriptions.topics(topicName))
      .map(consumerRecord => consumerRecord.value().toString)
      // .keepAlive(20.seconds, () => "\"\"")
      .idleTimeout(idleTimeout)
  }
}

