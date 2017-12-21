package hydra.kafka.endpoints

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.core.marshallers.GenericServiceResponse
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.util.KafkaUtils
import hydra.kafka.util.KafkaUtils._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}

/**
  * A cluster metadata endpoint implemented exclusively with akka streams.
  *
  * Created by alexsilva on 3/18/17.
  */
class StreamingTopicsEndpoint(implicit s: ActorSystem, implicit val e: ExecutionContext)
  extends RoutedEndpoints with LoggingAdapter with HydraDirectives with HydraKafkaJsonSupport {

  implicit val jsonStreamingSupport = EntityStreamingSupport.json()

  override val route = path("transports" / "kafka" / "streaming" / Segment) { topicName =>
    get {
      parameters('format.?, 'group.?, 'timeout ? "300s") { (format, groupId, timeout) =>
        KafkaUtils.topicExists(topicName) match {
          case Success(e) if e => complete(buildStream(timeout, format, topicName, groupId))
          case Success(e) if !e => complete(404, GenericServiceResponse(404, s"Topic $topicName doesn't exist."))
          case Failure(ex) => ex.printStackTrace; complete(503, GenericServiceResponse(503, ex.getMessage))
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

