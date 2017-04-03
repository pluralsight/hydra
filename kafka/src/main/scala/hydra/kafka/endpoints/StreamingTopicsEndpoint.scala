package hydra.kafka.endpoints

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.consumer.ConsumerSupport
import hydra.kafka.marshallers.HydraKafkaJsonSupport

import scala.concurrent.duration._

/**
  * A cluster metadata endpoint implemented exclusively with akka streams.
  *
  * Created by alexsilva on 3/18/17.
  */
class StreamingTopicsEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraDirectives with ConsumerSupport with HydraKafkaJsonSupport {

  implicit val ec = actorRefFactory.dispatcher

  implicit val jsonStreamingSupport = EntityStreamingSupport.json()

  override val route = path("transports" / "kafka" / "streaming" / Segment) { topicName =>
    get {
      parameters('format.?, 'group.?) { (format, groupId) =>
        val settings = loadConsumerSettings[Any, Any](format.getOrElse("avro"), groupId.getOrElse("hydra"))
        val source = Consumer.plainSource(settings, Subscriptions.topics(topicName))
          .map(consumerRecord => consumerRecord.value().toString)
          .keepAlive(20.seconds, () => "")
        complete(source)
      }
    }
  }
}

