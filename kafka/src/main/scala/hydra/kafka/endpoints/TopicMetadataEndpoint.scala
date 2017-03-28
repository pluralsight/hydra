package hydra.kafka.endpoints

import akka.actor.{ActorRefFactory, ActorSystem}
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.core.http.{CorsSupport, HydraDirectives}
import hydra.kafka.consumer.ConsumerSupport
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import org.apache.kafka.common.PartitionInfo

import scala.concurrent.Future
import scala.concurrent.duration._
import scalacache._
import scalacache.guava.GuavaCache

/**
  * A cluster metadata endpoint implemented exclusively with akka streams.
  *
  * Created by alexsilva on 3/18/17.
  */
class TopicMetadataEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraDirectives with ConsumerSupport with HydraKafkaJsonSupport
    with CorsSupport {

  private implicit val cache = ScalaCache(GuavaCache())

  implicit val ec = actorRefFactory.dispatcher

  private val showSystemTopics = applicationConfig
    .get[Boolean]("transports.kafka.show-system-topics").valueOrElse(false)

  private val filterSystemTopics = (t: String) => (t.startsWith("_") && showSystemTopics) || !t.startsWith("_")

  override val route = corsHandler(
    get {
      path("transports" / "kafka" / "topics") {
        parameters('names ?) { n =>
          n match {
            case Some(name) => complete(topics.map(_.keys))
            case None => complete(topics)
          }
        }
      }
    })

  private def topics: Future[Map[String, List[PartitionInfo]]] = {
    import scala.collection.JavaConverters._
    cachingWithTTL("topics")(30.seconds) {
      Future(defaultConsumer.listTopics().asScala
        .filter(t => filterSystemTopics(t._1)).map { case (k, v) => k -> v.asScala.toList }.toMap)
    }
  }
}


