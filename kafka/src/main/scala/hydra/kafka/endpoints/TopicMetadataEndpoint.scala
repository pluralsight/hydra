package hydra.kafka.endpoints

import akka.actor.{ActorRefFactory, ActorSystem}
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import hydra.common.logging.LoggingAdapter
import hydra.core.http.HydraDirectives
import hydra.kafka.akka.ConsumerSettingsHelper._
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scalacache._
import scalacache.guava.GuavaCache

/**
  * A cluster metadata endpoint implemented exclusively with akka streams.
  *
  * Created by alexsilva on 3/18/17.
  */
class TopicMetadataEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraDirectives with KafkaConfigSupport with HydraKafkaJsonSupport {

  implicit val cache = ScalaCache(GuavaCache())

  implicit val ec = actorRefFactory.dispatcher

  val consumer: Try[KafkaConsumer[String, String]] =
    loadConsumerSettings[String, String]("", "string", "hydra").map(_.createKafkaConsumer())

  override val route =
    get {
      path("kafka" / "topics") {
        parameters('names ?) { n =>
          consumer match {
            case Success(c) =>
              val topics = getTopics(c)
              n match {
                case Some(name) => complete(topics.map(_.keys))
                case None => complete(topics)
              }
            case Failure(ex) => failWith(ex)
          }
        }
      }
    }

  def getTopics(c: KafkaConsumer[String, String]): Future[Map[String, List[PartitionInfo]]] = {
    import scala.collection.JavaConverters._
    cachingWithTTL("topics")(30.seconds) {
      Future(c.listTopics().asScala.map { case (k, v) => k -> v.asScala.toList }.toMap)
    }
  }
}


