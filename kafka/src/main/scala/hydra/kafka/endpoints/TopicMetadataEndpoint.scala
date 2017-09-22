package hydra.kafka.endpoints

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.ExceptionHandler
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.common.util.ActorUtils
import hydra.core.http.{CorsSupport, HydraDirectives, NotFoundException}
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.consumer.KafkaConsumerProxy.{ListTopics, ListTopicsResponse}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.model.TopicMetadata
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
  extends RoutedEndpoints with LoggingAdapter with HydraDirectives with HydraKafkaJsonSupport
    with CorsSupport {

  private implicit val cache = ScalaCache(GuavaCache())

  implicit val ec = actorRefFactory.dispatcher

  private val showSystemTopics = applicationConfig
    .get[Boolean]("transports.kafka.show-system-topics").valueOrElse(false)

  private val consumerProxy = actorRefFactory
    .actorSelection(s"/user/service/${ActorUtils.actorName(classOf[KafkaConsumerProxy])}")

  private val filterSystemTopics = (t: String) => (t.startsWith("_") && showSystemTopics) || !t.startsWith("_")

  override val route = cors(settings) {
    pathPrefix("transports" / "kafka") {
      get {
        path("topics") {
          parameters('names ?) { n =>
            n match {
              case Some(name) => complete(topics.map(_.keys))
              case None => complete(topics)
            }
          }
        } ~ path("topics" / Segment) { name =>
          onSuccess(topics) { topics =>
            topics.get(name).map { topic =>
              complete(TopicMetadata(name, "topic description", "Engineering", "Data Team", "John Smith"
                , name, Seq("certified", "revenue", "finance")))
            } getOrElse failWith(new NotFoundException(s"Topic $name not found."))
          }
        }
      }
    }
  }

  private def topics: Future[Map[String, Seq[PartitionInfo]]] = {
    implicit val timeout = Timeout(5 seconds)
    cachingWithTTL("topics")(30.seconds) {
      import akka.pattern.ask
      (consumerProxy ? ListTopics).mapTo[ListTopicsResponse].map { response =>
        response.topics.filter(t => filterSystemTopics(t._1)).map { case (k, v) => k -> v.toList }
      }
    }
  }

  val exceptionHandler = ExceptionHandler {
    case e: NotFoundException =>
      complete(HttpResponse(NotFound, entity = e.msg))
  }
}


