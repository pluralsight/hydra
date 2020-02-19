package hydra.kafka.endpoints

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.ExceptionHandler
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import configs.syntax._
import hydra.common.logging.LoggingAdapter
import hydra.common.util.ActorUtils
import hydra.core.http.{CorsSupport, HydraDirectives, NotFoundException}
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.consumer.KafkaConsumerProxy.{GetPartitionInfo, ListTopics, ListTopicsResponse, PartitionInfoResponse}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.util.KafkaUtils
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.kafka.common.PartitionInfo
import scalacache._
import scalacache.guava.GuavaCache
import scalacache.modes.scalaFuture._

import scala.collection.immutable.Map
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * A cluster metadata endpoint implemented exclusively with akka streams.
  *
  * Created by alexsilva on 3/18/17.
  */
class TopicMetadataEndpoint(implicit system: ActorSystem, implicit val ec: ExecutionContext)
  extends RoutedEndpoints
    with LoggingAdapter
    with HydraDirectives
    with HydraKafkaJsonSupport
    with CorsSupport {

  private implicit val cache = GuavaCache[Map[String, Seq[PartitionInfo]]]

  private val showSystemTopics = applicationConfig
    .get[Boolean]("transports.kafka.show-system-topics").valueOrElse(false)

  private val consumerPath = applicationConfig.get[String]("actors.kafka.consumer_proxy.path")
    .valueOrElse(s"/user/service/${ActorUtils.actorName(classOf[KafkaConsumerProxy])}")

  private val consumerProxy = system.actorSelection(consumerPath)

  private implicit val createTopicFormat = jsonFormat4(CreateTopicReq)

  private implicit val errorFormat = jsonFormat1(CreateTopicResponseError)

  implicit val timeout = Timeout(5 seconds)

  private lazy val kafkaUtils = KafkaUtils()

  private val filterSystemTopics = (t: String) => (t.startsWith("_") && showSystemTopics) || !t.startsWith("_")

  override val route = cors(settings) {
    pathPrefix("transports" / "kafka") {
      handleExceptions(exceptionHandler) {
        get {
          path("topics") {
            parameters('pattern ?, 'fields ?) { (pattern, n) =>
              val topicList = pattern.map(filterByPattern) getOrElse topics
              n match {
                case Some(_) => complete(topicList.map(_.keys))
                case None => complete(topicList)
              }
            }
          } ~ path("topics" / Segment) { name =>
            onSuccess(topics) { topics =>
              topics.get(name).map(complete(_)).getOrElse(failWith(new NotFoundException(s"Topic $name not found.")))
            }
          }
        } ~ createTopic
      }
    }
  }

  private def filterByPattern(pattern: String): Future[Map[String, Seq[PartitionInfo]]] =
    topics.map(_.filter(e => e._1 matches pattern))


  private def createTopic = path("topics") {
    post {
      entity(as[CreateTopicReq]) { req =>
        val details = new TopicDetails(req.partitions, req.replicationFactor, req.config)
        val to = timeout.duration.toMillis.toInt
        onSuccess(kafkaUtils.createTopic(req.topic, details, to)) { result =>
          Try(result.all.get(timeout.duration.toSeconds, TimeUnit.SECONDS))
            .map(_ => complete(StatusCodes.OK, topicInfo(req.topic)))
            .recover { case e: Exception => complete(StatusCodes.BadRequest,
              CreateTopicResponseError(e.getMessage)) }.get
        }
      }
    }
  }

  private def topicInfo(name: String): Future[Seq[PartitionInfo]] = {
    import akka.pattern.ask
    (consumerProxy ? GetPartitionInfo(name)).mapTo[PartitionInfoResponse].map(_.partitionInfo)
  }

  private def topics: Future[Map[String, Seq[PartitionInfo]]] = {
    implicit val timeout = Timeout(5 seconds)
    cachingF("topics")(ttl = Some(1.minute)) {
      import akka.pattern.ask
      (consumerProxy ? ListTopics).mapTo[ListTopicsResponse].map { response =>
        response.topics.filter(t => filterSystemTopics(t._1)).map { case (k, v) => k -> v.toList }
      }
    }
  }

  val exceptionHandler = ExceptionHandler {
    case e: IllegalArgumentException =>
      complete(HttpResponse(BadRequest, entity = e.getMessage))
    case e: NotFoundException =>
      complete(HttpResponse(NotFound, entity = e.msg))
  }
}

case class CreateTopicReq(topic: String,
                          partitions: Int,
                          replicationFactor: Short,
                          config: Map[String, String])


case class CreateTopicResponseError(error:String)






