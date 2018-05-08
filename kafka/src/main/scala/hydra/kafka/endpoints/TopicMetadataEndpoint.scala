package hydra.kafka.endpoints

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.ExceptionHandler
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import configs.syntax._
import hydra.common.auth.AuthenticationDirectives
import hydra.common.logging.LoggingAdapter
import hydra.common.util.ActorUtils
import hydra.core.http.{CorsSupport, HydraDirectives, NotFoundException}
import hydra.kafka.config.KafkaConfigSupport
import hydra.kafka.consumer.KafkaConsumerProxy
import hydra.kafka.consumer.KafkaConsumerProxy.{GetPartitionInfo, ListTopics, ListTopicsResponse, PartitionInfoResponse}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.util.KafkaUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails
import org.apache.kafka.common.requests.CreateTopicsResponse

import scala.collection.immutable.Map
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scalacache._
import scalacache.guava.GuavaCache

import scala.collection.JavaConverters._
import scalacache.modes.scalaFuture._

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
    with CorsSupport
    with AuthenticationDirectives {

  private implicit val cache = GuavaCache[Map[String, Seq[PartitionInfo]]]

  private val showSystemTopics = applicationConfig
    .get[Boolean]("transports.kafka.show-system-topics").valueOrElse(false)

  private val consumerPath = applicationConfig.get[String]("actors.kafka.consumer_proxy.path")
    .valueOrElse(s"/user/service/${ActorUtils.actorName(classOf[KafkaConsumerProxy])}")

  private val consumerProxy = system.actorSelection(consumerPath)

  private implicit val createTopicFormat = jsonFormat4(CreateTopicReq)

  private implicit val errorFormat = jsonFormat1(CreateTopicResponseError)

  implicit val timeout = Timeout(5 seconds)

  private lazy val kafkaUtils = new KafkaUtils(KafkaConfigSupport.zkString,
    () => new ZkClient(KafkaConfigSupport.zkString))

  private val filterSystemTopics = (t: String) => (t.startsWith("_") && showSystemTopics) || !t.startsWith("_")

  override val route = cors(settings) {
    pathPrefix("transports" / "kafka") {
      handleExceptions(exceptionHandler) {
        get {
          path("topics") {
            parameters('names ?) { n =>
              n match {
                case Some(_) => complete(topics.map(_.keys))
                case None => complete(topics)
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

  private def createTopic = path("topics") {
    post {
      entity(as[CreateTopicReq]) { req =>
        val details = new TopicDetails(req.partitions, req.replicationFactor, req.config.asJava)
        val to = timeout.duration.toMillis.toInt
        onSuccess(Future.fromTry(kafkaUtils.createTopic(req.topic, details, to))) { f =>
          f.errors.asScala.find(_._2.error != Errors.NONE).map { _ =>
            complete(StatusCodes.BadRequest, fromKafka(f.errors.asScala.toMap))
          }.getOrElse(complete(StatusCodes.OK, topicInfo(req.topic)))
        }
      }
    }
  }


  private def fromKafka(errors: Map[String, CreateTopicsResponse.Error]) =
    CreateTopicResponseError(errors.map { err => err._1 -> err._2.message() })


  private def topicInfo(name: String): Future[Seq[PartitionInfo]] = {
    import akka.pattern.ask
    (consumerProxy ? GetPartitionInfo(name)).mapTo[PartitionInfoResponse].map(_.partitionInfo)

  }

  private def topics: Future[Map[String, Seq[PartitionInfo]]] = {
    implicit val timeout = Timeout(5 seconds)
    cachingF("topics")(ttl = Some(30.seconds)) {
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


case class CreateTopicResponseError(errors: Map[String, String])






