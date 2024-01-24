package hydra.kafka.endpoints

import java.time.Instant
import java.util.concurrent.TimeUnit
import akka.actor.ActorSelection
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import hydra.common.config.ConfigSupport._
import hydra.common.util.Futurable
import hydra.core.http.{CorsSupport, DefaultCorsSupport, NotFoundException, RouteSupport}
import hydra.core.monitor.HydraMetrics.addHttpMetric
import hydra.kafka.algebras.{MetadataAlgebra, TagsAlgebra}
import hydra.kafka.consumer.KafkaConsumerProxy.{GetPartitionInfo, ListTopics, ListTopicsResponse, PartitionInfoResponse}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{MetadataOnlyRequest, Schemas, SkipValidation, TopicMetadataV2Request, TopicMetadataV2Response}
import hydra.kafka.serializers.TopicMetadataV2Parser._
import hydra.kafka.util.KafkaUtils
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.apache.kafka.common.PartitionInfo
import scalacache._
import scalacache.guava.GuavaCache
import scalacache.modes.scalaFuture._

import scala.collection.immutable.Map
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import akka.http.scaladsl.server.Directives.{onComplete, parameter}
import akka.http.scaladsl.server.directives.Credentials
import cats.data.NonEmptyList
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.IncompatibleSchemaException
import hydra.common.config.KafkaConfigUtils.{KafkaClientSecurityConfig, kafkaClientSecurityConfig}
import hydra.common.validation.ValidationError
import hydra.core.http.security.AwsIamPolicyAction.KafkaAction
import hydra.core.http.security.security.RoleName
import hydra.core.http.security.{AccessControlService, AwsSecurityService}
import hydra.kafka.programs.CreateTopicProgram.MetadataOnlyTopicDoesNotExist
import hydra.kafka.programs.CreateTopicProgram
import org.apache.avro.{Schema, SchemaParseException}
import spray.json.DeserializationException

/**
  * A cluster metadata endpoint implemented exclusively with akka streams.
  *
  * Created by alexsilva on 3/18/17.
  */
class TopicMetadataEndpoint[F[_]: Futurable](consumerProxy:ActorSelection,
                                             metadataAlgebra: MetadataAlgebra[F],
                                             schemaRegistry: SchemaRegistry[F],
                                             createTopicProgram: CreateTopicProgram[F],
                                             defaultMinInsyncReplicas: Short,
                                             tagsAlgebra: TagsAlgebra[F],
                                             auth: AccessControlService[F],
                                             awsSecurityService: AwsSecurityService[F]
                                            )
                                            (implicit ec:ExecutionContext, corsSupport: CorsSupport)
  extends RouteSupport
    with HydraKafkaJsonSupport
    with DefaultCorsSupport {

  private implicit val cache = GuavaCache[Map[String, Seq[PartitionInfo]]]

  private val showSystemTopics = applicationConfig
    .getBooleanOpt("transports.kafka.show-system-topics")
    .getOrElse(false)

  private implicit val createTopicFormat = jsonFormat4(CreateTopicReq)

  private implicit val errorFormat = jsonFormat1(CreateTopicResponseError)

  implicit val timeout = Timeout(5 seconds)

  private lazy val kafkaUtils = KafkaUtils()

  private val filterSystemTopics = (t: String) =>
    (t.startsWith("_") && showSystemTopics) || !t.startsWith("_")

  override val route = cors(corsSupport.settings) {
    extractMethod { method =>
      extractExecutionContext { implicit ec =>
        pathPrefix("transports" / "kafka") {
          val startTime = Instant.now
          handleExceptions(exceptionHandler(startTime, method.value)) {
            get {
              path("topics") {
                parameters('pattern ?, 'fields ?) { (pattern, n) =>
                  val topicList = pattern.map(filterByPattern) getOrElse topics
                  n match {
                    case Some(_) =>
                      val response = topicList.map(_.keys)
                      addHttpMetric("", StatusCodes.OK, "/transports/kafka/topics-keys", startTime, "GET")
                      complete(response)
                    case None =>
                      addHttpMetric("", StatusCodes.OK, "/transports/kafka/topics", startTime, "GET")
                      complete(topicList)
                  }
                }
              } ~ path("topics" / Segment) { name =>
                onSuccess(topics) { topics =>
                  topics
                    .get(name)
                    .map { response =>
                      addHttpMetric(name, StatusCodes.OK, "/transports/kafka/topics/", startTime, "GET")
                      complete(response)
                    }
                    .getOrElse(
                      failWith {
                        addHttpMetric(name, StatusCodes.NotFound, "/transports/kafka/topics/", startTime, "GET", error = Some(s"Topic $name not found"))
                        new NotFoundException(s"Topic $name not found.")
                      }
                    )
                }
              }
            } ~ createTopic(startTime)
          }
        } ~ pathPrefix("v2" / "topics") {
          get {
            val startTime = Instant.now
            pathPrefix(Segment) { topicName =>
              getV2Metadata(topicName, startTime)
            } ~
            pathEndOrSingleSlash {
              getAllV2Metadata(startTime)
            }
          }
        } ~ pathPrefix("v2" / "streams") {
          val startTime = Instant.now
          get {
            getAllV2Metadata(startTime)
          }
        } ~ pathPrefix("v2" / "metadata" / Segment) { topic =>
          val startTime = Instant.now
          handleExceptions(exceptionHandler(startTime, method.value, topic)) {
            put {
              auth.mskAuth(KafkaAction.CreateTopic) { roleName =>
                parameter(Symbol("skipValidations").as[List[SkipValidation]].?) { maybeSkipValidations =>
                  putV2Metadata(startTime, topic, roleName, maybeSkipValidations)
                }
              }
            }
          }
        }
      }
    }
  }

  def getKeyValSchema(subject: Subject): Future[Schemas] = {
    for {
      keySchema <- Futurable[F].unsafeToFuture(schemaRegistry.getLatestSchemaBySubject(subject + "-key"))
      valueSchema <- Futurable[F].unsafeToFuture(schemaRegistry.getLatestSchemaBySubject(subject + "-value"))
    } yield Schemas(keySchema.getOrElse(throw new SchemaParseException("Unable to get Key Schema, please create Key Schema in Schema Registry and try again")),
      valueSchema.getOrElse(throw new SchemaParseException("Unable to get Value Schema, please create Value Schema in Schema Registry and try again")))
  }

  private def putV2Metadata(startTime: Instant, topic: String, userRoleName: Option[RoleName], maybeSkipValidations: Option[List[SkipValidation]]): Route = {
    extractMethod { method =>
      Subject.createValidated(topic) match {
        case Some(t) => {
          entity(as[MetadataOnlyRequest]) { mor =>
            if (mor.tags.isEmpty) {
              addHttpMetric(topic, StatusCodes.BadRequest, "/v2/metadata", startTime, method.value)
              complete(StatusCodes.BadRequest, s"You must include at least one tag to create metadata for topic: $topic")
            } else {
              onComplete(Futurable[F].unsafeToFuture(tagsAlgebra.validateTags(mor.tags))) {
                case Failure(exception) =>{
                  addHttpMetric(topic, StatusCodes.BadRequest, "/v2/metadata", startTime, method.value, error = Some(exception.getMessage))
                  complete(StatusCodes.BadRequest, exception.getMessage)
                }
                case Success(_) =>
                  onComplete(getKeyValSchema(t)) {
                    case Failure(exception) => {
                      addHttpMetric(topic, StatusCodes.BadRequest, "/v2/metadata", startTime, method.value)
                      complete(StatusCodes.BadRequest, exception.getMessage)
                    }
                    case Success(schemas) => {
                      val req = TopicMetadataV2Request.fromMetadataOnlyRequest(schemas, mor)
                      onComplete(
                        Futurable[F].unsafeToFuture(createTopicProgram
                          .createTopicFromMetadataOnly(t, req, withRequiredFields = true, maybeSkipValidations))
                      ) {
                        case Failure(exception) => exception match {
                          case e @ (_:IncompatibleSchemaException | _:ValidationError) =>
                            addHttpMetric(topic, StatusCodes.BadRequest, "/v2/metadata", startTime, method.value, error=Some(e.getMessage))
                            complete(StatusCodes.BadRequest, s"Unable to create Metadata for topic $topic : ${exception.getMessage}")
                          case e:MetadataOnlyTopicDoesNotExist =>
                            addHttpMetric(topic, StatusCodes.BadRequest, "/v2/metadata", startTime, method.value, error=Some(e.getMessage))
                            complete(StatusCodes.BadRequest, s"Unable to create Metadata for topic $topic : ${exception.getMessage}")
                          case _ =>
                            addHttpMetric(topic, StatusCodes.InternalServerError, "/v2/metadata", startTime, method.value, error=Some(exception.getMessage))
                            complete(StatusCodes.InternalServerError, s"Unable to create Metadata for topic $topic : ${exception.getMessage}")
                        }

                        case Success(value) =>
                          addHttpMetric(topic, StatusCodes.OK, "/v2/metadata", startTime, method.value)
                          if (userRoleName.isDefined) Futurable[F].unsafeToFuture(awsSecurityService.addAllTopicPermissionsPolicy(topic, userRoleName.get))
                          complete(StatusCodes.OK)
                      }
                    }
                  }
              }
            }
          }
        }
        case None =>
          addHttpMetric(topic, StatusCodes.BadRequest, "V2Bootstrap", startTime, "PUT", error = Some(Subject.invalidFormat))
          complete(StatusCodes.BadRequest, Subject.invalidFormat)
      }
    }

  }

  private def getAllV2Metadata(startTime: Instant): Route = {
    pathEndOrSingleSlash {
      onComplete(Futurable[F].unsafeToFuture(metadataAlgebra.getAllMetadata)) {
        case Success(metadata) =>
          val response = metadata.map(TopicMetadataV2Response.fromTopicMetadataContainer).filterNot(_.subject.value.startsWith("_"))
          addHttpMetric("", StatusCodes.OK,"/v2/streams", startTime, "GET")
          complete(StatusCodes.OK, response)
        case Failure(e) =>
          addHttpMetric("", StatusCodes.InternalServerError, "/v2/streams", startTime, "GET", error = Some(e.getMessage))
          complete(StatusCodes.InternalServerError, e)
      }
    }
  }

  private def getV2Metadata(topic: String, startTime: Instant) = get {
    pathEndOrSingleSlash {
      Subject.createValidated(topic) match {
        case None =>
          addHttpMetric(topic, StatusCodes.BadRequest,"/v2/topics/", startTime, "GET", error = Some(Subject.invalidFormat))
          complete(StatusCodes.BadRequest, Subject.invalidFormat)
        case Some(subject) =>
          onComplete(Futurable[F].unsafeToFuture(metadataAlgebra.getMetadataFor(subject))) {
            case Success(maybeContainer) =>
              maybeContainer match {
                case Some(container) =>
                  addHttpMetric(topic, StatusCodes.OK, "/v2/topics/", startTime, "GET")
                  complete(StatusCodes.OK, TopicMetadataV2Response.fromTopicMetadataContainer(container))
                case None =>
                  addHttpMetric(topic, StatusCodes.NotFound, "/v2/topics/", startTime, "GET", error = Some(s"Subject ${subject.value} could not be found."))
                  complete(StatusCodes.NotFound, s"Subject ${subject.value} could not be found.")
              }
            case Failure(e) =>
              addHttpMetric(topic, StatusCodes.InternalServerError,"/v2/topics/", startTime, "GET", error = Some(e.getMessage))
              complete(StatusCodes.InternalServerError, e)
          }
      }
    }
  }

  private def filterByPattern(
                               pattern: String
                             ): Future[Map[String, Seq[PartitionInfo]]] =
    topics.map(_.filter(e => e._1 matches pattern))

  private def createTopic(startTime: Instant) = path("topics") {
    post {
      entity(as[CreateTopicReq]) { req =>
        val details =
          new TopicDetails(req.partitions, req.replicationFactor, defaultMinInsyncReplicas, req.config)
        val to = timeout.duration.toMillis.toInt
        onSuccess(kafkaUtils.createTopic(req.topic, details, to)) { result =>
          Try(result.all.get(timeout.duration.toSeconds, TimeUnit.SECONDS))
            .map { _ =>
              addHttpMetric(req.topic, StatusCodes.OK, "TopicCreation", startTime, "POST")
              complete(StatusCodes.OK, topicInfo(req.topic))}
            .recover {
              case e: Exception =>
                addHttpMetric(req.topic,StatusCodes.BadRequest, "TopicCreation", startTime, "POST", error = Some(e.getMessage))
                complete(
                  StatusCodes.BadRequest,
                  CreateTopicResponseError(e.getMessage)
                )
            }
            .get
        }
      }
    }
  }

  private def topicInfo(name: String): Future[Seq[PartitionInfo]] = {
    import akka.pattern.ask
    (consumerProxy ? GetPartitionInfo(name))
      .mapTo[PartitionInfoResponse]
      .map(_.partitionInfo)
  }

  private def topics: Future[Map[String, Seq[PartitionInfo]]] = {
    implicit val timeout = Timeout(5 seconds)
    cachingF("topics")(ttl = Some(1.minute)) {
      import akka.pattern.ask
      (consumerProxy ? ListTopics).mapTo[ListTopicsResponse].map { response =>
        response.topics.filter(t => filterSystemTopics(t._1)).map {
          case (k, v) => k -> v.toList
        }
      }
    }
  }

  private def exceptionHandler(startTime: Instant, method: String, topic: String = "") = ExceptionHandler {
    case e: IllegalArgumentException =>
      addHttpMetric("",StatusCodes.BadRequest, "topicMetadataEndpoint", startTime, method, error = Some(e.getMessage))
      complete(HttpResponse(BadRequest, entity = e.getMessage))
    case e: NotFoundException =>
      addHttpMetric("",StatusCodes.NotFound, "topicMetadataEndpoint", startTime, method, error = Some(e.getMessage))
      complete(HttpResponse(NotFound, entity = e.msg))
    case e: DeserializationException =>
      addHttpMetric(topic, StatusCodes.BadRequest, "topicMetadataEndpoint", startTime, method, error=Some(e.getMessage))
      complete(HttpResponse(BadRequest, entity = e.getMessage))
    case e =>
      addHttpMetric("", InternalServerError, "topicMetadataEndpoint", startTime, method, error = Some(e.getMessage))
      complete(HttpResponse(InternalServerError, entity = e.getMessage))
  }
}

case class CreateTopicReq(
                           topic: String,
                           partitions: Int,
                           replicationFactor: Short,
                           config: Map[String, String]
                         )

case class CreateTopicResponseError(error: String)
