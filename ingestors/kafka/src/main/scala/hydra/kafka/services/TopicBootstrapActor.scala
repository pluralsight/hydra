package hydra.kafka.services

import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.Status.{Failure => AkkaFailure}
import akka.actor.{
  Actor,
  ActorLogging,
  ActorRef,
  ActorSelection,
  Props,
  Stash,
  Timers
}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import ConfigSupport._
import hydra.core.akka.SchemaRegistryActor.{
  RegisterSchemaRequest,
  RegisterSchemaResponse
}
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.core.marshallers.{
  GenericSchema,
  History,
  HydraJsonSupport,
  TopicMetadataRequest
}
import hydra.core.protocol.{Ingest, IngestorCompleted, IngestorError}
import hydra.core.transport.{AckStrategy, ValidationStrategy}
import hydra.kafka.model.TopicMetadata
import hydra.kafka.producer.{AvroRecord, AvroRecordFactory}
import hydra.kafka.services.StreamsManagerActor.{
  GetMetadata,
  GetMetadataResponse,
  StopStream
}
import hydra.kafka.util.KafkaUtils
import hydra.kafka.util.KafkaUtils.TopicDetails

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.Source
import scala.util._

class TopicBootstrapActor(
    schemaRegistryActor: ActorRef,
    kafkaIngestor: ActorSelection,
    streamsManagerProps: Props,
    bootstrapConfig: Option[Config] = None
) extends Actor
    with ActorLogging
    with ConfigSupport
    with HydraJsonSupport
    with Stash
    with Timers {

  import TopicBootstrapActor._
  import spray.json._

  implicit val metadataFormat = jsonFormat11(TopicMetadata)

  implicit val ec = context.dispatcher

  implicit val timeout = Timeout(10.seconds)

  val schema = Source.fromResource("HydraMetadataTopic.avsc").mkString

  private val kafkaUtils = KafkaUtils()

  val bootstrapKafkaConfig: Config = bootstrapConfig getOrElse
    applicationConfig.getConfig("bootstrap-config")

  val topicDetails = new TopicDetails(
    bootstrapKafkaConfig.getInt("partitions"),
    bootstrapKafkaConfig.getInt("replication-factor").toShort,
    Map.empty
  )

  private val failureRetryInterval = bootstrapKafkaConfig
    .getIntOpt("failure-retry-millis")
    .getOrElse(2000)

  private val streamsManagerActor = context.actorOf(streamsManagerProps)

  override def preStart(): Unit = {
    pipe(registerSchema(schema)) to self
  }

  override def postStop(): Unit = {
    streamsManagerActor ! StopStream
  }

  override def receive: Receive = initializing

  def initializing: Receive = {
    case RegisterSchemaResponse(_) =>
      context.become(active)
      unstashAll()

    case AkkaFailure(ex) =>
      log.error(
        s"TopicBootstrapActor entering failed state due to: ${ex.getMessage}"
      )
      unstashAll()
      timers.startSingleTimer(
        "retry-failure",
        Retry,
        failureRetryInterval.millis
      )
      context.become(failed(ex))

    case _ => stash()
  }

  def active: Receive = {
    case InitiateTopicBootstrap(topicMetadataRequest) =>
      (getGenericSchema(topicMetadataRequest) match {
        case Some(schema) =>
          (streamsManagerActor ? GetMetadata)
            .mapTo[GetMetadataResponse]
            .flatMap { metadataReponse =>
              metadataReponse.metadata.get(schema.subject) match {
                case Some(topicMetadata) =>
                  executeEndpoint(topicMetadataRequest, Some(topicMetadata))
                case None =>
                  TopicMetadataValidator.validate(Some(schema)) match {
                    case Success(_) =>
                      executeEndpoint(topicMetadataRequest)
                    case Failure(ex: SchemaValidatorException) =>
                      Future(BootstrapFailure(ex.reasons))
                    case Failure(e) =>
                      Future(BootstrapFailure(e.getMessage :: Nil))
                  }
              }
            }
        case None =>
          Future(BootstrapFailure(ErrorMessages.SchemaError :: Nil))
      }) pipeTo sender()

    case GetStreams(subject) =>
      val streams: Future[GetStreamsResponse] =
        (streamsManagerActor ? GetMetadata)
          .mapTo[GetMetadataResponse]
          .map { metadataResponse =>
            val resp = subject.map(s =>
              metadataResponse.metadata.values.filter(p => p.subject == s)
            ) getOrElse metadataResponse.metadata.values
            GetStreamsResponse(resp.toSeq)
          }

      pipe(streams) to sender
  }

  def failed(ex: Throwable): Receive = {
    case Retry =>
      log.info("Retrying metadata schema registration...")
      context.become(initializing)
      pipe(registerSchema(schema)) to self

    case _ =>
      val failureMessage =
        s"TopicBootstrapActor is in a failed state due to cause: ${ex.getMessage}"
      Future.failed(new Exception(failureMessage)) pipeTo sender
  }

  private def executeEndpoint(
      topicMetadataRequest: TopicMetadataRequest,
      existingTopicMetadata: Option[TopicMetadata] = None
  ): Future[BootstrapResult] = {
    val result = for {
      schema <- registerSchema(topicMetadataRequest.schema.compactPrint)
      topicMetadata <- ingestMetadata(
        topicMetadataRequest,
        schema.schemaResource.id,
        existingTopicMetadata
      )
      _ <- createKafkaTopics(topicMetadataRequest)
    } yield topicMetadata

    result
      .map(BootstrapSuccess.apply)
      .recover {
        case e => BootstrapFailure(Seq(e.getMessage))
      }
  }

  private[kafka] def registerSchema(
      schemaJson: String
  ): Future[RegisterSchemaResponse] = {
    (schemaRegistryActor ? RegisterSchemaRequest(schemaJson))
      .mapTo[RegisterSchemaResponse]
  }

  private[kafka] def ingestMetadata(
      topicMetadataRequest: TopicMetadataRequest,
      schemaId: Int,
      existingTopicMetadata: Option[TopicMetadata] = None
  ): Future[TopicMetadata] = {

    val schema = getGenericSchema(topicMetadataRequest)
    val topicMetadata = TopicMetadata(
      schema
        .map(_.subject)
        .getOrElse("Schema does not conform to GenericSchema"),
      schemaId,
      topicMetadataRequest.streamType.toString,
      topicMetadataRequest.derived,
      topicMetadataRequest.deprecated,
      topicMetadataRequest.dataClassification,
      topicMetadataRequest.contact,
      topicMetadataRequest.additionalDocumentation,
      topicMetadataRequest.notes,
      UUID.randomUUID(),
      existingTopicMetadata
        .map(_.createdDate)
        .getOrElse(org.joda.time.DateTime.now())
    )

    buildAvroRecord(topicMetadata)
      .flatMap { record =>
        (kafkaIngestor ? Ingest(record, record.ackStrategy)).map {
          case IngestorCompleted => topicMetadata
          case IngestorError(ex) =>
            val errorMessage = ex.getMessage
            log.error(
              s"TopicBootstrapActor received an IngestorError from KafkaIngestor: $errorMessage"
            )
            throw ex
        }
      }
      .recover {
        case ex: Throwable =>
          val errorMessage = ex.getMessage
          log.error(
            s"Unexpected error occurred during initiateBootstrap: $errorMessage"
          )
          throw ex
      }
  }

  private[kafka] def buildAvroRecord(
      metadata: TopicMetadata
  ): Future[AvroRecord] = {

    val jsonString = metadata.toJson.compactPrint
    val metadataTopicName = bootstrapKafkaConfig
      .getStringOpt("metadata-topic-name")
      .getOrElse("_hydra.metadata.topic")
    new AvroRecordFactory(schemaRegistryActor).build(
      HydraRequest(
        "0",
        jsonString,
        metadata =
          Map(RequestParams.HYDRA_KAFKA_TOPIC_PARAM -> metadataTopicName),
        ackStrategy = AckStrategy.Replicated,
        validationStrategy = ValidationStrategy.Strict
      )
    )
  }

  private[kafka] def createKafkaTopics(
      topicMetadataRequest: TopicMetadataRequest
  ): Future[BootstrapResult] = {
    val timeoutMillis = bootstrapKafkaConfig.getInt("timeout")

    val schema = getGenericSchema(topicMetadataRequest)
    val topicName = schema
      .map(_.subject)
      .getOrElse("Schema does not conform to GenericSchema")

    var topicMap: Map[String, TopicDetails] = Map(topicName -> topicDetails)

    kafkaUtils
      .createTopics(topicMap, timeout = timeoutMillis)
      .map { r => r.all.get(timeoutMillis, TimeUnit.MILLISECONDS) }
      .map { _ => BootstrapSuccess }
      .recover {
        case illegalArg: IllegalArgumentException => {
          log.info(s"Topic $topicName already exists, proceeding anyway...")
          BootstrapSuccess
        }
        case e: Exception => throw e
      }

  }

  private[kafka] def getGenericSchema(
      topicMetadataRequest: TopicMetadataRequest
  ): Option[GenericSchema] = {
    try {
      Some(topicMetadataRequest.schema.convertTo[GenericSchema])
    } catch {
      case _: DeserializationException =>
        None
    }
  }

}

object TopicBootstrapActor {

  def props(
      schemaRegistryActor: ActorRef,
      kafkaIngestor: ActorSelection,
      streamsManagerProps: Props,
      config: Option[Config] = None
  ): Props =
    Props(
      classOf[TopicBootstrapActor],
      schemaRegistryActor,
      kafkaIngestor,
      streamsManagerProps,
      config
    )

  sealed trait TopicBootstrapMessage

  case class InitiateTopicBootstrap(topicMetadataRequest: TopicMetadataRequest)
      extends TopicBootstrapMessage

  case class ForwardBootstrapPayload(request: HydraRequest)
      extends TopicBootstrapMessage

  sealed trait BootstrapResult

  case class BootstrapFailure(reasons: Seq[String]) extends BootstrapResult
  case class BootstrapSuccess(metadata: TopicMetadata) extends BootstrapResult
  case object BootstrapSuccess extends BootstrapResult
  case object BootstrapFailure extends BootstrapResult

  /**
    * Filter by subject is the only supported.
    *
    * @param subject
    */
  case class GetStreams(subject: Option[String]) extends TopicBootstrapMessage

  case class GetStreamsResponse(metadata: Seq[TopicMetadata])
      extends BootstrapResult

  case object Retry

}
