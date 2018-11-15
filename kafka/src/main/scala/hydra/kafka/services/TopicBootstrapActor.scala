package hydra.kafka.services

import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.Status.{Failure => AkkaFailure}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Props, Stash, Timers}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.typesafe.config.Config
import configs.syntax._
import hydra.common.config.ConfigSupport
import hydra.core.akka.SchemaRegistryActor.{RegisterSchemaRequest, RegisterSchemaResponse}
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.core.marshallers.{HydraJsonSupport, TopicMetadataRequest}
import hydra.core.protocol.{Ingest, IngestorCompleted, IngestorError}
import hydra.core.transport.{AckStrategy, ValidationStrategy}
import hydra.kafka.producer.{AvroRecord, AvroRecordFactory}
import hydra.kafka.util.KafkaUtils
import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success}

class TopicBootstrapActor(
                           schemaRegistryActor: ActorRef,
                           kafkaIngestor: ActorSelection,
                           bootstrapConfig: Option[Config] = None
                         ) extends Actor
  with HydraJsonSupport
  with ActorLogging
  with ConfigSupport
  with Stash
  with Timers {

  import TopicBootstrapActor._

  implicit val ec = context.dispatcher

  implicit val timeout = Timeout(10.seconds)

  val schema = Source.fromResource("HydraMetadataTopic.avsc").mkString

  override def preStart(): Unit = {
    pipe(registerSchema(schema)) to self
  }

  val kafkaUtils = KafkaUtils()

  val bootstrapKafkaConfig: Config = bootstrapConfig getOrElse
    applicationConfig.getConfig("bootstrap-config")

  val topicDetailsConfig: util.Map[String, String] = Map[String, String]().empty.asJava
  val topicDetails = new TopicDetails(
    bootstrapKafkaConfig.getInt("partitions"),
    bootstrapKafkaConfig.getInt("replication-factor").toShort,
    topicDetailsConfig
  )

  private val failureRetryInterval = bootstrapKafkaConfig
    .get[Int]("failure-retry-millis")
    .value

  override def receive: Receive = initializing

  def initializing: Receive = {
    case RegisterSchemaResponse(_) =>
      context.become(active)
      unstashAll()

    case AkkaFailure(ex) =>
      log.error(s"TopicBootstrapActor entering failed state due to: ${ex.getMessage}")
      unstashAll()
      timers.startSingleTimer("retry-failure", Retry, failureRetryInterval.millis)
      context.become(failed(ex))

    case _ => stash()
  }

  def active: Receive = {
    case InitiateTopicBootstrap(topicMetadataRequest) =>
      TopicNameValidator.validate(topicMetadataRequest.subject) match {
        case Success(_) =>
          val ingestFuture = ingestMetadata(topicMetadataRequest)

          val registerSchemaFuture = registerSchema(topicMetadataRequest.schema.compactPrint)

          val result = for {
            _ <- ingestFuture
            _ <- registerSchemaFuture
            bootstrapResult <- createKafkaTopic(topicMetadataRequest)
          } yield bootstrapResult

          pipe(
            result.recover {
              case t: Throwable => BootstrapFailure(Seq(t.getMessage))
            }) to sender

        case Failure(ex: TopicNameValidatorException) =>
          Future(BootstrapFailure(ex.reasons)) pipeTo sender
      }
  }

  def failed(ex: Throwable): Receive = {
    case Retry =>
      log.info("Retrying metadata schema registration...")
      context.become(initializing)
      pipe(registerSchema(schema)) to self

    case _ =>
      val failureMessage = s"TopicBootstrapActor is in a failed state due to cause: ${ex.getMessage}"
      Future.failed(new Exception(failureMessage)) pipeTo sender
  }

  private[kafka] def registerSchema(schemaJson: String): Future[RegisterSchemaResponse] = {
    (schemaRegistryActor ? RegisterSchemaRequest(schemaJson)).mapTo[RegisterSchemaResponse]
  }

  private[kafka] def ingestMetadata(topicMetadataRequest: TopicMetadataRequest): Future[BootstrapResult] = {
    buildAvroRecord(topicMetadataRequest).flatMap { avroRecord =>
      (kafkaIngestor ? Ingest(avroRecord, avroRecord.ackStrategy)).map {
        case IngestorCompleted => BootstrapSuccess
        case IngestorError(ex) =>
          val errorMessage = ex.getMessage
          log.error(
            s"TopicBootstrapActor received an IngestorError from KafkaIngestor: $errorMessage")
          throw ex
      }
    }.recover {
      case ex: Throwable =>
        val errorMessage = ex.getMessage
        log.error(s"Unexpected error occurred during initiateBootstrap: $errorMessage")
        throw ex
    }
  }

  private[kafka] def buildAvroRecord(topicMetadataRequest: TopicMetadataRequest): Future[AvroRecord] = {
    val enrichedReq = topicMetadataRequest
      .copy(createdDate = Some(org.joda.time.DateTime.now()), id = Some(UUID.randomUUID()))

    val jsonString = enrichedReq.toJson.compactPrint
    new AvroRecordFactory(schemaRegistryActor).build(
      HydraRequest(
        "0",
        jsonString,
        metadata = Map(
          RequestParams.HYDRA_KAFKA_TOPIC_PARAM ->
            bootstrapKafkaConfig.get[String]("metadata-topic-name")
              .valueOrElse("hydra.metadata.topic")),
        ackStrategy = AckStrategy.Replicated,
        validationStrategy = ValidationStrategy.Strict
      )
    )
  }

  private[kafka] def createKafkaTopic(topicMetadataRequest: TopicMetadataRequest): Future[BootstrapResult] = {
    val timeoutMillis = bootstrapKafkaConfig.getInt("timeout")

    val topic = topicMetadataRequest.subject

    val topicExists = kafkaUtils.topicExists(topic) match {
      case Success(value) => value
      case Failure(exception) =>
        log.error(s"Unable to determine if topic exists: ${exception.getMessage}")
        return Future.failed(exception)
    }

    // Don't fail when topic already exists
    if (topicExists) {
      log.info(s"Topic $topic already exists, proceeding anyway...")
      Future.successful(BootstrapSuccess)}
    else {
      kafkaUtils.createTopic(topicMetadataRequest.subject, topicDetails, timeout = timeoutMillis)
        .map { r =>
          r.all.get(timeoutMillis, TimeUnit.MILLISECONDS)
        }
        .map { _ =>
          BootstrapSuccess
        }
        .recover {
          case e: Exception => BootstrapFailure(e.getMessage :: Nil)
        }
    }
  }
}

object TopicBootstrapActor {

  def props(schemaRegistryActor: ActorRef, kafkaIngestor: ActorSelection,
            config: Option[Config] = None): Props =
    Props(classOf[TopicBootstrapActor], schemaRegistryActor, kafkaIngestor, config)

  sealed trait TopicBootstrapMessage

  case class InitiateTopicBootstrap(topicMetadataRequest: TopicMetadataRequest) extends TopicBootstrapMessage

  case class ForwardBootstrapPayload(request: HydraRequest) extends TopicBootstrapMessage

  sealed trait BootstrapResult

  case object BootstrapSuccess extends BootstrapResult

  case class BootstrapFailure(reasons: Seq[String]) extends BootstrapResult

  case class BootstrapStep[A](stepResult: A) extends BootstrapResult

  case object Retry

}
