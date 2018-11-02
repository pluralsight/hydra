package hydra.kafka.services

import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.Status.{Failure => AkkaFailure}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Props, Stash}
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
                           config: Config,
                           schemaRegistryActor: ActorRef,
                           kafkaIngestor: ActorSelection
                         ) extends Actor
  with HydraJsonSupport
  with ActorLogging
  with Stash {

  import TopicBootstrapActor._

  implicit val ec = context.dispatcher

  implicit val timeout = Timeout(10.seconds)

  override def receive: Receive = initializing

  override def preStart(): Unit = {
    val schema = Source.fromResource("HydraMetadataTopic.avsc").mkString
    pipe(registerSchema(schema)) to self
  }

  val kafkaUtils = KafkaUtils()

  val bootstrapKafkaConfig: Config = config.getConfig("bootstrap-config")
  val topicDetailsConfig: util.Map[String, String] = Map[String, String]().empty.asJava
  val topicDetails = new TopicDetails(
    bootstrapKafkaConfig.getInt("partitions"),
    bootstrapKafkaConfig.getInt("replication-factor").toShort,
    topicDetailsConfig
  )

  def initializing: Receive = {
    case RegisterSchemaResponse(_) =>
      context.become(active)
      unstashAll()

    case AkkaFailure(ex) =>
      log.error(s"TopicBootstrapActor entering failed state due to: ${ex.getMessage}")
      unstashAll()
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
            config.get[String]("metadata-topic-name")
              .valueOrElse("hydra.metadata.topic")),
        ackStrategy = AckStrategy.Replicated,
        validationStrategy = ValidationStrategy.Strict
      )
    )
  }

  private[kafka] def createKafkaTopic(topicMetadataRequest: TopicMetadataRequest): Future[BootstrapResult] = {
    val timeoutMillis = 3000

    kafkaUtils.createTopic(topicMetadataRequest.subject, topicDetails, timeout = timeoutMillis).map { r =>
      r.all.get(timeoutMillis, TimeUnit.MILLISECONDS)
    }.map { _ =>
      BootstrapSuccess
    } recover {
      case e: Exception => BootstrapFailure(e.getMessage :: Nil)
    }
  }
}

object TopicBootstrapActor {

  def props(config: Config, schemaRegistryActor: ActorRef, kafkaIngestor: ActorSelection): Props =
    Props(classOf[TopicBootstrapActor], config, schemaRegistryActor, kafkaIngestor)

  sealed trait TopicBootstrapMessage

  case class InitiateTopicBootstrap(topicMetadataRequest: TopicMetadataRequest) extends TopicBootstrapMessage

  case class ForwardBootstrapPayload(request: HydraRequest) extends TopicBootstrapMessage

  sealed trait BootstrapResult

  case object BootstrapSuccess extends BootstrapResult

  case class BootstrapFailure(reasons: Seq[String]) extends BootstrapResult

  case class BootstrapStep[A](stepResult: A) extends BootstrapResult

}
