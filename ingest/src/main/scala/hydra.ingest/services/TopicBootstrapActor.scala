package hydra.ingest.services

import akka.actor.Status.{Failure => AkkaFailure}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Props, Stash}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.typesafe.config.Config
import configs.syntax._
import hydra.core.akka.SchemaRegistryActor.{RegisterSchemaRequest, RegisterSchemaResponse}
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.core.marshallers.{HydraJsonSupport, TopicMetadataRequest}
import hydra.core.protocol.{Ingest, IngestorCompleted, IngestorError}
import hydra.core.transport.{AckStrategy, ValidationStrategy}
import hydra.ingest.services.TopicBootstrapActor.{BootstrapSuccess, _}
import hydra.kafka.producer.{AvroRecord, AvroRecordFactory}
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success}

class TopicBootstrapActor(config: Config,
                          schemaRegistryActor: ActorRef,
                          kafkaIngestor: ActorSelection) extends Actor
  with HydraJsonSupport
  with ActorLogging
  with Stash {

  implicit val ec = context.dispatcher

  implicit val timeout = Timeout(10.seconds)

  override def receive: Receive = initializing

  override def preStart(): Unit = {
    val schema = Source.fromResource("HydraMetadataTopic.avsc").mkString
    pipe(registerSchema(schema)) to self
  }

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
      TopicNameValidator.validate(topicMetadataRequest.streamName) match {
        case Success(_) =>
          val ingestFuture = ingestMetadata(topicMetadataRequest)

          val registerSchemaFuture = registerSchema(topicMetadataRequest.streamSchema.compactPrint)

          val result = for {
            bootstrapResult <- ingestFuture
            _ <- registerSchemaFuture
          } yield bootstrapResult

          pipe(result.recover {
            case t: Throwable => BootstrapFailure(Seq(t.getMessage))}
          ) to sender

        case Failure(ex: TopicNameValidatorException) =>
          Future(BootstrapFailure(ex.reasons)) pipeTo sender
      }
  }

  def failed(ex: Throwable): Receive = {
    case _ =>
      val failureMessage = s"TopicBootstrapActor is in a failed state due to cause: ${ex.getMessage}"
      Future.failed(new Exception(failureMessage)) pipeTo sender
  }

  private[ingest] def registerSchema(schemaJson: String): Future[RegisterSchemaResponse] = {
    (schemaRegistryActor ? RegisterSchemaRequest(schemaJson)).mapTo[RegisterSchemaResponse]
  }

  private[ingest] def ingestMetadata(topicMetadataRequest: TopicMetadataRequest): Future[BootstrapResult] = {
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

  private[ingest] def buildAvroRecord(topicMetadataRequest: TopicMetadataRequest): Future[AvroRecord] = {
    val jsonString = topicMetadataRequest.toJson.toString
    new AvroRecordFactory(schemaRegistryActor).build(
      HydraRequest(
        "0",
        jsonString,
        metadata = Map(
          RequestParams.HYDRA_KAFKA_TOPIC_PARAM ->
            config.get[String]("hydra-metadata-topic-name")
              .valueOrElse("hydra.metadata.topic")),
        ackStrategy = AckStrategy.Replicated,
        validationStrategy = ValidationStrategy.Strict
      )
    )
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
