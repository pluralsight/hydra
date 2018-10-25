package hydra.ingest.services

import akka.actor.Status.Failure
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
    pipe(schemaRegistryActor ? RegisterSchemaRequest(schema)) to self
  }

  def initializing: Receive = {
    case RegisterSchemaResponse(_) =>
      context.become(active)
      unstashAll()
    case Failure(ex) =>
      unstashAll()
      context.become(failed(ex))
    case _ => stash()
  }

  def active: Receive = {
    case InitiateTopicBootstrap(topicMetadataRequest) =>
      initiateBootstrap(topicMetadataRequest) pipeTo sender
  }

  def failed(ex: Throwable): Receive = {
    case _ => Future.failed(ex) pipeTo sender
  }

  private[ingest] def initiateBootstrap(topicMetadataRequest: TopicMetadataRequest): Future[BootstrapResult] = {
    val result = TopicNameValidator.validate(topicMetadataRequest.streamName)
    result.map { _ =>
      buildAvroRecord(topicMetadataRequest).flatMap { avroRecord =>
        (kafkaIngestor ? Ingest(avroRecord, avroRecord.ackStrategy)).map {
          case IngestorCompleted => BootstrapSuccess
          case IngestorError(ex) => BootstrapFailure(Seq(ex.getMessage))
          case _ => throw new RuntimeException(
            "Kafka Ingestor is unable to respond to requests. Please try again later.")
        }
      }
    }.recover {
      case e: TopicNameValidatorException => Future(BootstrapFailure(e.reasons))
    }.get
  }

  private[ingest] def buildAvroRecord(topicMetadataRequest: TopicMetadataRequest): Future[AvroRecord] = {
    val jsonString = topicMetadataRequest.toJson.toString
    new AvroRecordFactory(schemaRegistryActor).build(
      HydraRequest(
        "0",
        jsonString,
        metadata = Map(
          RequestParams.HYDRA_KAFKA_TOPIC_PARAM ->
            config.get[String]("hydra-metadata-topic-name").value),
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

  case object ActorInitializing extends BootstrapResult

}
