package hydra.ingest.services

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import configs.syntax._
import hydra.core.akka.SchemaRegistryActor.{FetchSchemaResponse, RegisterSchemaRequest}
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.core.marshallers.{HydraJsonSupport, TopicMetadataRequest}
import hydra.core.protocol.{Ingest, IngestorCompleted, IngestorError}
import hydra.core.transport.{AckStrategy, ValidationStrategy}
import hydra.ingest.services.TopicBootstrapActor.{BootstrapSuccess, _}
import hydra.kafka.producer.{AvroRecord, AvroRecordFactory}
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._

class TopicBootstrapActor(
                           config: Config,
                           schemaRegistryActor: ActorRef,
                           kafkaIngestor: ActorSelection
                         ) extends Actor with HydraJsonSupport with ActorLogging {

  implicit val ec = context.dispatcher

  implicit val timeout = Timeout(10.seconds)

  override def receive: Receive = initializing

  override def preStart(): Unit = {
    (schemaRegistryActor ? RegisterSchemaRequest("")) foreach {
      case FetchSchemaResponse(_) => context.become(active)
      case Failure(ex) => context.become(failed(ex))
    }
  }

  def initializing: Receive = {
    case _ => sender ! ActorInitializing
  }

  def active: Receive = {
    case InitiateTopicBootstrap(topicMetadataRequest) => initiateBootstrap(topicMetadataRequest)
    case IngestorCompleted => sender ! BootstrapSuccess
    case IngestorError(ex) => sender ! BootstrapFailure(ex.getMessage)
  }

  def failed(ex: Throwable): Receive = {
    case _ =>
      BootstrapFailure(s"TopicBootstrapActor is in an errored state due to ${ex.getCause.getMessage}")
  }

  private[ingest] def initiateBootstrap(topicMetadataRequest: TopicMetadataRequest): Unit = {
    val result: BootstrapResult = validateTopicName(topicMetadataRequest)
    result match {
      case BootstrapSuccess => buildAvroRecord(topicMetadataRequest).foreach {
        case avro: AvroRecord => kafkaIngestor ! Ingest(avro, avro.ackStrategy)
        case _ => sender ! BootstrapFailure("Failed to build avro record for metadata request.")
      }
      case failureMsg: BootstrapFailure => sender ! failureMsg
      case _ =>
    }
  }

  private[ingest] def validateTopicName(topicMetadataRequest: TopicMetadataRequest): BootstrapResult = {
    val isValidOrErrorReport = TopicNameValidator.validate(topicMetadataRequest.streamName)
    isValidOrErrorReport match {
      case Valid => BootstrapSuccess
      case InvalidReport(reasons) =>
        val invalidDisplayString = reasons
          .map(_.reason)
          .map("\t" + _)
          .mkString("\n")
        BootstrapFailure(invalidDisplayString)
      case _ => BootstrapFailure("Couldn't find match on validateTopicName")
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

  case class BootstrapFailure(reasons: String) extends BootstrapResult

  case object ActorInitializing extends BootstrapResult

}
