package hydra.ingest.services

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import configs.syntax._
import hydra.avro.resource.SchemaResource
import hydra.core.akka.SchemaRegistryActor.{FetchSchemaResponse, RegisterSchemaRequest}
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.core.marshallers.{HydraJsonSupport, TopicMetadataRequest}
import hydra.core.protocol.InitiateHttpRequest
import hydra.ingest.services.TopicBootstrapActor._

import scala.concurrent.duration._

class TopicBootstrapActor(
                         config: Config,
                         schemaRegistryActor: ActorRef,
                         ingestionHandlerGateway: ActorRef,
                         ) extends Actor with HydraJsonSupport with ActorLogging {

  implicit val ec = context.dispatcher

  implicit val timeout = Timeout(10.seconds)

  override def receive: Receive = initializing

  override def preStart(): Unit = {
    (schemaRegistryActor ? RegisterSchemaRequest()) foreach {
      case FetchSchemaResponse(schema) => context.become(active(schema))
      case Failure(ex) => context.become(failed)
    }
  }

  def initializing: Receive = {
    case _ => sender ! ActorInitializing
  }

  def active(schema: SchemaResource): Receive = {
    case InitiateTopicBootstrap(topicMetadataRequest) =>
  }

  def failed(ex: Throwable): Receive = {
    case _ =>
      BootstrapFailure(s"TopicBootstrapActor is in an errored state due to ${ex.getCause.getMessage}")
  }

  private[ingest] def initiateBootstrap(topicMetadataRequest: TopicMetadataRequest): Unit = {
    val enrichedRequest = enrichRequest(topicMetadataRequest)
    val result: BootstrapResult = validateTopicName(topicMetadataRequest)
    result match {
      case BootstrapSuccess =>
        ingestionHandlerGateway ! InitiateHttpRequest(enrichedRequest, 100.millis, ctx)
      case BootstrapFailure(reasons) =>
        ctx.complete(StatusCodes.BadRequest,
          s"Topic name is invalid for the following reasons: $reasons")
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

  private[ingest] def buildHydraRequest(topicMetadataRequest: TopicMetadataRequest): HydraRequest = {
    //convert topicMetadataRequest back to payload string?
    //set ack level, validation, and kafka topic here

  }

  private[ingest] def enrichRequest(hydraRequest: HydraRequest) = {
    hydraRequest.copy(metadata = Map(RequestParams.HYDRA_KAFKA_TOPIC_PARAM -> config.get[String]("hydra-metadata-topic-name").value))
  }
}


object TopicBootstrapActor {

  def props(config: Config, schemaRegistryActor: ActorRef, ingestionHandlerGateway: ActorRef): Props =
    Props(classOf[TopicBootstrapActor], config, schemaRegistryActor, ingestionHandlerGateway)

  sealed trait TopicBootstrapMessage

  case class InitiateTopicBootstrap(topicMetadataRequest: TopicMetadataRequest) extends TopicBootstrapMessage

  case class ForwardBootstrapPayload(request: HydraRequest) extends TopicBootstrapMessage

  sealed trait BootstrapResult

  case object BootstrapSuccess extends BootstrapResult

  case class BootstrapFailure(reasons: String) extends BootstrapResult

  case object ActorInitializing extends BootstrapResult
}
