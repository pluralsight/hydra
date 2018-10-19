package hydra.ingest.services

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.config.Config
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.core.marshallers.{HydraJsonSupport, TopicMetadataRequest}
import hydra.core.protocol.InitiateHttpRequest
import hydra.ingest.services.TopicBootstrapActor._
import spray.json._

import scala.concurrent.duration._
import configs.syntax._

class TopicBootstrapActor(
                         config: Config,
                         schemaRegistryActor: ActorRef,
                         ingestionHandlerGateway: ActorRef,
                         ) extends Actor with HydraJsonSupport with ActorLogging {

  override def receive: Receive = {
    case InitiateTopicBootstrap(hydraRequest, ctx) => {
      initiateBootstrap(hydraRequest, ctx)
    }
  }

  private[ingest] def initiateBootstrap(hydraRequest: HydraRequest, ctx: ImperativeRequestContext): Unit = {
    val mdRequest = hydraRequest.payload.parseJson.convertTo[TopicMetadataRequest]
    val enrichedRequest = enrichRequest(hydraRequest)
    val result: BootstrapResult = validateTopicName(mdRequest)
    result match {
      case BootstrapStepSuccess =>
        ingestionHandlerGateway ! InitiateHttpRequest(enrichedRequest, 100.millis, ctx)
      case BootstrapStepFailure(reasons) =>
        ctx.complete(StatusCodes.BadRequest,
          s"Topic name is invalid for the following reasons: $reasons")
    }
  }

  private[ingest] def validateTopicName(topicMetadataRequest: TopicMetadataRequest): BootstrapResult = {
    val isValidOrErrorReport = TopicNameValidator.validate(topicMetadataRequest.streamName)
    isValidOrErrorReport match {
      case Valid => BootstrapStepSuccess
      case InvalidReport(reasons) =>
        val invalidDisplayString = reasons
          .map(_.reason)
          .map("\t" + _)
          .mkString("\n")
        BootstrapStepFailure(invalidDisplayString)
      case _ => BootstrapStepFailure("Couldn't find match on validateTopicName")
    }
  }

  private[ingest] def enrichRequest(hydraRequest: HydraRequest) = {
    hydraRequest.copy(metadata = Map(RequestParams.HYDRA_KAFKA_TOPIC_PARAM -> config.get[String]("hydra-metadata-topic-name").value))
  }
}


object TopicBootstrapActor {

  def props(config: Config, schemaRegistryActor: ActorRef, ingestionHandlerGateway: ActorRef): Props =
    Props(classOf[TopicBootstrapActor], config, schemaRegistryActor, ingestionHandlerGateway)

  sealed trait TopicBootstrapMessage

  case class InitiateTopicBootstrap(hydraRequest: HydraRequest,
                                    context: ImperativeRequestContext) extends TopicBootstrapMessage

  case class ForwardBootstrapPayload(request: HydraRequest) extends TopicBootstrapMessage


  sealed trait BootstrapResult
  case object BootstrapStepSuccess extends BootstrapResult
  case class BootstrapStepFailure(reasons: String) extends BootstrapResult
}
