package hydra.ingest.services

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import com.typesafe.config.Config
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.HydraRequest
import hydra.core.marshallers.{HydraJsonSupport, TopicMetadataRequest}
import hydra.ingest.services.TopicBootstrapActor._

//first we make sure topic name is valid
//first we need to try and create the topic
//then we post the schema
class TopicBootstrapActor(
                         config: Config,
                         schemaRegistryActor: ActorRef,
                         ingestionHandlerGateway: ActorRef,
                         ) extends Actor with HydraJsonSupport with ActorLogging {


  override def receive: Receive = {
    //need to pass ctx forward to IngestionHandlerGateway
    case InitiateTopicBootstrap(topicMetadataRequest, ctx) => {
      initiateBootstrap(topicMetadataRequest, ctx)
    }
    case ForwardBootstrapPayload => {}
  }

  private[ingest] def initiateBootstrap(topicMetadataReqest: TopicMetadataRequest, ctx: ImperativeRequestContext): Unit = {
    val result: BootstrapResult = validateTopicName(topicMetadataReqest)
    result match {
      case BootstrapStepSuccess => ctx.complete(HttpResponse(StatusCodes.OK))
      case BootstrapStepFailure(reasons) => ctx.complete(StatusCodes.BadRequest, s"Topic name is invalid for the following reasons: $reasons")
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
}


object TopicBootstrapActor {

  def props(config: Config, schemaRegistryActor: ActorRef, ingestionHandlerGateway: ActorRef): Props = Props(classOf[TopicBootstrapActor], config, schemaRegistryActor, ingestionHandlerGateway)

  sealed trait TopicBootstrapMessage

  case class InitiateTopicBootstrap(topicMetadata: TopicMetadataRequest, context: ImperativeRequestContext) extends TopicBootstrapMessage

  case class ForwardBootstrapPayload(request: HydraRequest) extends TopicBootstrapMessage


  sealed trait BootstrapResult
  case object BootstrapStepSuccess extends BootstrapResult
  case class BootstrapStepFailure(reasons: String) extends BootstrapResult


}
