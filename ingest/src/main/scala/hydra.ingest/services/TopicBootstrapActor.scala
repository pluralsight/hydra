package hydra.ingest.services

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.config.Config
import hydra.core.akka.SchemaRegistryActor
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.HydraRequest
import hydra.core.marshallers.{HydraJsonSupport, TopicMetadataRequest}
import hydra.ingest.services.TopicBootstrapActor._

//first we make sure topic name is valid
//first we need to try and create the topic
//then we post the schema
class TopicBootstrapActor(
                         config: Config,
                         schemaRegistryActor: SchemaRegistryActor,
                         ingestionHandlerGateway: IngestionHandlerGateway,
                         ) extends Actor with HydraJsonSupport with ActorLogging {

  //actor could have multiple instances, need to refactor this
  var ctx: ImperativeRequestContext = _

  override def receive: Receive = {
    //need to pass ctx forward to IngestionHandlerGateway
    case InitiateTopicBootstrap(topicMetadataRequest, ctx) => {
      this.ctx = ctx
      validateTopicName(topicMetadataRequest)
    }
    case TopicNameValidated => {
      ctx.complete(StatusCodes.OK)
    }
    case TopicNameValidationError(reasons) => {
      ctx.complete(StatusCodes.BadRequest, reasons)
    }
    case ForwardBootstrapPayload => {}
  }

  private[ingest] def validateTopicName(topicMetadataRequest: TopicMetadataRequest): Unit = {
    val isValidOrErrorReport = TopicNameValidator.validate(topicMetadataRequest.streamName)
    isValidOrErrorReport match {
      case Valid => self ! TopicNameValidated
      case InvalidReport(reasons) =>
        val invalidDisplayString = reasons
          .map(_.reason)
          .map("\t" + _)
          .mkString("\n")
        self ! TopicNameValidationError(invalidDisplayString)
    }
  }

}


object TopicBootstrapActor {

  def props(config: Config, schemaRegistryActor: ActorRef, ingestionHandlerGateway: ActorRef): Props = Props(
    classOf[SchemaRegistryActor], config, schemaRegistryActor, ingestionHandlerGateway)

  sealed trait TopicBootstrapMessage
  case object TopicNameValidated extends TopicBootstrapMessage


  case class TopicNameValidationError(reasons: String) extends TopicBootstrapMessage

  case class InitiateTopicBootstrap(topicMetadata: TopicMetadataRequest, context: ImperativeRequestContext) extends TopicBootstrapMessage

  case class ForwardBootstrapPayload(request: HydraRequest) extends TopicBootstrapMessage


}
