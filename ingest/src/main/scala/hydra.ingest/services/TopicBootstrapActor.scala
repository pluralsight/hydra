package hydra.ingest.services

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import hydra.core.akka.SchemaRegistryActor
import TopicBootstrapActor._
import com.typesafe.config.Config
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.HydraRequest
import hydra.core.marshallers.TopicCreationMetadata


class TopicBootstrapActor(
                         config: Config,
                         schemaRegistryActor: SchemaRegistryActor,
                         ingestionHandlerGateway: IngestionHandlerGateway,
                         ) extends Actor with ActorLogging {

  override def receive: Receive = {
    //need to pass ctx forward to IngestionHandlerGateway
    case InitiateTopicBootstrap(topicMetadata, ctx) => doValidate(topicMetadata)
    case TopicNameValidated => {}
    case TopicNameValidationError => {}
    case ForwardBootstrapPayload => {}
  }

  private[ingest] def doValidate(topicCreationMetadata: TopicCreationMetadata): Unit = {
    val isValidOrErrorReport = TopicNameValidator.validate(topicCreationMetadata.topicName)
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

  case class InitiateTopicBootstrap(topicMetadata: TopicCreationMetadata, context: ImperativeRequestContext) extends TopicBootstrapMessage

  case class ForwardBootstrapPayload(request: HydraRequest) extends TopicBootstrapMessage


}
