package hydra.ingest.services

/**
  * Created by alexsilva on 3/10/17.
  */
import akka.actor.Status.Failure
import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.util.Timeout
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.ingest.{HydraRequest, IngestionReport, RequestParams}
import hydra.core.protocol.HydraError
import hydra.core.transport.{AckStrategy, ValidationStrategy}
import hydra.ingest.bootstrap.HydraIngestorRegistryClient
import hydra.ingest.services.IngestionSocketActor._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Success, Try}

class IngestionSocketActor
    extends Actor
    with LoggingAdapter
    with ConfigSupport {

  private implicit val system: ActorSystem = context.system
  private implicit val akkaTimeout: Timeout = Timeout(3.seconds)
  private implicit val ec: ExecutionContext = context.dispatcher

  private lazy val registry = context
    .actorSelection(HydraIngestorRegistryClient.registryPath(applicationConfig))
    .resolveOne()

  override def receive: Receive = waitForSocket orElse ackInit

  private val waitForSocket: Receive = {
    case SocketStarted(actor) =>
      context.become(ingestOrReceiveCommand(actor, SocketSession()))
  }

  private val ackInit: Receive = {
    case SocketInit => sender ! SocketAck
  }

  private def ingestOrReceiveCommand(
      flowActor: ActorRef,
      session: SocketSession
  ): Receive =
    ingesting(flowActor, session) orElse commandReceive(flowActor, session) orElse ackInit

  private def commandReceive(
      flowActor: ActorRef,
      session: SocketSession
  ): Receive = {

    case IncomingMessage(SetPattern(null, _)) =>
      flowActor ! SimpleOutgoingMessage(200, session.metadata.mkString(";"))
      sender ! SocketAck

    case IncomingMessage(SetPattern(key, value)) =>
      val theKey = key.toUpperCase.trim
      val theValue = value.trim
      log.debug(s"Setting metadata $theKey to $theValue")
      if (theKey.equalsIgnoreCase(RequestParams.HYDRA_ACK_STRATEGY)) { //this is a special case
        val response = setAckStrategy(theValue, flowActor, session)
        flowActor ! SimpleOutgoingMessage(response._1, response._2)
      } else {
        context.become(
          ingestOrReceiveCommand(
            flowActor,
            session.withMetadata(theKey -> theValue)
          )
        )
        flowActor ! SimpleOutgoingMessage(200, s"OK[$theKey=$theValue]")
      }
      sender ! SocketAck

    case IncomingMessage(HelpPattern()) =>
      flowActor ! SimpleOutgoingMessage(
        200,
        "Set metadata: --set (name)=(value)"
      )
      sender ! SocketAck

    case IncomingMessage(_) =>
      flowActor ! SimpleOutgoingMessage(
        400,
        "BAD_REQUEST:Not a valid message. Use 'HELP' for help."
      )
      sender ! SocketAck

    case SocketEnded =>
      context.stop(flowActor)
      context.stop(self)
  }

  private def setAckStrategy(
      strategy: String,
      flowActor: ActorRef,
      session: SocketSession
  ): (Int, String) = {
    val key = RequestParams.HYDRA_ACK_STRATEGY
    AckStrategy(strategy)
      .map { ack =>
        context.become(
          ingestOrReceiveCommand(
            flowActor,
            session.withMetadata(key -> ack.toString)
          )
        )
        200 -> s"OK[$key=$strategy]"
      }
      .recover {
        case e: Exception =>
          400 -> s"BAD REQUEST[$key=$strategy] is not a valid ack strategy."
      }
      .get
  }

  def ingesting(flowActor: ActorRef, session: SocketSession): Receive = {
    case IncomingMessage(IngestPattern(correlationId, payload)) =>
      registry.foreach { r =>
        val request = session.buildRequest(Option(correlationId), payload)
        request match {
          case Success(req) =>
            context.actorOf(DefaultIngestionHandler.props(req, r, self))
          case scala.util.Failure(ex) => sender ! Failure(ex)
        }
      }
      sender ! SocketAck
    case report: IngestionReport =>
      flowActor ! IngestionOutgoingMessage(report)
      sender ! SocketAck
    case e: HydraError =>
      flowActor ! SimpleOutgoingMessage(
        StatusCodes.InternalServerError.intValue,
        e.cause.getMessage
      )
      sender ! SocketAck
  }

}

object IngestionSocketActor {
  private val HelpPattern = "(?i)(?:\\-c help)".r
  private val SetPattern = "(?i)(?:\\-c set)(?:[ \\t]*)(?:(.*)(?:=)(.*))?".r
  private val IngestPattern = "(?:^(?!-c))(?:(?:-i) ([\\w]*))*(.*)".r
}

case class SocketSession(metadata: Map[String, String] = Map.empty) {

  def withMetadata(meta: (String, String)*): SocketSession =
    copy(metadata = this.metadata ++ meta.map(m => m._1 -> m._2))

  def buildRequest(
      correlationId: Option[String],
      payload: String
  ): Try[HydraRequest] = {
    import hydra.core.ingest.RequestParams._

    val vs = metadata
      .find(_._1.equalsIgnoreCase(HYDRA_VALIDATION_STRATEGY))
      .map(h => ValidationStrategy(h._2))
      .getOrElse(ValidationStrategy.Strict)

    val as = metadata
      .find(_._1.equalsIgnoreCase(HYDRA_ACK_STRATEGY))
      .map(h => AckStrategy(h._2))
      .getOrElse(Success(AckStrategy.NoAck))

    lazy val clientId = metadata
      .find(_._1.toLowerCase() == HydraClientId)
      .map(_._2.toLowerCase)

    as.map(ack =>
      HydraRequest(
        correlationId.getOrElse("0"),
        payload,
        clientId,
        metadata,
        vs,
        ack
      )
    )
  }
}
