package hydra.ingest.ws

/**
  * Created by alexsilva on 3/10/17.
  */

import akka.actor.{Actor, ActorRef}
import akka.http.scaladsl.model.StatusCodes
import akka.util.Timeout
import hydra.common.logging.LoggingAdapter
import hydra.core.ingest
import hydra.core.ingest.IngestionReport
import hydra.core.protocol.HydraError
import hydra.core.transport.{AckStrategy, ValidationStrategy}
import hydra.ingest.bootstrap.HydraIngestorRegistry
import hydra.ingest.services.IngestionSupervisor
import hydra.ingest.ws.IngestionSocketActor._

import scala.concurrent.duration._

class IngestionSocketActor extends Actor with LoggingAdapter with HydraIngestorRegistry {

  implicit val system = context.system

  private var flowActor: ActorRef = _

  implicit val timeout = 3.seconds

  implicit val akkaTimeout = Timeout(timeout)

  private var session: SocketSession = _

  override def preStart(): Unit = {
    session = new SocketSession()
  }

  override def receive = waitForSocket

  def waitForSocket: Receive = commandReceive orElse {
    case SocketStarted(actor) =>
      flowActor = actor
      context.become(ingesting orElse commandReceive)
  }

  def commandReceive: Receive = {

    case IncomingMessage(SetPattern(null, _)) =>
      flowActor ! SimpleOutgoingMessage(200, session.metadata.mkString(";"))

    case IncomingMessage(SetPattern(key, value)) =>
      val theKey = key.toUpperCase.trim
      val theValue = value.trim
      log.debug(s"Setting metadata $theKey to $theValue")
      this.session = session.withMetadata(theKey -> theValue)
      flowActor ! SimpleOutgoingMessage(200, s"OK[$theKey=$theValue]")

    case IncomingMessage(HelpPattern()) =>
      flowActor ! SimpleOutgoingMessage(200, "Set metadata: --set (name)=(value)")

    case IncomingMessage(_) =>
      flowActor ! SimpleOutgoingMessage(400, "BAD_REQUEST:Not a valid message. Use 'HELP' for help.")

    case SocketEnded =>
      context.stop(flowActor)
      context.stop(self)
  }

  def ingesting: Receive = {
    case IncomingMessage(IngestPattern(correlationId, payload)) =>
      val request = session.buildRequest(Option(correlationId).map(_.toLong), payload)
      ingestorRegistry.map(r => context.actorOf(IngestionSupervisor.props(request, timeout, r)))(context.dispatcher)

    case report: IngestionReport =>
      flowActor ! IngestionOutgoingMessage(report)

    case e: HydraError => flowActor ! SimpleOutgoingMessage(StatusCodes.InternalServerError.intValue, e.cause.getMessage)
  }
}

object IngestionSocketActor {
  private val HelpPattern = "(?i)(?:\\-c help)".r
  private val SetPattern = "(?i)(?:\\-c set)(?:[ \\t]*)(?:(.*)(?:=)(.*))?".r
  private val IngestPattern = "(?:^(?!-c))(?:(?:-i) ([\\w]*))*(.*)".r
}

case class SocketSession(metadata: Map[String, String] = Map.empty) {

  def withMetadata(meta: (String, String)*) =
    copy(metadata = this.metadata ++ meta.map(m => m._1 -> m._2))

  def buildRequest(correlationId: Option[Long], payload: String) = {
    import hydra.core.ingest.RequestParams._

    val vs = metadata.find(_._1.equalsIgnoreCase(HYDRA_VALIDATION_STRATEGY))
      .map(h => ValidationStrategy(h._2)).getOrElse(ValidationStrategy.Strict)

    val as = metadata.find(_._1.equalsIgnoreCase(HYDRA_ACK_STRATEGY))
      .map(h => AckStrategy(h._2)).getOrElse(AckStrategy.NoAck)

    ingest.HydraRequest(correlationId.getOrElse(0), payload, metadata,
      validationStrategy = vs, ackStrategy = as)
  }
}
