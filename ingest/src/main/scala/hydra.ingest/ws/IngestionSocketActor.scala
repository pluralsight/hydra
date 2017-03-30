package hydra.ingest.ws

/**
  * Created by alexsilva on 3/10/17.
  */

import akka.actor.{Actor, ActorRef}
import akka.http.scaladsl.model.StatusCodes
import akka.util.Timeout
import com.github.vonnagy.service.container.service.ServicesManager
import hydra.common.logging.LoggingAdapter
import hydra.core.ingest.{HydraRequest, HydraRequestMetadata}
import hydra.core.protocol.HydraError
import hydra.core.transport.{AckStrategy, RetryStrategy, ValidationStrategy}
import hydra.ingest.marshallers.IngestionJsonSupport
import hydra.ingest.protocol.IngestionReport
import hydra.ingest.services.IngestionSupervisor
import hydra.ingest.ws.IngestionSocketActor._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class IngestionSocketActor(initialMetadata: Map[String, String]) extends Actor with LoggingAdapter {

  val registry: Future[ActorRef] = ServicesManager.findService("ingestor_registry",
    "hydra-ingest/user/service/")(context.system)

  implicit val ec = context.dispatcher

  private var flowActor: ActorRef = _

  implicit val timeout = 3.seconds

  implicit val akkaTimeout = Timeout(timeout)

  private var session: SocketSession = _

  override def preStart(): Unit = {
    session = new SocketSession().withMetadata(initialMetadata.toSeq: _*)
  }

  override def receive = waitForSocket

  def waitForSocket: Receive = commandReceive orElse {
    case SocketStarted(name, actor) =>
      flowActor = actor
      log.info(s"User $name started a web socket.")
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

    case SocketEnded => context.stop(self)
  }

  def ingesting: Receive = {
    case IncomingMessage(IngestPattern(correlationId, payload)) =>
      val request = session.buildRequest(Option(correlationId), payload)
      registry.map(r => context.actorOf(IngestionSupervisor.props(request, timeout, r)))

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

  private val r = Random.alphanumeric

  lazy val hydraRequestMedatata = metadata.map(m => HydraRequestMetadata(m._1, m._2)).toSeq

  def withMetadata(meta: (String, String)*) =
    copy(metadata = this.metadata ++ meta.map(m => m._1 -> m._2))

  def buildRequest(correlationId: Option[String], payload: String) = {
    import hydra.core.ingest.IngestionParams._
    val rs = metadata.find(_._1.equalsIgnoreCase(HYDRA_RETRY_STRATEGY))
      .map(h => RetryStrategy(h._2)).getOrElse(RetryStrategy.Fail)

    val vs = metadata.find(_._1.equalsIgnoreCase(HYDRA_VALIDATION_STRATEGY))
      .map(h => ValidationStrategy(h._2)).getOrElse(ValidationStrategy.Strict)

    val as = metadata.find(_._1.equalsIgnoreCase(HYDRA_ACK_STRATEGY))
      .map(h => AckStrategy(h._2)).getOrElse(AckStrategy.None)

    HydraRequest(correlationId.getOrElse(r take(8) mkString), payload, hydraRequestMedatata, retryStrategy = rs,
      validationStrategy = vs, ackStrategy = as)
  }
}
