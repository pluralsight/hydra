package hydra.ingest.ws

/**
  * Created by alexsilva on 3/10/17.
  */

import akka.actor.{Actor, ActorRef}
import akka.http.scaladsl.model.StatusCodes
import akka.util.Timeout
import com.github.vonnagy.service.container.service.ServicesManager
import hydra.common.logging.LoggingAdapter
import hydra.core.ingest.{HydraRequest, HydraRequestMedatata}
import hydra.core.produce.{AckStrategy, RetryStrategy, ValidationStrategy}
import hydra.core.protocol.HydraError
import hydra.ingest.protocol.IngestionReport
import hydra.ingest.services.IngestionSupervisor
import hydra.ingest.ws.IngestionSocketActor._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class IngestionSocketActor extends Actor with LoggingAdapter {

  val registry: Future[ActorRef] = ServicesManager.findService("ingestor_registry",
    "hydra-ingest/user/service/")(context.system)

  implicit val ec = context.dispatcher

  private val randomCorrelationGenerator = Random.alphanumeric

  private var flowActor: ActorRef = _

  implicit val timeout = 3.seconds

  implicit val akkaTimeout = Timeout(timeout)

  private var session = SocketSession()

  override def receive = waitForSocket

  def waitForSocket: Receive = defaultReceive orElse {
    case SocketStarted(name, actor) =>
      flowActor = actor
      log.info(s"User $name started a web socket.")
      context.become(settingParams)
  }

  def settingParams = setReceive orElse defaultReceive

  def setReceive: Receive = {

    case IncomingMessage(SetPattern(null, _)) =>
      flowActor ! OutgoingMessage(200, session.metadata.mkString(";"))

    case IncomingMessage(SetPattern(key, value)) =>
      val theKey = key.toUpperCase.trim
      val theValue = value.trim
      log.debug(s"Setting metadata $theKey to $theValue")
      this.session = session.withMetadata(theKey -> theValue)
      flowActor ! OutgoingMessage(200, s"OK[$theKey=$theValue]")
  }

  def ingesting: Receive = {
    case SocketEnded => context.stop(self)

    case IncomingMessage("-s") =>
      flowActor ! OutgoingMessage(200, "Stand-by mode. [-i for ingest].")
      context.become(settingParams)

    case IncomingMessage(IngestPattern(correlationId, payload)) =>
      val request = session.buildRequest(correlationId, payload)
      registry.map(r => context.actorOf(IngestionSupervisor.props(request, timeout, r)))

    case IncomingMessage(payload) =>
      val request = session.buildRequest(randomCorrelationGenerator.take(8).mkString, payload)
      registry.map(r => context.actorOf(IngestionSupervisor.props(request, timeout, r)))

    case report: IngestionReport =>
      flowActor ! OutgoingMessage(report.statusCode.intValue(), report.statusCode.reason())

    case e: HydraError => flowActor ! OutgoingMessage(StatusCodes.InternalServerError.intValue, e.cause.getMessage)
  }

  def defaultReceive: Receive = {
    case SocketEnded => context.stop(self)

    case IncomingMessage("-i") =>
      flowActor ! OutgoingMessage(200, "Ingest mode. Stream and strict text payloads are supported.")
      context.become(ingesting)

    case IncomingMessage(HelpPattern()) =>
      flowActor ! OutgoingMessage(200, "Set metadata: --set (name)=(value)")

    case IncomingMessage(_) =>
      flowActor ! OutgoingMessage(400, "BAD_REQUEST:Not valid. Use 'HELP' for help.")
  }
}

object IngestionSocketActor {
  private val HelpPattern = "(?i)(?:help)".r
  private val SetPattern = "(?i)(?:set)(?:[ \\t]*)(?:(.*)(?:=)(.*))?".r
  private val IngestPattern = "(?:(?:-c) ([\\w]*))* (.*)".r
}

case class SocketSession(metadata: Map[String, String] = Map.empty) {
  lazy val hydraRequestMedatata = metadata.map(m => HydraRequestMedatata(m._1, m._2)).toSeq

  def withMetadata(meta: (String, String)*) =
    copy(metadata = this.metadata ++ meta.map(m => m._1 -> m._2))

  def buildRequest(correlationId: String, payload: String) = {
    import hydra.core.ingest.IngestionParams._
    val rs = metadata.find(_._1.equalsIgnoreCase(HYDRA_RETRY_STRATEGY))
      .map(h => RetryStrategy(h._2)).getOrElse(RetryStrategy.Fail)

    val vs = metadata.find(_._1.equalsIgnoreCase(HYDRA_VALIDATION_STRATEGY))
      .map(h => ValidationStrategy(h._2)).getOrElse(ValidationStrategy.Strict)

    val as = metadata.find(_._1.equalsIgnoreCase(HYDRA_ACK_STRATEGY))
      .map(h => AckStrategy(h._2)).getOrElse(AckStrategy.None)

    HydraRequest(correlationId, payload, hydraRequestMedatata, retryStrategy = rs,
      validationStrategy = vs, ackStrategy = as)
  }
}
