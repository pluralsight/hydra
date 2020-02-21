package hydra.ingest.services

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, OneForOneStrategy, ReceiveTimeout}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import hydra.core.ingest.{HydraRequest, IngestionReport, RequestParams}
import hydra.ingest.services.IngestorRegistry.{
  FindAll,
  FindByName,
  LookupResult
}

import scala.concurrent.duration.FiniteDuration

trait IngestionHandler {
  this: Actor =>

  def timeout: FiniteDuration

  def request: HydraRequest

  //we require an actorref here for performance reasons
  def registry: ActorRef

  private val targetIngestor =
    request.metadataValue(RequestParams.HYDRA_INGESTOR_PARAM)

  targetIngestor match {
    case Some(ingestor) => registry ! FindByName(ingestor)
    case None           => registry ! FindAll
  }

  override def receive: Receive = {
    case LookupResult(Nil) =>
      val errorCode = targetIngestor
        .map(i =>
          StatusCodes
            .custom(404, s"No ingestor named $i was found in the registry.")
        )
        .getOrElse(StatusCodes.BadRequest)

      complete(errorWith(errorCode))

    case LookupResult(ingestors) =>
      context.actorOf(
        IngestionSupervisor.props(request, self, ingestors, timeout)
      )

    case report: IngestionReport =>
      complete(report)

  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e: Exception =>
        fail(e)
        Stop
    }

  private def errorWith(statusCode: StatusCode) = {
    IngestionReport(request.correlationId, Map.empty, statusCode.intValue())
  }

  def complete(report: IngestionReport)

  def fail(e: Throwable)
}
