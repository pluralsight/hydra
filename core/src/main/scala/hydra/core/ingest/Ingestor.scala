package hydra.core.ingest

import akka.actor.{Actor, OneForOneStrategy, SupervisorStrategy}
import akka.pattern.pipe
import hydra.core.akka.InitializingActor
import hydra.core.monitor.HydraMetrics
import hydra.core.protocol._
import hydra.core.transport.{RecordFactory, Transport}
import org.apache.commons.lang3.ClassUtils

import scala.concurrent.Future
import scala.util.{Success, Try}

/**
  * Created by alexsilva on 12/1/15.
  */

trait Ingestor extends InitializingActor {

  import Ingestor._

  private implicit val ec = context.dispatcher

  def recordFactory: RecordFactory[_, _]

  override val baseReceive: Receive = {
    case Publish(_) =>
      log.info(s"Publish message was not handled by $self.  Will not join.")
      sender ! Ignore

    case Validate(request) =>
      doValidate(request) pipeTo sender

    case RecordProduced(md, sup) =>
      Transport.decrementTransportGauge(md.record)
      sup ! IngestorCompleted

    case RecordAccepted(sup) =>
      sup ! IngestorCompleted

    case RecordNotProduced(rec, error, supervisor) =>
      supervisor ! IngestorError(error)
  }

  /**
    * To be overriden by ingestors needing extra validation
    *
    * @param request
    * @return
    */
  def validateRequest(request: HydraRequest): Try[HydraRequest] = Success(request)

  final def doValidate(request: HydraRequest): Future[MessageValidationResult] = {
    Future.fromTry(validateRequest(request))
      .flatMap[MessageValidationResult] { r =>
      recordFactory.build(r).map { r =>

        val destination = r.destination

        val ackStrategy = r.ackStrategy.toString

        val recordType = ClassUtils.getSimpleName(r.getClass)

        HydraMetrics.incrementGauge(
          lookupKey = ReconciliationGaugeName + s"_${destination}_$ackStrategy",
          metricName = ReconciliationMetricName,
          tags = Seq("recordType" -> recordType, "destination" -> destination,
            "ackStrategy" -> ackStrategy)
        )

        ValidRequest(r)
      }
    }.recover { case e => InvalidRequest(e) }
  }

  override def initializationError(ex: Throwable): Receive = {
    case Publish(req) =>
      sender ! IngestorError(ex)
      context.system.eventStream.publish(IngestorUnavailable(thisActorName, ex, req))
    case _ =>
      sender ! IngestorError(ex)
  }

  def ingest(next: Actor.Receive) = compose(next)

  override val supervisorStrategy = OneForOneStrategy() { case _ => SupervisorStrategy.Restart }
}

object Ingestor {

  val ReconciliationGaugeName = "hydra_ingest_reconciliation"

  val ReconciliationMetricName = "ingest_reconciliation"
}
