package hydra.core.ingest

import akka.actor.{Actor, OneForOneStrategy, SupervisorStrategy}
import akka.pattern.pipe
import hydra.core.akka.InitializingActor
import hydra.core.monitor.HydraMetrics
import hydra.core.protocol._
import hydra.core.transport.RecordFactory

import scala.concurrent.Future
import scala.util.{Success, Try}

/**
  * Created by alexsilva on 12/1/15.
  */

trait Ingestor extends InitializingActor {

  import Ingestor._

  private implicit val ec = context.dispatcher

  val ingestorName: String = this.getClass.getSimpleName

  def recordFactory: RecordFactory[_, _]

  override val baseReceive: Receive = {
    case Publish(_) =>
      log.info(s"Publish message was not handled by $self.  Will not join.")
      sender ! Ignore

    case Validate(request) =>
      doValidate(request) pipeTo sender

    case RecordProduced(_, sup) =>
      sup ! IngestorCompleted

    case RecordAccepted(sup) =>
      sup ! IngestorCompleted

    case RecordNotProduced(_, error, supervisor) =>
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
      val ackStrategy = request.ackStrategy.toString

      HydraMetrics.incrementGauge(
        lookupKey = ReconciliationGaugeName + s"_$ackStrategy",
        metricName = ReconciliationMetricName,
        tags = Seq("ingestor" -> ingestorName, "ack_level" -> ackStrategy)
      )

      recordFactory.build(r).map(ValidRequest(_))
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
  val ReconciliationGaugeName = "hydra_ingest_kafka_reconciliation"
  val ReconciliationMetricName = "ingest_kafka_reconciliation"
}
