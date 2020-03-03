package hydra.core.ingest

import akka.actor.{Actor, OneForOneStrategy, SupervisorStrategy}
import akka.pattern.pipe
import hydra.core.akka.InitializingActor
import hydra.core.monitor.HydraMetrics
import hydra.core.protocol._
import hydra.core.transport.{AckStrategy, HydraRecord, RecordFactory, Transport}
import org.apache.commons.lang3.ClassUtils

import scala.concurrent.Future
import scala.util.{Success, Try}

/**
  * Created by alexsilva on 12/1/15.
  */

trait Ingestor extends InitializingActor {

  import Ingestor._

  def name: String = ClassUtils.getSimpleName(this.getClass)

  private implicit val ec = context.dispatcher

  def recordFactory: RecordFactory[_, _]

  override val baseReceive: Receive = {
    case Publish(_) =>
      log.info(s"Publish message was not handled by $self.  Will not join.")
      sender ! Ignore

    case Validate(request) =>
      doValidate(request) pipeTo sender

    case RecordProduced(md, sup) =>
      decrementGaugeOnReceipt(md.destination, md.ackStrategy.toString)
      sup ! IngestorCompleted

    case RecordAccepted(sup, destination) =>
      decrementGaugeOnReceipt(destination, AckStrategy.NoAck.toString)
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
  def validateRequest(request: HydraRequest): Try[HydraRequest] =
    Success(request)

  def doValidate(request: HydraRequest): Future[MessageValidationResult] = {
    Future
      .fromTry(validateRequest(request))
      .flatMap[MessageValidationResult] { r =>
        recordFactory.build(r).map { r =>
          val destination = r.destination

          val ackStrategy = r.ackStrategy.toString

          HydraMetrics.incrementGauge(
            lookupKey =
              ReconciliationMetricName + s"_${destination}_$ackStrategy",
            metricName = ReconciliationMetricName,
            tags = Seq(
              "ingestor" -> name,
              "destination" -> destination,
              "ackStrategy" -> ackStrategy
            )
          )

          HydraMetrics.incrementCounter(
            lookupKey =
              IngestCounterMetricName + s"_${destination}_$ackStrategy",
            metricName = IngestCounterMetricName,
            tags = Seq(
              "ingestor" -> name,
              "destination" -> destination,
              "ackStrategy" -> ackStrategy
            )
          )

          ValidRequest(r)
        }
      }
      .recover { case e => InvalidRequest(e) }
  }

  override def initializationError(ex: Throwable): Receive = {
    case Publish(req) =>
      sender ! IngestorError(ex)
      context.system.eventStream
        .publish(IngestorUnavailable(thisActorName, ex, req))
    case _ =>
      sender ! IngestorError(ex)
  }

  def ingest(next: Actor.Receive) = compose(next)

  override val supervisorStrategy = OneForOneStrategy() {
    case _ => SupervisorStrategy.Restart
  }

  def decrementGaugeOnReceipt(
      destination: String,
      ackStrategy: String
  ): Future[Unit] = {
    Future {
      HydraMetrics.decrementGauge(
        lookupKey =
          Ingestor.ReconciliationMetricName + s"_${destination}_$ackStrategy",
        metricName = Ingestor.ReconciliationMetricName,
        tags = Seq(
          "ingestor" -> name,
          "destination" -> destination,
          "ackStrategy" -> ackStrategy
        )
      )
    }
  }
}

object Ingestor {

  val ReconciliationMetricName = "hydra_ingest_reconciliation"

  val IngestCounterMetricName = "hydra_ingest_message_counter"
}
