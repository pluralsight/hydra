package hydra.core.ingest

import akka.actor.{Actor, OneForOneStrategy, SupervisorStrategy}
import hydra.core.akka.InitializingActor
import hydra.core.protocol._
import hydra.core.transport.RecordFactory
import akka.pattern.pipe

import scala.concurrent.Future

/**
  * Created by alexsilva on 12/1/15.
  */

trait Ingestor extends InitializingActor {

  private implicit val ec = context.dispatcher

  def recordFactory: RecordFactory[_, _]

  override val baseReceive: Receive = {
    case Publish(_) =>
      log.info(s"Publish message was not handled by ${self}.  Will not join.")
      sender ! Ignore

    case Validate(request) =>
      val supervisor = sender
      pipe(validate(request)) to supervisor

    case RecordProduced(_, sup) =>
      sup ! IngestorCompleted

    case RecordAccepted(sup) =>
      sup ! IngestorCompleted

    case RecordNotProduced(_, error, supervisor) =>
      supervisor ! IngestorError(error)
  }

  def validate(request: HydraRequest): Future[MessageValidationResult] = {
    recordFactory.build(request).map(ValidRequest(_))
      .recover { case e => InvalidRequest(e) }
  }

  override def initializationError(ex: Throwable): Receive = {
    case Publish(req) =>
      sender ! IngestorError(ex)
      ingestionError(HydraIngestionError(thisActorName, ex, req))
    case _ =>
      sender ! IngestorError(ex)
  }

  def ingest(next: Actor.Receive) = compose(next)

  def ingestionError(error: HydraIngestionError): Unit = context.system.eventStream.publish(error)

  override val supervisorStrategy = OneForOneStrategy() { case _ => SupervisorStrategy.Restart }
}
