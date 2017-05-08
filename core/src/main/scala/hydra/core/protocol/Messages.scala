package hydra.core.protocol

import akka.actor.ActorRef
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import hydra.core.ingest.HydraRequest
import hydra.core.transport.{HydraRecord, RecordMetadata}

/**
  * Created by alexsilva on 2/22/17.
  */
trait HydraMessage

trait HydraError extends HydraMessage {
  def cause: Throwable

}

case class GenericHydraError(cause: Throwable) extends HydraError

case class HydraIngestionError(handler: String, cause: Throwable, payload: String) extends HydraError

case class Validate(req: HydraRequest) extends HydraMessage

trait MessageValidationResult extends HydraMessage

case class Publish(request: HydraRequest) extends HydraMessage

case class Ingest(request: HydraRequest) extends HydraMessage

case object Join extends HydraMessage

case object Ignore extends HydraMessage

case class InitiateRequest(request: HydraRequest) extends HydraMessage

//These are the Produce-related messages
case class Produce[K, V](record: HydraRecord[K, V]) extends HydraMessage

case class ProduceWithAck[K, V](record: HydraRecord[K, V], ingestor: ActorRef, supervisor: ActorRef) extends HydraMessage

case class RecordProduced(md: RecordMetadata) extends HydraMessage

case class RecordNotProduced[K, V](record: HydraRecord[K, V], error: Throwable) extends HydraMessage

case class ProducerAck(supervisor: ActorRef, error: Option[Throwable])

sealed trait IngestorStatus extends HydraMessage with Product {
  val name: String = productPrefix

  /**
    * Whether this Transport is completed, either by completing the ingestion or error.
    */
  val completed: Boolean = false

  def statusCode: StatusCode

  def message: String = statusCode.reason()
}


case object IngestorJoined extends IngestorStatus {
  val statusCode = StatusCodes.Accepted
}

case object WaitingForAck extends IngestorStatus {
  val statusCode = StatusCodes.Processing
}

case object IngestorIgnored extends IngestorStatus {
  val statusCode = StatusCodes.NotAcceptable
}

case object RequestPublished extends IngestorStatus {
  val statusCode = StatusCodes.Created
}

case class IngestorError(error: Throwable) extends IngestorStatus {
  override val completed = true
  override val message = error.getMessage
  val statusCode = StatusCodes.InternalServerError
}

case class InvalidRequest(error: Throwable) extends IngestorStatus with MessageValidationResult {
  def this(msg: String) = this(new IllegalArgumentException(msg))

  override val completed = true
  override val message = error.getMessage
  val statusCode = StatusCodes.BadRequest
}

case object ValidRequest extends IngestorStatus with MessageValidationResult {
  override val completed = true
  val statusCode = StatusCodes.Continue
}

case object IngestorTimeout extends IngestorStatus {
  override val completed = true
  val statusCode = StatusCodes.RequestTimeout
}

case object IngestorCompleted extends IngestorStatus {
  override val completed = true
  val statusCode = StatusCodes.OK
}



