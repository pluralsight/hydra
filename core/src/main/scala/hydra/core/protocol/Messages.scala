package hydra.core.protocol

import akka.actor.ActorRef
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import hydra.core.ingest.HydraRequest
import hydra.core.transport.{AckStrategy, HydraRecord, RecordMetadata}
import org.joda.time.DateTime

/**
  * Created by alexsilva on 2/22/17.
  */
trait HydraMessage

trait HydraError extends HydraMessage {
  def error: Throwable

}

case class IngestionError(error: Throwable) extends HydraError

case class Validate(req: HydraRequest) extends HydraMessage

trait MessageValidationResult extends HydraMessage

case class Publish(request: HydraRequest) extends HydraMessage

case class Ingest[K, V](record: HydraRecord[K, V], ack: AckStrategy) extends HydraMessage

case object Join extends HydraMessage

case object Ignore extends HydraMessage

/**
  *
  * @param record
  * @param supervisor
  * @param ack
  * @tparam K
  * @tparam V
  */
case class Produce[K, V](record: HydraRecord[K, V], supervisor: ActorRef, ack: AckStrategy) extends HydraMessage

/**
  * Signals the record was accepted by a transport for production, but it hasn't been necessarily saved yet.
  * Used exclusively for NoAck replication pipelines.
  *
  * @param supervisor
  */
case class RecordAccepted(supervisor: ActorRef) extends HydraMessage

/**
  * Signals that a record was successfully produced by Hydra.
  *
  * The semantics of this varies according to the Ack tye and the underlying transport.
  *
  * For all local acks, this message is sent as soon as the message is saved into the journal.
  *
  * For transport acks this depends on the underlying acking mechanism. For Kafka, for instance,
  * this message is sent when the broker acknowledge the receipt (and potential replication) of the record.
  *
  * @param md         The record metadata specific to the record produced.
  * @param supervisor The ingestor should send this message to its supervisor if present.
  *                   This reference is here because most times the record is produced asynchronously.
  */
case class RecordProduced(md: RecordMetadata, supervisor: ActorRef) extends HydraMessage

case class RecordNotProduced[K, V](record: HydraRecord[K, V], error: Throwable,
                                   supervisor: ActorRef) extends HydraMessage

//todo:rename this class
case class HydraIngestionError(ingestor: String, error: Throwable,
                               request: HydraRequest, time: DateTime = DateTime.now) extends HydraError


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

case object IngestorIgnored extends IngestorStatus {
  val statusCode = StatusCodes.NotAcceptable
}

case object RequestPublished extends IngestorStatus {
  val statusCode = StatusCodes.Created
}

case class IngestorError(error: Throwable) extends IngestorStatus with HydraError {
  override val completed = true
  override val message = Option(error.getMessage).getOrElse("")
  val statusCode = StatusCodes.ServiceUnavailable
}

case class InvalidRequest(error: Throwable) extends IngestorStatus with HydraError with MessageValidationResult {
  def this(msg: String) = this(new IllegalArgumentException(msg))

  override val completed = true
  override val message = Option(error.getMessage).getOrElse("")
  val statusCode = StatusCodes.BadRequest
}

case class ValidRequest[K, V](record: HydraRecord[K, V]) extends IngestorStatus with MessageValidationResult {
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



