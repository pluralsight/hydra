package hydra.core.protocol

import akka.actor.ActorRef
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import hydra.core.http.ImperativeRequestContext
import hydra.core.ingest.HydraRequest
import hydra.core.transport.{AckStrategy, HydraRecord, RecordMetadata}
import org.joda.time.DateTime

import scala.concurrent.duration.FiniteDuration

/**
  * Created by alexsilva on 2/22/17.
  */
trait HydraMessage

trait HydraError extends HydraMessage {
  def cause: Throwable

}

case class HydraApplicationError(cause: Throwable) extends HydraError

case class Validate(req: HydraRequest) extends HydraMessage

trait MessageValidationResult extends HydraMessage

case class Publish(request: HydraRequest) extends HydraMessage

case class Ingest[K, V](record: HydraRecord[K, V], ack: AckStrategy) extends HydraMessage

case object Join extends HydraMessage

case object Ignore extends HydraMessage

trait IngestionError extends HydraError {
  val request: HydraRequest
  val time: DateTime
  val statusCode: Int
  val ingestor: String
}

case class GenericIngestionError(ingestor: String,
                                 cause: Throwable,
                                 request: HydraRequest,
                                 statusCode: Int,
                                 time: DateTime = DateTime.now) extends IngestionError

/**
  * Event emitted into the ActorSystem
  * event stream by the ingestion supervisor when an ingestion times out.
  *
  * @param request
  * @param time
  * @param timeout
  */
case class IngestionTimedOut(request: HydraRequest, time: DateTime,
                             timeout: FiniteDuration, ingestor: String) extends IngestionError {
  val statusCode: Int = 408
  val cause = new IngestionTimedOutException("Ingestion timed out.", timeout)
}

case class IngestionTimedOutException(msg: String, timeout: FiniteDuration)
  extends RuntimeException(msg)


case class InvalidRequestError(ingestor: String,
                               request: HydraRequest,
                               time: DateTime, cause: Throwable) extends IngestionError {
  val statusCode: Int = 400
}


case class IngestorUnavailable(ingestor: String,
                               cause: Throwable,
                               request: HydraRequest,
                               time: DateTime = DateTime.now) extends IngestionError {
  override val statusCode: Int = 503
}

/**
  *
  * @param record
  * @param supervisor
  * @param ack
  * @tparam K
  * @tparam V
  */
case class Produce[K, V](record: HydraRecord[K, V], supervisor: ActorRef, ack: AckStrategy)
  extends HydraMessage

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

case class IngestorError(cause: Throwable) extends IngestorStatus with HydraError {
  override val completed = true
  override val message = Option(cause.getMessage).getOrElse("")
  val statusCode = StatusCodes.ServiceUnavailable
}

case class InvalidRequest(cause: Throwable) extends IngestorStatus with HydraError with MessageValidationResult {
  def this(msg: String) = this(new IllegalArgumentException(msg))

  override val completed = true
  override val message = Option(cause.getMessage).getOrElse("")
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

//Initiate ingestion messages
case class InitiateHttpRequest(request: HydraRequest, timeout: FiniteDuration,
                               ctx: ImperativeRequestContext)

/**
  *
  * @param request
  * @param timeout
  * @param clientId Requests with a clientId are guaranteed to go to the same Transport instance
  * @param requestor
  */
case class InitiateRequest(request: HydraRequest, timeout: FiniteDuration,
                           clientId: Option[String] = None,
                           requestor: Option[ActorRef] = None)


case class MissingMetadataException(name: String, msg: String) extends RuntimeException(msg)


