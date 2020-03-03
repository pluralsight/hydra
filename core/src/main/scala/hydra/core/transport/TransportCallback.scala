package hydra.core.transport

import akka.actor.ActorRef
import hydra.core.protocol.{RecordNotProduced, RecordProduced}
import hydra.core.transport.Transport.{Confirm, TransportError}

/**
  * A callback interface that the user can implement to allow code to execute when the transport is complete.
  *
  * This callback will generally execute in a separate Akka thread so it should be fast.
  */
trait TransportCallback {

  /**
    * Will be called when the transport has finished producing the record.
    *
    * Exactly one of the optional arguments will be a `Some`.
    */
  def onCompletion(
      deliveryId: Long,
      md: Option[RecordMetadata],
      exception: Option[Throwable]
  ): Unit
}

class IngestorCallback[K, V](
    record: HydraRecord[K, V],
    ingestor: ActorRef,
    supervisor: ActorRef,
    transport: ActorRef
) extends TransportCallback {

  override def onCompletion(
      deliveryId: Long,
      md: Option[RecordMetadata],
      exception: Option[Throwable]
  ): Unit = {
    md match {
      case Some(recordMetadata) => onSuccess(deliveryId, recordMetadata)
      case None                 => onError(deliveryId, exception.get)
    }
  }

  private def onSuccess(deliveryId: Long, md: RecordMetadata) = {
    ingestor ! RecordProduced(md, supervisor)
    transport ! Confirm(deliveryId)
  }

  private def onError(deliveryId: Long, err: Throwable) = {
    ingestor ! RecordNotProduced(record, err, supervisor)
    transport ! TransportError(deliveryId)
  }
}

object NoCallback extends TransportCallback {

  override def onCompletion(
      deliveryId: Long,
      md: Option[RecordMetadata],
      exception: Option[Throwable]
  ): Unit = {}
}

/**
  * Does not ack ingestor/supervisor; only sends the record production messages to the transport supervisor.
  */
class TransportSupervisorCallback(transport: ActorRef)
    extends TransportCallback {

  override def onCompletion(
      deliveryId: Long,
      md: Option[RecordMetadata],
      exception: Option[Throwable]
  ): Unit = {
    md match {
      case Some(_) => transport ! Confirm(deliveryId)
      case None    => transport ! TransportError(deliveryId)
    }
  }
}
