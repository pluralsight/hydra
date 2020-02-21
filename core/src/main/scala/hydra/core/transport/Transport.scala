package hydra.core.transport

import akka.persistence.{AtLeastOnceDelivery, PersistentActor}
import hydra.common.config.ConfigSupport
import hydra.core.protocol.{
  HydraMessage,
  Produce,
  RecordAccepted,
  RecordProduced
}
import hydra.core.transport.AckStrategy.{NoAck, Persisted, Replicated}

trait Transport
    extends PersistentActor
    with ConfigSupport
    with AtLeastOnceDelivery {

  import Transport._

  override val persistenceId = getClass.getSimpleName

  def transport: Receive

  private final def baseCommand: Receive = {
    case p @ Produce(_, _, _) => deliver(p)

    case Confirm(deliveryId) =>
      if (deliveryId > 0)
        persistAsync(DestinationConfirmed(deliveryId))(updateState)

    case t @ TransportError(_) =>
      context.system.eventStream.publish(t)
  }

  final override def receiveCommand = baseCommand orElse transport

  final override def receiveRecover: Receive = {
    case evt: HydraMessage => updateState(evt)
  }

  protected def updateState(evt: HydraMessage): Unit = evt match {
    case Produce(rec, _, _) =>
      deliver(self.path)(deliveryId =>
        Deliver(rec, deliveryId, new TransportSupervisorCallback(self))
      )

    case DestinationConfirmed(deliveryId) =>
      confirmDelivery(deliveryId)
  }

  private def deliver(p: Produce[Any, Any]): Unit = {
    p.ack match {
      case NoAck =>
        sender ! RecordAccepted(p.supervisor, p.record.destination)
        self ! Deliver(p.record)

      case Persisted =>
        val destination = p.record.destination

        val ackStrategy = p.ack

        val ingestor = sender
        persistAsync(p) { p =>
          updateState(p)
          ingestor ! RecordProduced(
            HydraRecordMetadata(
              System.currentTimeMillis,
              destination,
              ackStrategy
            ),
            p.supervisor
          )
        }

      case Replicated =>
        val ingestor = sender
        self ! Deliver(
          p.record,
          -1,
          new IngestorCallback[Any, Any](p.record, ingestor, p.supervisor, self)
        )
    }
  }
}

object Transport {

  trait TransportMessage extends HydraMessage

  case class DestinationConfirmed(deliveryId: Long) extends TransportMessage

  case class Confirm(deliveryId: Long) extends TransportMessage

  case class TransportError(deliveryId: Long) extends TransportMessage

  case class Deliver[K, V](
      record: HydraRecord[K, V],
      deliveryId: Long = -1,
      callback: TransportCallback = NoCallback
  ) extends TransportMessage

}
