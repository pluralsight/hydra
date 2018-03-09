package hydra.core.transport

import akka.actor.Props
import akka.persistence.{AtLeastOnceDelivery, PersistentActor}
import hydra.core.protocol._
import hydra.core.transport.AckStrategy.{NoAck, Persisted, Replicated}
import hydra.core.transport.TransportSupervisor1.{Confirm, Deliver, DestinationConfirmed, TransportError}

/**
  * Created by alexsilva on 12/1/15.
  */

class TransportSupervisor1(id: String, destProps: Props) extends PersistentActor
  with AtLeastOnceDelivery {

  override def persistenceId = id

  private val destination = context.actorOf(destProps, id)

  override def receiveCommand: Receive = {
    case p@Produce(_, _, _) => transport(p)

    case d@Deliver(_, _, _) => destination forward d

    case Confirm(deliveryId) =>
      if (deliveryId > 0) persistAsync(DestinationConfirmed(deliveryId))(updateState)

    case TransportError(deliveryId) =>
      if (deliveryId > 0) persistAsync(DestinationConfirmed(deliveryId))(updateState) //delete from journal (error)
  }

  override def receiveRecover: Receive = {
    case evt: HydraMessage => updateState(evt)
  }

  protected def updateState(evt: HydraMessage): Unit = evt match {
    case p@Produce(rec, _, _) => deliver(destination.path)(deliveryId => Deliver(rec, deliveryId,
      new TransportSupervisorCallback(self)))
    case DestinationConfirmed(deliveryId) => confirmDelivery(deliveryId)
  }

  private def transport(p: Produce[Any, Any]): Unit = {
    p.ack match {
      case NoAck =>
        sender ! RecordAccepted(p.supervisor)
        destination ! Deliver(p.record)

      case Persisted =>
        val ingestor = sender
        persistAsync(p) { p =>
          updateState(p)
          ingestor ! RecordProduced(HydraRecordMetadata(System.currentTimeMillis), p.supervisor)
        }

      case Replicated =>
        val ingestor = sender
        destination ! Deliver(p.record, -1, new IngestorCallback[Any, Any](p.record, ingestor, p.supervisor, self))
    }
  }
}


object TransportSupervisor1 {

  trait TransportMessage extends HydraMessage

  case class DestinationConfirmed(deliveryId: Long) extends TransportMessage

  case class Confirm(deliveryId: Long) extends TransportMessage

  case class TransportError(deliveryId: Long) extends TransportMessage

  case class Deliver[K, V](record: HydraRecord[K, V], deliveryId: Long = -1,
                           callback: TransportCallback = NoCallback) extends TransportMessage

  def props(name: String, destProps: Props): Props = Props(new TransportSupervisor1(name, destProps))

}

