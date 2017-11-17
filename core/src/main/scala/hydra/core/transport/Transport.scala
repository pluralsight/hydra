package hydra.core.transport

import akka.actor.Props
import akka.persistence.{AtLeastOnceDelivery, PersistentActor}
import hydra.core.protocol._
import hydra.core.transport.AckStrategy.{LocalAck, NoAck, TransportAck}
import hydra.core.transport.Transport._

/**
  * Created by alexsilva on 12/1/15.
  */

class Transport(id: String, destProps: Props) extends PersistentActor with AtLeastOnceDelivery {

  override def persistenceId = id

  private val destination = context.actorOf(destProps, id)

  override def receiveCommand: Receive = {
    case p@Produce(_, _, _) => transport(p)

    case Confirm(deliveryId) =>
      if (deliveryId > 0) persistAsync(DestinationConfirmed(deliveryId))(updateState)

    case TransportError(deliveryId) =>
      if (deliveryId > 0) persistAsync(DestinationConfirmed(deliveryId))(updateState) //delete from journal (error)
  }

  override def receiveRecover: Receive = {
    case evt: HydraMessage => updateState(evt)
  }

  protected def updateState(evt: HydraMessage): Unit = evt match {
    case p@Produce(rec, _, _) => deliver(destination.path)(deliveryId => Deliver(rec, deliveryId))
    case DestinationConfirmed(deliveryId) => confirmDelivery(deliveryId)
  }

  private def transport(p: Produce[Any, Any]): Unit = {
    p.ack match {
      case NoAck =>
        sender ! RecordAccepted(p.supervisor)
        destination ! Deliver(p.record)
      case LocalAck =>
        val ingestor = sender
        persistAsync(p)(updateState)
        ingestor ! RecordProduced(HydraRecordMetadata(-1, System.currentTimeMillis), p.supervisor)
      case TransportAck =>
        val ingestor = sender
        val callback: AckCallback = (md, err) => ingestor ! (md.map(RecordProduced(_, p.supervisor))
          .getOrElse(RecordNotProduced(p.record, err.get, p.supervisor)))
        destination ! Deliver(p.record, -1, callback)
    }
  }
}

object Transport {

  type AckCallback = (Option[RecordMetadata], Option[Throwable]) => Unit

  val NoAck: AckCallback = (_, _) => Unit

  trait TransportMessage extends HydraMessage

  case class DestinationConfirmed(deliveryId: Long) extends TransportMessage

  case class Confirm(deliveryId: Long) extends TransportMessage

  case class TransportError(deliveryId: Long) extends TransportMessage

  case class Deliver[K, V](record: HydraRecord[K, V], deliveryId: Long = -1,
                           ackCallback: AckCallback = NoAck) extends TransportMessage


  def props(name: String, destProps: Props): Props = Props(new Transport(name, destProps))

}

