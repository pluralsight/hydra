package hydra.core.connect

import akka.persistence.AtLeastOnceDelivery.UnconfirmedWarning
import akka.persistence.{AtLeastOnceDelivery, PersistentActor}
import hydra.core.ingest.{HydraRequest, IngestionReport}
import hydra.core.protocol.HydraMessage

/**
  * Created by alexsilva on 12/17/15.
  */
trait PersistentConnector extends Connector with PersistentActor with AtLeastOnceDelivery {

  override def receive = super.receive orElse receiveCommand

  override def persistenceId: String = id

  override def receiveCommand: Receive = {
    case req: HydraRequest =>  persist(RequestReceived(req))(updateState)

    case IngestionReport(id, _, 200) ⇒ persist(RequestConfirmed(id.toLong))(updateState)

    case r: IngestionReport => onIngestionError(r)

    case UnconfirmedWarning(deliveries) =>
      val msg = s"There are ${deliveries.size} unconfirmed messages from connector $id."
      log.error(msg)
      context.system.eventStream.publish(HydraConnectIngestError(id, id, 503, msg))
  }

  val receiveRecover: Receive = {
    case msg: HydraMessage => updateState(msg)
  }

  def updateState(msg: HydraMessage): Unit = msg match {
    case RequestReceived(r) => deliver(publisher.path) { id =>
      publishRequest(r.withCorrelationId(id.toString))
    }
    case RequestConfirmed(id) => confirmDelivery(id)
  }
}