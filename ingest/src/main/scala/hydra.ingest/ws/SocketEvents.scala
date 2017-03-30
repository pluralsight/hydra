package hydra.ingest.ws


import akka.actor.ActorRef
import hydra.ingest.protocol.IngestionReport

/**
  * Created by alexsilva on 3/10/17.
  */

sealed trait SocketEvent

case class SocketStarted(name: String, ref: ActorRef) extends SocketEvent

case object SocketEnded extends SocketEvent

case class IncomingMessage(message: String) extends SocketEvent

sealed trait OutgoingMessage extends SocketEvent

case class SimpleOutgoingMessage(status: Int, message:String) extends OutgoingMessage

case class IngestionOutgoingMessage(report:IngestionReport) extends OutgoingMessage