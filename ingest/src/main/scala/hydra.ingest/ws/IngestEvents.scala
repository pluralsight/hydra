package hydra.ingest.ws


import akka.actor.ActorRef

/**
  * Created by alexsilva on 3/10/17.
  */

case class SocketMessage(sender: String, text: String)

object SystemMessage {
  def apply(text: String) = SocketMessage("System", text)
}


sealed trait ChatEvent

case class SocketStarted(name: String) extends ChatEvent

case class SocketEnded(name: String) extends ChatEvent

case class IncomingMessage(message: String) extends ChatEvent