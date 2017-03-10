package hydra.ingest.ws

/**
  * Created by alexsilva on 3/10/17.
  */

import akka.actor.{Actor, ActorRef}
import hydra.ingest.ws.chat._

class ChatRoomActor(destination: String) extends Actor {

  private var participants: Map[String, ActorRef] = Map.empty[String, ActorRef]

  override def receive: Receive = {
    case UserJoined(name, actorRef) =>
      participants += name -> actorRef
      broadcast(SystemMessage(s"User $name joined channel..."))
      println(s"User $name joined channel[$destination]")

    case UserLeft(name) =>
      println(s"User $name left channel[$destination]")
      broadcast(SystemMessage(s"User $name left channel[$destination]"))
      participants -= name

    case msg: IncomingMessage => broadcast(msg)
  }

  def broadcast(message: ChatMessage): Unit = participants.values.foreach(_ ! message)

}
