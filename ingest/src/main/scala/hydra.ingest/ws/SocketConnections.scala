package hydra.ingest.ws

import akka.actor.ActorRefFactory

/**
  * Created by alexsilva on 3/7/17.
  */
object SocketConnections {
  private var connections: Map[String, ChatRoom] = Map.empty[String, ChatRoom]

  def findOrCreate(label: String)(implicit factory: ActorRefFactory): ChatRoom =
    connections.getOrElse(label, createNewConnection(label))

  private def createNewConnection(label: String)(implicit factory: ActorRefFactory): ChatRoom = {
    val connection = ChatRoom(label)
    connections += label -> connection
    connection
  }
}