package hydra.ingest.ws

import akka.actor.ActorRefFactory

/**
  * Created by alexsilva on 3/7/17.
  */
object SocketConnections {
  private var connections: Map[Int, SocketConnection] = Map.empty[Int, SocketConnection]

  def findOrCreate(number: Int)(implicit factory: ActorRefFactory): SocketConnection =
    connections.getOrElse(number, createNewConnection(number))

  private def createNewConnection(connectionId: Int)(implicit factory: ActorRefFactory): SocketConnection = {
    val connection = SocketConnection(connectionId)
    connections += connectionId -> connection
    connection
  }
}